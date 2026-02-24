"""
AsyncStorageContext - Async version of StorageContext for Python async/await patterns.

Represents a specific Service Provider + DataSet pair with full async support.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, TYPE_CHECKING, Awaitable

import httpx

from pynapse.core.piece import calculate_piece_cid
from pynapse.core.typed_data import sign_add_pieces_extra_data, sign_create_dataset_extra_data
from pynapse.pdp import AsyncPDPServer
from pynapse.utils.metadata import combine_metadata, metadata_matches, metadata_object_to_entries

if TYPE_CHECKING:
    from pynapse.sp_registry import AsyncSPRegistryService, ProviderInfo
    from pynapse.warm_storage import AsyncWarmStorageService


# Size constants
MIN_UPLOAD_SIZE = 256  # bytes
MAX_UPLOAD_SIZE = 254 * 1024 * 1024  # 254 MiB


@dataclass
class AsyncUploadResult:
    """Result of an async upload operation."""
    piece_cid: str
    size: int
    tx_hash: Optional[str] = None
    piece_id: Optional[int] = None


@dataclass
class AsyncProviderSelectionResult:
    """Result of async provider and dataset selection."""
    provider: "ProviderInfo"
    pdp_endpoint: str
    data_set_id: int  # -1 means needs to be created
    client_data_set_id: int
    is_existing: bool
    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass 
class AsyncStorageContextOptions:
    """Options for creating an async storage context."""
    provider_id: Optional[int] = None
    provider_address: Optional[str] = None
    data_set_id: Optional[int] = None
    with_cdn: bool = False
    force_create_data_set: bool = False
    metadata: Optional[Dict[str, str]] = None
    exclude_provider_ids: Optional[List[int]] = None
    # Async callbacks
    on_provider_selected: Optional[Callable[["ProviderInfo"], Awaitable[None]]] = None
    on_data_set_resolved: Optional[Callable[[dict], Awaitable[None]]] = None


class AsyncStorageContext:
    """
    Async storage context for a specific provider and dataset.
    
    Use the factory methods `create()` or `create_contexts()` to construct
    instances with proper provider selection and dataset resolution.
    
    Example:
        ctx = await AsyncStorageContext.create(
            chain=chain,
            private_key=private_key,
            warm_storage=warm_storage,
            sp_registry=sp_registry,
        )
        result = await ctx.upload(data)
    """
    
    def __init__(
        self,
        pdp_endpoint: str,
        chain,
        private_key: str,
        data_set_id: int,
        client_data_set_id: int,
        provider: Optional["ProviderInfo"] = None,
        with_cdn: bool = False,
        metadata: Optional[Dict[str, str]] = None,
        warm_storage: Optional["AsyncWarmStorageService"] = None,
    ) -> None:
        self._pdp = AsyncPDPServer(pdp_endpoint)
        self._pdp_endpoint = pdp_endpoint
        self._chain = chain
        self._private_key = private_key
        self._data_set_id = data_set_id
        self._client_data_set_id = client_data_set_id
        self._provider = provider
        self._with_cdn = with_cdn
        self._metadata = metadata or {}
        self._warm_storage = warm_storage

    @property
    def data_set_id(self) -> int:
        return self._data_set_id

    @property
    def client_data_set_id(self) -> int:
        return self._client_data_set_id

    @property
    def provider(self) -> Optional["ProviderInfo"]:
        return self._provider

    @property
    def with_cdn(self) -> bool:
        return self._with_cdn

    @property
    def data_set_metadata(self) -> Dict[str, str]:
        return self._metadata

    @staticmethod
    def _validate_size(size_bytes: int, context: str = "upload") -> None:
        """Validate data size against limits."""
        if size_bytes < MIN_UPLOAD_SIZE:
            raise ValueError(
                f"Data size {size_bytes} bytes is below minimum allowed size of {MIN_UPLOAD_SIZE} bytes"
            )
        if size_bytes > MAX_UPLOAD_SIZE:
            raise ValueError(
                f"Data size {size_bytes} bytes exceeds maximum allowed size of {MAX_UPLOAD_SIZE} bytes "
                f"({MAX_UPLOAD_SIZE // 1024 // 1024} MiB)"
            )

    @classmethod
    async def create(
        cls,
        chain,
        private_key: str,
        warm_storage: "AsyncWarmStorageService",
        sp_registry: "AsyncSPRegistryService",
        options: Optional[AsyncStorageContextOptions] = None,
    ) -> "AsyncStorageContext":
        """
        Create an async storage context with smart provider and dataset selection.
        
        Args:
            chain: The chain configuration
            private_key: Private key for signing
            warm_storage: AsyncWarmStorageService instance
            sp_registry: AsyncSPRegistryService instance
            options: Optional configuration for context creation
            
        Returns:
            A configured AsyncStorageContext instance
        """
        from eth_account import Account
        acct = Account.from_key(private_key)
        client_address = acct.address
        
        options = options or AsyncStorageContextOptions()
        requested_metadata = combine_metadata(options.metadata, options.with_cdn)
        
        # Resolve provider and dataset
        resolution = await cls._resolve_provider_and_data_set(
            client_address=client_address,
            chain=chain,
            private_key=private_key,
            warm_storage=warm_storage,
            sp_registry=sp_registry,
            options=options,
            requested_metadata=requested_metadata,
        )
        
        # Fire callbacks
        if options.on_provider_selected and resolution.provider:
            try:
                await options.on_provider_selected(resolution.provider)
            except Exception:
                pass
        
        # Create dataset if needed
        data_set_id = resolution.data_set_id
        client_data_set_id = resolution.client_data_set_id
        
        if data_set_id == -1:
            # Need to create a new dataset
            pdp = AsyncPDPServer(resolution.pdp_endpoint)
            await cls._ensure_provider_approved(warm_storage, resolution.provider.provider_id)
            
            # Use a random client_data_set_id like the TypeScript SDK does
            # This ensures uniqueness and avoids collisions with existing datasets
            from pynapse.core.rand import rand_u256
            next_client_id = rand_u256()
            
            # Convert metadata dict to list of {key, value} entries
            metadata_entries = metadata_object_to_entries(requested_metadata)
            
            extra_data = sign_create_dataset_extra_data(
                private_key=private_key,
                chain=chain,
                client_data_set_id=next_client_id,
                payee=resolution.provider.payee,
                metadata=metadata_entries,
            )
            resp = await pdp.create_data_set(
                record_keeper=chain.contracts.warm_storage,
                extra_data=extra_data,
            )
            # Wait for creation
            status = await pdp.wait_for_data_set_creation(resp.tx_hash)
            data_set_id = status.data_set_id
            # Get client_data_set_id from the new dataset
            ds_info = await warm_storage.get_data_set(data_set_id)
            client_data_set_id = ds_info.client_data_set_id
        
        # Fire dataset resolved callback
        if options.on_data_set_resolved:
            try:
                await options.on_data_set_resolved({
                    "is_existing": resolution.is_existing,
                    "data_set_id": data_set_id,
                    "provider": resolution.provider,
                })
            except Exception:
                pass
        
        return cls(
            pdp_endpoint=resolution.pdp_endpoint,
            chain=chain,
            private_key=private_key,
            data_set_id=data_set_id,
            client_data_set_id=client_data_set_id,
            provider=resolution.provider,
            with_cdn=options.with_cdn,
            metadata=requested_metadata,
            warm_storage=warm_storage,
        )

    @classmethod
    async def create_contexts(
        cls,
        chain,
        private_key: str,
        warm_storage: "AsyncWarmStorageService",
        sp_registry: "AsyncSPRegistryService",
        count: int = 2,
        options: Optional[AsyncStorageContextOptions] = None,
    ) -> List["AsyncStorageContext"]:
        """
        Create multiple async storage contexts for multi-provider redundancy.
        
        Args:
            chain: The chain configuration
            private_key: Private key for signing
            warm_storage: AsyncWarmStorageService instance
            sp_registry: AsyncSPRegistryService instance
            count: Number of contexts to create (default: 2)
            options: Optional configuration for context creation
            
        Returns:
            List of configured AsyncStorageContext instances
        """
        contexts: List[AsyncStorageContext] = []
        used_provider_ids: List[int] = []
        
        options = options or AsyncStorageContextOptions()
        
        for _ in range(count):
            # Build options with exclusions
            ctx_options = AsyncStorageContextOptions(
                provider_id=options.provider_id if not contexts else None,
                provider_address=options.provider_address if not contexts else None,
                data_set_id=options.data_set_id if not contexts else None,
                with_cdn=options.with_cdn,
                force_create_data_set=options.force_create_data_set,
                metadata=options.metadata,
                exclude_provider_ids=(options.exclude_provider_ids or []) + used_provider_ids,
                on_provider_selected=options.on_provider_selected,
                on_data_set_resolved=options.on_data_set_resolved,
            )
            
            try:
                ctx = await cls.create(
                    chain=chain,
                    private_key=private_key,
                    warm_storage=warm_storage,
                    sp_registry=sp_registry,
                    options=ctx_options,
                )
                contexts.append(ctx)
                if ctx.provider:
                    used_provider_ids.append(ctx.provider.provider_id)
            except Exception as e:
                # If we can't create more contexts, return what we have
                if not contexts:
                    raise
                break
        
        return contexts

    @classmethod
    async def _resolve_provider_and_data_set(
        cls,
        client_address: str,
        chain,
        private_key: str,
        warm_storage: "AsyncWarmStorageService",
        sp_registry: "AsyncSPRegistryService",
        options: AsyncStorageContextOptions,
        requested_metadata: Dict[str, str],
    ) -> AsyncProviderSelectionResult:
        """Resolve provider and dataset based on options."""
        
        # 1. If explicit data_set_id provided
        if options.data_set_id is not None and not options.force_create_data_set:
            return await cls._resolve_by_data_set_id(
                data_set_id=options.data_set_id,
                client_address=client_address,
                warm_storage=warm_storage,
                sp_registry=sp_registry,
                requested_metadata=requested_metadata,
                options=options,
            )
        
        # 2. If explicit provider_id provided
        if options.provider_id is not None:
            return await cls._resolve_by_provider_id(
                provider_id=options.provider_id,
                client_address=client_address,
                warm_storage=warm_storage,
                sp_registry=sp_registry,
                requested_metadata=requested_metadata,
                force_create=options.force_create_data_set,
            )
        
        # 3. If explicit provider_address provided
        if options.provider_address is not None:
            provider = await sp_registry.get_provider_by_address(options.provider_address)
            if provider is None:
                raise ValueError(f"Provider {options.provider_address} not found in registry")
            return await cls._resolve_by_provider_id(
                provider_id=provider.provider_id,
                client_address=client_address,
                warm_storage=warm_storage,
                sp_registry=sp_registry,
                requested_metadata=requested_metadata,
                force_create=options.force_create_data_set,
            )
        
        # 4. Smart selection
        return await cls._smart_select_provider(
            client_address=client_address,
            warm_storage=warm_storage,
            sp_registry=sp_registry,
            requested_metadata=requested_metadata,
            exclude_provider_ids=options.exclude_provider_ids or [],
            force_create=options.force_create_data_set,
        )

    @classmethod
    async def _resolve_by_data_set_id(
        cls,
        data_set_id: int,
        client_address: str,
        warm_storage: "AsyncWarmStorageService",
        sp_registry: "AsyncSPRegistryService",
        requested_metadata: Optional[Dict[str, str]] = None,
        options: Optional[AsyncStorageContextOptions] = None,
    ) -> AsyncProviderSelectionResult:
        """Resolve using explicit dataset ID."""
        await warm_storage.validate_data_set(data_set_id)
        ds_info = await warm_storage.get_data_set(data_set_id)

        if ds_info.payer.lower() != client_address.lower():
            raise ValueError(
                f"Data set {data_set_id} is not owned by {client_address} (owned by {ds_info.payer})"
            )

        # Provider consistency check: if user specified a provider, verify it matches the dataset
        if options is not None:
            if options.provider_id is not None and options.provider_id != ds_info.provider_id:
                raise ValueError(
                    f"Data set {data_set_id} belongs to provider {ds_info.provider_id}, "
                    f"not the requested provider {options.provider_id}"
                )
            if options.provider_address is not None:
                provider_by_addr = await sp_registry.get_provider_by_address(options.provider_address)
                if provider_by_addr is not None and provider_by_addr.provider_id != ds_info.provider_id:
                    raise ValueError(
                        f"Data set {data_set_id} belongs to provider {ds_info.provider_id}, "
                        f"not the requested provider address {options.provider_address}"
                    )

        provider = await sp_registry.get_provider(ds_info.provider_id)
        if provider is None:
            raise ValueError(f"Provider ID {ds_info.provider_id} for data set {data_set_id} not found")
        if not provider.is_active:
            raise ValueError(f"Provider ID {provider.provider_id} is not active")
        await cls._ensure_provider_approved(warm_storage, provider.provider_id)

        # Get PDP endpoint from provider product info
        pdp_endpoint = await cls._get_pdp_endpoint(sp_registry, provider.provider_id)
        metadata = await warm_storage.get_all_data_set_metadata(data_set_id)

        # Metadata consistency check: if user requested specific metadata, verify it matches
        if requested_metadata and not metadata_matches(metadata, requested_metadata):
            raise ValueError(
                f"Data set {data_set_id} metadata {metadata} does not match "
                f"requested metadata {requested_metadata}"
            )

        return AsyncProviderSelectionResult(
            provider=provider,
            pdp_endpoint=pdp_endpoint,
            data_set_id=data_set_id,
            client_data_set_id=ds_info.client_data_set_id,
            is_existing=True,
            metadata=metadata,
        )

    @classmethod
    async def _resolve_by_provider_id(
        cls,
        provider_id: int,
        client_address: str,
        warm_storage: "AsyncWarmStorageService",
        sp_registry: "AsyncSPRegistryService",
        requested_metadata: Dict[str, str],
        force_create: bool = False,
    ) -> AsyncProviderSelectionResult:
        """Resolve by provider ID, finding or creating dataset."""
        provider = await sp_registry.get_provider(provider_id)
        if provider is None:
            raise ValueError(f"Provider ID {provider_id} not found in registry")
        if not provider.is_active:
            raise ValueError(f"Provider ID {provider_id} is not active")
        await cls._ensure_provider_approved(warm_storage, provider_id)
        
        pdp_endpoint = await cls._get_pdp_endpoint(sp_registry, provider_id)
        
        if force_create:
            return AsyncProviderSelectionResult(
                provider=provider,
                pdp_endpoint=pdp_endpoint,
                data_set_id=-1,
                client_data_set_id=0,
                is_existing=False,
                metadata=requested_metadata,
            )
        
        # Try to find existing dataset for this provider
        try:
            datasets = await warm_storage.get_client_data_sets(client_address)
            for ds in datasets:
                if ds.provider_id == provider_id and ds.pdp_end_epoch == 0:
                    # Check metadata match
                    ds_metadata = await warm_storage.get_all_data_set_metadata(ds.data_set_id)
                    if metadata_matches(ds_metadata, requested_metadata):
                        return AsyncProviderSelectionResult(
                            provider=provider,
                            pdp_endpoint=pdp_endpoint,
                            data_set_id=ds.data_set_id,
                            client_data_set_id=ds.client_data_set_id,
                            is_existing=True,
                            metadata=ds_metadata,
                        )
        except Exception:
            pass
        
        # No matching dataset found, need to create
        return AsyncProviderSelectionResult(
            provider=provider,
            pdp_endpoint=pdp_endpoint,
            data_set_id=-1,
            client_data_set_id=0,
            is_existing=False,
            metadata=requested_metadata,
        )

    @classmethod
    async def _smart_select_provider(
        cls,
        client_address: str,
        warm_storage: "AsyncWarmStorageService",
        sp_registry: "AsyncSPRegistryService",
        requested_metadata: Dict[str, str],
        exclude_provider_ids: List[int],
        force_create: bool = False,
    ) -> AsyncProviderSelectionResult:
        """Smart provider selection with existing dataset reuse."""
        exclude_set = set(exclude_provider_ids)
        
        # First, try to find existing datasets with matching metadata
        if not force_create:
            try:
                datasets = await warm_storage.get_client_data_sets_with_details(client_address)
                # Filter for live, managed datasets with matching metadata
                matching = [
                    ds for ds in datasets
                    if ds.is_live 
                    and ds.is_managed 
                    and ds.pdp_end_epoch == 0
                    and ds.provider_id not in exclude_set
                    and metadata_matches(ds.metadata, requested_metadata)
                ]
                
                # Prefer datasets with pieces, sorted by ID (older first)
                matching.sort(key=lambda ds: (-ds.active_piece_count, ds.data_set_id))
                
                for ds in matching:
                    provider = await sp_registry.get_provider(ds.provider_id)
                    if provider and provider.is_active:
                        # Health check: try to ping the PDP endpoint
                        pdp_endpoint = await cls._get_pdp_endpoint(sp_registry, ds.provider_id)
                        if await cls._ping_provider(pdp_endpoint):
                            return AsyncProviderSelectionResult(
                                provider=provider,
                                pdp_endpoint=pdp_endpoint,
                                data_set_id=ds.data_set_id,
                                client_data_set_id=ds.client_data_set_id,
                                is_existing=True,
                                metadata=ds.metadata,
                            )
            except Exception:
                pass
        
        # No existing dataset, select a new provider
        try:
            approved_ids = await warm_storage.get_approved_provider_ids()
        except Exception:
            approved_ids = []
        
        # Filter out excluded providers
        candidate_ids = [pid for pid in approved_ids if pid not in exclude_set]
        
        # Shuffle for random selection
        random.shuffle(candidate_ids)
        
        # Find a healthy provider
        for pid in candidate_ids:
            try:
                provider = await sp_registry.get_provider(pid)
                if provider and provider.is_active:
                    pdp_endpoint = await cls._get_pdp_endpoint(sp_registry, pid)
                    if await cls._ping_provider(pdp_endpoint):
                        return AsyncProviderSelectionResult(
                            provider=provider,
                            pdp_endpoint=pdp_endpoint,
                            data_set_id=-1,
                            client_data_set_id=0,
                            is_existing=False,
                            metadata=requested_metadata,
                        )
            except Exception:
                continue
        
        raise ValueError("No approved service providers available")

    @classmethod
    async def _get_pdp_endpoint(cls, sp_registry: "AsyncSPRegistryService", provider_id: int) -> str:
        """Get the PDP service URL for a provider."""
        try:
            product = await sp_registry.get_provider_with_product(provider_id, 0)  # PDP product type
            # Look for serviceURL in capability values
            for i, key in enumerate(product.product.capability_keys):
                if key == "serviceURL" and i < len(product.product_capability_values):
                    val = product.product_capability_values[i]
                    # Values are returned as bytes from the contract
                    if isinstance(val, bytes):
                        return val.decode('utf-8')
                    return str(val)
        except Exception:
            pass
        
        raise ValueError(f"Could not find PDP endpoint for provider {provider_id}")

    @classmethod
    async def _ping_provider(cls, pdp_endpoint: str, timeout: float = 5.0) -> bool:
        """Health check a provider's PDP endpoint."""
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.head(pdp_endpoint)
                return resp.status_code < 500
        except Exception:
            return False

    @classmethod
    async def _ensure_provider_approved(
        cls,
        warm_storage: "AsyncWarmStorageService",
        provider_id: int,
    ) -> None:
        try:
            if not await warm_storage.is_provider_approved(provider_id):
                try:
                    approved = await warm_storage.get_approved_provider_ids()
                except Exception:
                    approved = None
                if approved:
                    raise ValueError(
                        f"Provider {provider_id} is not approved for Warm Storage. "
                        f"Approved providers: {approved}"
                    )
                raise ValueError(
                    f"Provider {provider_id} is not approved for Warm Storage. "
                    "Choose an approved provider or request approval."
                )
        except Exception as exc:
            if isinstance(exc, ValueError):
                raise
            raise ValueError(f"Failed to verify provider approval for {provider_id}: {exc}") from exc

    async def _preflight_add_pieces(self, total_size_bytes: int, piece_count: int) -> None:
        if self._warm_storage is None:
            return
        try:
            await self._warm_storage.validate_data_set(self._data_set_id)
        except Exception as exc:
            raise ValueError(
                f"Preflight failed for data set {self._data_set_id}: {exc}. "
                "Ensure the data set is live and managed by the Warm Storage contract."
            ) from exc
        try:
            ds_info = await self._warm_storage.get_data_set(self._data_set_id)
        except Exception as exc:
            raise ValueError(
                f"Failed to load data set {self._data_set_id} for preflight: {exc}"
            ) from exc
        try:
            from eth_account import Account
            acct = Account.from_key(self._private_key)
            if ds_info.payer.lower() != acct.address.lower():
                raise ValueError(
                    f"Data set {self._data_set_id} is owned by {ds_info.payer}, "
                    f"not {acct.address}. Use the correct private key or dataset ID."
                )
        except Exception as exc:
            if isinstance(exc, ValueError):
                raise
            raise ValueError(f"Failed to verify data set ownership: {exc}") from exc
        await self._ensure_provider_approved(self._warm_storage, ds_info.provider_id)

    async def upload(
        self,
        data: bytes,
        metadata: Optional[Dict[str, str]] = None,
        on_progress: Optional[Callable[[int], Awaitable[None]]] = None,
        on_upload_complete: Optional[Callable[[str], Awaitable[None]]] = None,
        on_pieces_added: Optional[Callable[[str], Awaitable[None]]] = None,
    ) -> AsyncUploadResult:
        """
        Upload data to this storage context asynchronously.
        
        Args:
            data: Bytes to upload
            metadata: Optional piece metadata
            on_progress: Async callback for upload progress
            on_upload_complete: Async callback when upload completes
            on_pieces_added: Async callback when pieces are added on-chain
            
        Returns:
            AsyncUploadResult with piece CID and transaction info
        """
        self._validate_size(len(data))
        await self._preflight_add_pieces(len(data), 1)
        
        info = calculate_piece_cid(data)
        
        # Upload to PDP server (include padded_piece_size for PieceCIDv1)
        await self._pdp.upload_piece(data, info.piece_cid, info.padded_piece_size)
        
        # Wait for piece to be indexed before adding to dataset
        # The PDP server needs time to process and index uploaded pieces
        await self._pdp.wait_for_piece(info.piece_cid, timeout_seconds=60, poll_interval=2)
        
        if on_upload_complete:
            try:
                await on_upload_complete(info.piece_cid)
            except Exception:
                pass
        
        # Add piece to dataset
        pieces = [(info.piece_cid, [{"key": k, "value": v} for k, v in (metadata or {}).items()])]
        extra_data = sign_add_pieces_extra_data(
            private_key=self._private_key,
            chain=self._chain,
            client_data_set_id=self._client_data_set_id,
            pieces=pieces,
        )
        
        add_resp = await self._pdp.add_pieces(self._data_set_id, [info.piece_cid], extra_data)
        
        if on_pieces_added:
            try:
                await on_pieces_added(add_resp.tx_hash)
            except Exception:
                pass
        
        return AsyncUploadResult(
            piece_cid=info.piece_cid,
            size=info.payload_size,
            tx_hash=add_resp.tx_hash,
        )

    async def upload_multi(
        self,
        data_items: List[bytes],
        metadata: Optional[Dict[str, str]] = None,
    ) -> List[AsyncUploadResult]:
        """
        Upload multiple pieces in a batch asynchronously.
        
        Args:
            data_items: List of byte arrays to upload
            metadata: Optional metadata to apply to all pieces
            
        Returns:
            List of AsyncUploadResults
        """
        results = []
        piece_infos = []
        total_size = 0
        
        # Validate sizes and compute total size up front
        for data in data_items:
            self._validate_size(len(data))
            total_size += len(data)
        await self._preflight_add_pieces(total_size, len(data_items))

        # Calculate CIDs and upload all pieces
        for data in data_items:
            info = calculate_piece_cid(data)
            await self._pdp.upload_piece(data, info.piece_cid, info.padded_piece_size)
            piece_infos.append(info)
        
        # Wait for all pieces to be indexed before adding to dataset
        for info in piece_infos:
            await self._pdp.wait_for_piece(info.piece_cid, timeout_seconds=60, poll_interval=2)
        
        # Batch add pieces
        pieces = [
            (info.piece_cid, [{"key": k, "value": v} for k, v in (metadata or {}).items()])
            for info in piece_infos
        ]
        extra_data = sign_add_pieces_extra_data(
            private_key=self._private_key,
            chain=self._chain,
            client_data_set_id=self._client_data_set_id,
            pieces=pieces,
        )
        
        piece_cids = [info.piece_cid for info in piece_infos]
        add_resp = await self._pdp.add_pieces(self._data_set_id, piece_cids, extra_data)
        
        for info in piece_infos:
            results.append(AsyncUploadResult(
                piece_cid=info.piece_cid,
                size=info.payload_size,
                tx_hash=add_resp.tx_hash,
            ))
        
        return results

    async def download(self, piece_cid: str) -> bytes:
        """Download a piece by CID asynchronously."""
        return await self._pdp.download_piece(piece_cid)

    async def has_piece(self, piece_cid: str) -> bool:
        """Check if a piece exists on this provider asynchronously."""
        try:
            await self._pdp.find_piece(piece_cid)
            return True
        except Exception:
            return False

    async def wait_for_piece(self, piece_cid: str, timeout_seconds: int = 300) -> None:
        """Wait for a piece to be available on this provider asynchronously."""
        await self._pdp.wait_for_piece(piece_cid, timeout_seconds)
