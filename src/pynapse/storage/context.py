"""
StorageContext - Represents a specific Service Provider + DataSet pair

This class provides a connection to a specific service provider and data set,
handling uploads and downloads within that context. It manages:
- Provider selection and data set creation/reuse
- PieceCID calculation and validation
- Payment rail setup through Warm Storage
- Batched piece additions for efficiency
"""
from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, TYPE_CHECKING

from pynapse.core.piece import calculate_piece_cid
from pynapse.core.typed_data import sign_add_pieces_extra_data, sign_create_dataset_extra_data
from pynapse.pdp import PDPServer
from pynapse.pdp.server import AlreadyExistsError, IdempotencyError
from pynapse.utils.metadata import combine_metadata, metadata_matches, metadata_object_to_entries

if TYPE_CHECKING:
    from pynapse.sp_registry import ProviderInfo
    from pynapse.warm_storage import SyncWarmStorageService


# Size constants
MIN_UPLOAD_SIZE = 256  # bytes
MAX_UPLOAD_SIZE = 254 * 1024 * 1024  # 254 MiB


@dataclass
class UploadResult:
    """Result of an upload operation."""
    piece_cid: str
    size: int
    tx_hash: Optional[str] = None
    piece_id: Optional[int] = None


@dataclass
class ProviderSelectionResult:
    """Result of provider and dataset selection."""
    provider: "ProviderInfo"
    pdp_endpoint: str
    data_set_id: int  # -1 means needs to be created
    client_data_set_id: int
    is_existing: bool
    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass 
class StorageContextOptions:
    """Options for creating a storage context."""
    provider_id: Optional[int] = None
    provider_address: Optional[str] = None
    data_set_id: Optional[int] = None
    with_cdn: bool = False
    force_create_data_set: bool = False
    metadata: Optional[Dict[str, str]] = None
    exclude_provider_ids: Optional[List[int]] = None
    # Callbacks
    on_provider_selected: Optional[Callable[["ProviderInfo"], None]] = None
    on_data_set_resolved: Optional[Callable[[dict], None]] = None


class StorageContext:
    """
    Storage context for a specific provider and dataset.

    Use the factory methods `create()` or `create_contexts()` to construct
    instances with proper provider selection and dataset resolution.
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
    ) -> None:
        self._pdp = PDPServer(pdp_endpoint)
        self._pdp_endpoint = pdp_endpoint
        self._chain = chain
        self._private_key = private_key
        self._data_set_id = data_set_id
        self._client_data_set_id = client_data_set_id
        self._provider = provider
        self._with_cdn = with_cdn
        self._metadata = metadata or {}

    @staticmethod
    def _generate_idempotency_key(operation: str, *args: str) -> str:
        """
        Generate a deterministic idempotency key based on operation and parameters.

        Args:
            operation: The operation type (e.g., "create_dataset", "add_pieces")
            *args: Additional parameters to include in the key

        Returns:
            Hex-encoded SHA-256 hash as idempotency key
        """
        content = f"{operation}:" + ":".join(args)
        return hashlib.sha256(content.encode('utf-8')).hexdigest()[:32]

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
    def create(
        cls,
        chain,
        private_key: str,
        warm_storage: "SyncWarmStorageService",
        sp_registry,
        options: Optional[StorageContextOptions] = None,
    ) -> "StorageContext":
        """
        Create a storage context with smart provider and dataset selection.
        
        Args:
            chain: The chain configuration
            private_key: Private key for signing
            warm_storage: WarmStorageService instance
            sp_registry: SPRegistryService instance
            options: Optional configuration for context creation
            
        Returns:
            A configured StorageContext instance
        """
        from eth_account import Account
        acct = Account.from_key(private_key)
        client_address = acct.address
        
        options = options or StorageContextOptions()
        requested_metadata = combine_metadata(options.metadata, options.with_cdn)
        
        # Resolve provider and dataset
        resolution = cls._resolve_provider_and_data_set(
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
                options.on_provider_selected(resolution.provider)
            except Exception:
                pass
        
        # Create dataset if needed
        data_set_id = resolution.data_set_id
        client_data_set_id = resolution.client_data_set_id
        
        if data_set_id == -1:
            # Need to create a new dataset
            pdp = PDPServer(resolution.pdp_endpoint)

            # Get next client_data_set_id by counting existing datasets
            try:
                existing = warm_storage.get_client_data_sets(acct.address)
                next_client_id = len(existing) + 1
            except Exception:
                next_client_id = 1

            # Convert metadata dict to list of {key, value} entries
            metadata_entries = metadata_object_to_entries(requested_metadata)

            extra_data = sign_create_dataset_extra_data(
                private_key=private_key,
                chain=chain,
                client_data_set_id=next_client_id,
                payee=resolution.provider.payee,
                metadata=metadata_entries,
            )

            # Generate idempotency key for dataset creation
            idempotency_key = cls._generate_idempotency_key(
                "create_dataset",
                acct.address,
                str(resolution.provider.provider_id),
                str(next_client_id),
                str(sorted(requested_metadata.items()))
            )

            try:
                resp = pdp.create_data_set(
                    record_keeper=chain.contracts.warm_storage,
                    extra_data=extra_data,
                    idempotency_key=idempotency_key,
                )
                # Wait for creation
                status = pdp.wait_for_data_set_creation(resp.tx_hash)
                data_set_id = status.data_set_id
                # Get client_data_set_id from the new dataset
                ds_info = warm_storage.get_data_set(data_set_id)
                client_data_set_id = ds_info.client_data_set_id
            except AlreadyExistsError as e:
                # Dataset already exists - this is acceptable for idempotency
                # Try to find the existing dataset
                if e.existing_resource_id:
                    try:
                        data_set_id = int(e.existing_resource_id)
                        ds_info = warm_storage.get_data_set(data_set_id)
                        client_data_set_id = ds_info.client_data_set_id
                    except (ValueError, Exception):
                        # Fall back to original error if we can't resolve the existing dataset
                        raise e
                else:
                    # Try to find a matching dataset by searching client datasets
                    try:
                        datasets = warm_storage.get_client_data_sets(acct.address)
                        for ds in datasets:
                            if (ds.provider_id == resolution.provider.provider_id
                                and ds.pdp_end_epoch == 0):
                                # Check metadata match
                                ds_metadata = warm_storage.get_all_data_set_metadata(ds.data_set_id)
                                if metadata_matches(ds_metadata, requested_metadata):
                                    data_set_id = ds.data_set_id
                                    client_data_set_id = ds.client_data_set_id
                                    break
                        else:
                            # No matching dataset found
                            raise e
                    except Exception:
                        raise e
        
        # Fire dataset resolved callback
        if options.on_data_set_resolved:
            try:
                options.on_data_set_resolved({
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
        )

    @classmethod
    def create_contexts(
        cls,
        chain,
        private_key: str,
        warm_storage: "SyncWarmStorageService",
        sp_registry,
        count: int = 2,
        options: Optional[StorageContextOptions] = None,
    ) -> List["StorageContext"]:
        """
        Create multiple storage contexts for multi-provider redundancy.
        
        Args:
            chain: The chain configuration
            private_key: Private key for signing
            warm_storage: WarmStorageService instance
            sp_registry: SPRegistryService instance
            count: Number of contexts to create (default: 2)
            options: Optional configuration for context creation
            
        Returns:
            List of configured StorageContext instances
        """
        contexts: List[StorageContext] = []
        used_provider_ids: List[int] = []
        
        options = options or StorageContextOptions()
        
        for _ in range(count):
            # Build options with exclusions
            ctx_options = StorageContextOptions(
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
                ctx = cls.create(
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
    def _resolve_provider_and_data_set(
        cls,
        client_address: str,
        chain,
        private_key: str,
        warm_storage: "SyncWarmStorageService",
        sp_registry,
        options: StorageContextOptions,
        requested_metadata: Dict[str, str],
    ) -> ProviderSelectionResult:
        """Resolve provider and dataset based on options."""
        
        # 1. If explicit data_set_id provided
        if options.data_set_id is not None and not options.force_create_data_set:
            return cls._resolve_by_data_set_id(
                data_set_id=options.data_set_id,
                client_address=client_address,
                warm_storage=warm_storage,
                sp_registry=sp_registry,
                requested_metadata=requested_metadata,
                options=options,
            )
        
        # 2. If explicit provider_id provided
        if options.provider_id is not None:
            return cls._resolve_by_provider_id(
                provider_id=options.provider_id,
                client_address=client_address,
                warm_storage=warm_storage,
                sp_registry=sp_registry,
                requested_metadata=requested_metadata,
                force_create=options.force_create_data_set,
            )
        
        # 3. If explicit provider_address provided
        if options.provider_address is not None:
            provider = sp_registry.get_provider_by_address(options.provider_address)
            if provider is None:
                raise ValueError(f"Provider {options.provider_address} not found in registry")
            return cls._resolve_by_provider_id(
                provider_id=provider.provider_id,
                client_address=client_address,
                warm_storage=warm_storage,
                sp_registry=sp_registry,
                requested_metadata=requested_metadata,
                force_create=options.force_create_data_set,
            )
        
        # 4. Smart selection
        return cls._smart_select_provider(
            client_address=client_address,
            warm_storage=warm_storage,
            sp_registry=sp_registry,
            requested_metadata=requested_metadata,
            exclude_provider_ids=options.exclude_provider_ids or [],
            force_create=options.force_create_data_set,
        )

    @classmethod
    def _resolve_by_data_set_id(
        cls,
        data_set_id: int,
        client_address: str,
        warm_storage: "SyncWarmStorageService",
        sp_registry,
        requested_metadata: Optional[Dict[str, str]] = None,
        options: Optional[StorageContextOptions] = None,
    ) -> ProviderSelectionResult:
        """Resolve using explicit dataset ID."""
        warm_storage.validate_data_set(data_set_id)
        ds_info = warm_storage.get_data_set(data_set_id)

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
                provider_by_addr = sp_registry.get_provider_by_address(options.provider_address)
                if provider_by_addr is not None and provider_by_addr.provider_id != ds_info.provider_id:
                    raise ValueError(
                        f"Data set {data_set_id} belongs to provider {ds_info.provider_id}, "
                        f"not the requested provider address {options.provider_address}"
                    )

        provider = sp_registry.get_provider(ds_info.provider_id)
        if provider is None:
            raise ValueError(f"Provider ID {ds_info.provider_id} for data set {data_set_id} not found")

        # Get PDP endpoint from provider product info
        pdp_endpoint = cls._get_pdp_endpoint(sp_registry, provider.provider_id)
        metadata = warm_storage.get_all_data_set_metadata(data_set_id)

        # Metadata consistency check: if user requested specific metadata, verify it matches
        if requested_metadata and not metadata_matches(metadata, requested_metadata):
            raise ValueError(
                f"Data set {data_set_id} metadata {metadata} does not match "
                f"requested metadata {requested_metadata}"
            )

        return ProviderSelectionResult(
            provider=provider,
            pdp_endpoint=pdp_endpoint,
            data_set_id=data_set_id,
            client_data_set_id=ds_info.client_data_set_id,
            is_existing=True,
            metadata=metadata,
        )

    @classmethod
    def _resolve_by_provider_id(
        cls,
        provider_id: int,
        client_address: str,
        warm_storage: "SyncWarmStorageService",
        sp_registry,
        requested_metadata: Dict[str, str],
        force_create: bool = False,
    ) -> ProviderSelectionResult:
        """Resolve by provider ID, finding or creating dataset."""
        provider = sp_registry.get_provider(provider_id)
        if provider is None:
            raise ValueError(f"Provider ID {provider_id} not found in registry")
        
        pdp_endpoint = cls._get_pdp_endpoint(sp_registry, provider_id)
        
        if force_create:
            return ProviderSelectionResult(
                provider=provider,
                pdp_endpoint=pdp_endpoint,
                data_set_id=-1,
                client_data_set_id=0,
                is_existing=False,
                metadata=requested_metadata,
            )
        
        # Try to find existing dataset for this provider
        try:
            datasets = warm_storage.get_client_data_sets(client_address)
            for ds in datasets:
                if ds.provider_id == provider_id and ds.pdp_end_epoch == 0:
                    # Check metadata match
                    ds_metadata = warm_storage.get_all_data_set_metadata(ds.data_set_id)
                    if metadata_matches(ds_metadata, requested_metadata):
                        return ProviderSelectionResult(
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
        return ProviderSelectionResult(
            provider=provider,
            pdp_endpoint=pdp_endpoint,
            data_set_id=-1,
            client_data_set_id=0,
            is_existing=False,
            metadata=requested_metadata,
        )

    @classmethod
    def _smart_select_provider(
        cls,
        client_address: str,
        warm_storage: "SyncWarmStorageService",
        sp_registry,
        requested_metadata: Dict[str, str],
        exclude_provider_ids: List[int],
        force_create: bool = False,
    ) -> ProviderSelectionResult:
        """Smart provider selection with existing dataset reuse."""
        exclude_set = set(exclude_provider_ids)
        
        # First, try to find existing datasets with matching metadata
        if not force_create:
            try:
                datasets = warm_storage.get_client_data_sets_with_details(client_address)
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
                    provider = sp_registry.get_provider(ds.provider_id)
                    if provider and provider.is_active:
                        # Health check: try to ping the PDP endpoint
                        pdp_endpoint = cls._get_pdp_endpoint(sp_registry, ds.provider_id)
                        if cls._ping_provider(pdp_endpoint):
                            return ProviderSelectionResult(
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
            approved_ids = warm_storage.get_approved_provider_ids()
        except Exception:
            approved_ids = []
        
        # Filter out excluded providers
        candidate_ids = [pid for pid in approved_ids if pid not in exclude_set]
        
        # Shuffle for random selection
        random.shuffle(candidate_ids)
        
        # Find a healthy provider
        for pid in candidate_ids:
            try:
                provider = sp_registry.get_provider(pid)
                if provider and provider.is_active:
                    pdp_endpoint = cls._get_pdp_endpoint(sp_registry, pid)
                    if cls._ping_provider(pdp_endpoint):
                        return ProviderSelectionResult(
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
    def _get_pdp_endpoint(cls, sp_registry, provider_id: int) -> str:
        """Get the PDP service URL for a provider."""
        try:
            product = sp_registry.get_provider_with_product(provider_id, 0)  # PDP product type
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
    def _ping_provider(cls, pdp_endpoint: str, timeout: float = 5.0) -> bool:
        """Health check a provider's PDP endpoint."""
        import httpx
        try:
            # Try a simple HEAD request to check if the server is responsive
            with httpx.Client(timeout=timeout) as client:
                resp = client.head(pdp_endpoint)
                return resp.status_code < 500
        except Exception:
            return False

    def upload(
        self,
        data: bytes,
        metadata: Optional[Dict[str, str]] = None,
        on_progress: Optional[Callable[[int], None]] = None,
        on_upload_complete: Optional[Callable[[str], None]] = None,
        on_pieces_added: Optional[Callable[[str], None]] = None,
    ) -> UploadResult:
        """
        Upload data to this storage context.
        
        Args:
            data: Bytes to upload
            metadata: Optional piece metadata
            on_progress: Callback for upload progress
            on_upload_complete: Callback when upload completes
            on_pieces_added: Callback when pieces are added on-chain
            
        Returns:
            UploadResult with piece CID and transaction info
        """
        self._validate_size(len(data))
        
        info = calculate_piece_cid(data)
        
        # Upload to PDP server (include padded_piece_size for PieceCIDv1)
        self._pdp.upload_piece(data, info.piece_cid, info.padded_piece_size)
        
        # Wait for piece to be indexed before adding to dataset
        # The PDP server needs time to process and index uploaded pieces
        self._pdp.wait_for_piece(info.piece_cid, timeout_seconds=60, poll_interval=2)
        
        if on_upload_complete:
            try:
                on_upload_complete(info.piece_cid)
            except Exception:
                pass
        
        # Add piece to dataset with idempotency
        pieces = [(info.piece_cid, [{"key": k, "value": v} for k, v in (metadata or {}).items()])]
        extra_data = sign_add_pieces_extra_data(
            private_key=self._private_key,
            chain=self._chain,
            client_data_set_id=self._client_data_set_id,
            pieces=pieces,
        )

        # Generate idempotency key for piece addition
        piece_metadata_str = str(sorted((metadata or {}).items()))
        idempotency_key = self._generate_idempotency_key(
            "add_pieces",
            str(self._data_set_id),
            info.piece_cid,
            piece_metadata_str
        )

        try:
            add_resp = self._pdp.add_pieces(
                self._data_set_id,
                [info.piece_cid],
                extra_data,
                idempotency_key=idempotency_key
            )
        except AlreadyExistsError:
            # Piece already exists in the dataset - this is acceptable for idempotency
            # Create a mock response since the piece is already there
            add_resp = type('MockResponse', (), {
                'tx_hash': None,
                'message': f'Piece {info.piece_cid} already exists in dataset {self._data_set_id}'
            })()
        
        if on_pieces_added:
            try:
                on_pieces_added(add_resp.tx_hash)
            except Exception:
                pass
        
        return UploadResult(
            piece_cid=info.piece_cid,
            size=info.payload_size,
            tx_hash=add_resp.tx_hash,
        )

    def upload_multi(
        self,
        data_items: List[bytes],
        metadata: Optional[Dict[str, str]] = None,
    ) -> List[UploadResult]:
        """
        Upload multiple pieces in a batch.
        
        Args:
            data_items: List of byte arrays to upload
            metadata: Optional metadata to apply to all pieces
            
        Returns:
            List of UploadResults
        """
        results = []
        piece_infos = []
        
        # Calculate CIDs and upload all pieces
        for data in data_items:
            self._validate_size(len(data))
            info = calculate_piece_cid(data)
            self._pdp.upload_piece(data, info.piece_cid, info.padded_piece_size)
            piece_infos.append(info)
        
        # Wait for all pieces to be indexed before adding to dataset
        for info in piece_infos:
            self._pdp.wait_for_piece(info.piece_cid, timeout_seconds=60, poll_interval=2)
        
        # Batch add pieces with idempotency
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
        piece_metadata_str = str(sorted((metadata or {}).items()))

        # Generate idempotency key for batch piece addition
        idempotency_key = self._generate_idempotency_key(
            "add_pieces_batch",
            str(self._data_set_id),
            ":".join(sorted(piece_cids)),  # Sort to ensure deterministic key
            piece_metadata_str
        )

        try:
            add_resp = self._pdp.add_pieces(
                self._data_set_id,
                piece_cids,
                extra_data,
                idempotency_key=idempotency_key
            )
        except AlreadyExistsError:
            # Some/all pieces already exist in the dataset - this is acceptable for idempotency
            # Create a mock response since pieces are already there
            add_resp = type('MockResponse', (), {
                'tx_hash': None,
                'message': f'Some pieces already exist in dataset {self._data_set_id}'
            })()
        
        for info in piece_infos:
            results.append(UploadResult(
                piece_cid=info.piece_cid,
                size=info.payload_size,
                tx_hash=add_resp.tx_hash,
            ))
        
        return results

    def download(self, piece_cid: str) -> bytes:
        """Download a piece by CID."""
        return self._pdp.download_piece(piece_cid)

    def has_piece(self, piece_cid: str) -> bool:
        """Check if a piece exists on this provider."""
        try:
            self._pdp.find_piece(piece_cid)
            return True
        except Exception:
            return False

    def wait_for_piece(self, piece_cid: str, timeout_seconds: int = 300) -> None:
        """Wait for a piece to be available on this provider."""
        self._pdp.wait_for_piece(piece_cid, timeout_seconds)
