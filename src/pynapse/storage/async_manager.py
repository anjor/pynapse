"""
AsyncStorageManager - Central async facade for storage operations.

Manages storage contexts (SP + DataSet pairs) with intelligent provider selection
and dataset reuse. Fully async for Python async/await patterns.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Awaitable, TYPE_CHECKING

from .async_context import AsyncStorageContext, AsyncStorageContextOptions, AsyncUploadResult

if TYPE_CHECKING:
    from pynapse.retriever import AsyncChainRetriever
    from pynapse.sp_registry import AsyncSPRegistryService
    from pynapse.warm_storage import AsyncWarmStorageService


# Size and time constants matching TypeScript SDK
TIB = 1024 ** 4
EPOCHS_PER_DAY = 2880
DAYS_PER_MONTH = 30


@dataclass
class AsyncProviderFilter:
    """Filter criteria for provider selection."""
    provider_ids: Optional[List[int]] = None
    with_cdn: bool = False
    with_ipni: bool = False
    min_piece_size: Optional[int] = None
    max_piece_size: Optional[int] = None
    location: Optional[str] = None
    exclude_provider_ids: Optional[List[int]] = None


@dataclass
class AsyncPreflightInfo:
    """Preflight estimation for storage costs."""
    size_bytes: int
    estimated_cost_per_epoch: int
    estimated_total_cost: int
    duration_epochs: int
    provider_count: int
    providers: List[int] = field(default_factory=list)


@dataclass
class AsyncDataSetMatch:
    """A dataset that matches search criteria."""
    data_set_id: int
    client_data_set_id: int
    provider_id: int
    pdp_endpoint: str
    metadata: Dict[str, str]


@dataclass
class AsyncStoragePricing:
    """Pricing information per time unit."""
    per_tib_per_month: int
    per_tib_per_day: int
    per_tib_per_epoch: int


@dataclass
class AsyncServiceParameters:
    """Service configuration parameters."""
    epochs_per_month: int
    epochs_per_day: int = EPOCHS_PER_DAY
    epoch_duration: int = 30  # seconds
    min_upload_size: int = 256  # bytes
    max_upload_size: int = 254 * 1024 * 1024  # 254 MiB


@dataclass 
class AsyncStorageInfo:
    """Comprehensive storage service information."""
    pricing_no_cdn: AsyncStoragePricing
    pricing_with_cdn: AsyncStoragePricing
    token_address: str
    token_symbol: str
    providers: List[dict]  # List of provider info dicts
    service_parameters: AsyncServiceParameters
    approved_provider_ids: List[int] = field(default_factory=list)


class AsyncStorageManager:
    """
    Central async storage manager with provider selection and dataset reuse.
    
    Features:
    - Smart provider selection by capabilities (CDN, IPNI, location)
    - Dataset reuse based on metadata matching
    - Multi-provider uploads for redundancy
    - Preflight cost estimation
    
    Example:
        # Simple upload (auto-selects provider)
        result = await manager.upload(data)
        
        # Upload with specific provider
        result = await manager.upload(data, provider_id=1)
        
        # Multi-provider upload for redundancy
        results = await manager.upload_multi(data, provider_count=3)
        
        # Preflight check
        info = await manager.preflight(len(data), provider_count=2)
    """
    
    def __init__(
        self, 
        chain, 
        private_key: str,
        sp_registry: Optional["AsyncSPRegistryService"] = None,
        warm_storage: Optional["AsyncWarmStorageService"] = None,
        retriever: Optional["AsyncChainRetriever"] = None,
    ) -> None:
        self._chain = chain
        self._private_key = private_key
        self._sp_registry = sp_registry
        self._warm_storage = warm_storage
        self._retriever = retriever
        self._default_context: Optional[AsyncStorageContext] = None
        self._context_cache: Dict[int, AsyncStorageContext] = {}  # provider_id -> context

    def create_context(
        self, 
        pdp_endpoint: str, 
        data_set_id: int, 
        client_data_set_id: int,
        provider_id: Optional[int] = None,
    ) -> AsyncStorageContext:
        """Create a storage context for a specific provider/dataset (low-level)."""
        context = AsyncStorageContext(
            pdp_endpoint=pdp_endpoint,
            chain=self._chain,
            private_key=self._private_key,
            data_set_id=data_set_id,
            client_data_set_id=client_data_set_id,
            warm_storage=self._warm_storage,
        )
        if provider_id is not None:
            self._context_cache[provider_id] = context
        return context

    async def get_context(
        self,
        provider_id: Optional[int] = None,
        provider_address: Optional[str] = None,
        data_set_id: Optional[int] = None,
        with_cdn: bool = False,
        force_create_data_set: bool = False,
        metadata: Optional[Dict[str, str]] = None,
        exclude_provider_ids: Optional[List[int]] = None,
        on_provider_selected: Optional[Callable] = None,
        on_data_set_resolved: Optional[Callable] = None,
    ) -> AsyncStorageContext:
        """
        Get or create an async storage context with smart provider/dataset selection.
        
        This is the recommended way to get a context - it handles provider
        selection, dataset reuse, and dataset creation automatically.
        
        Args:
            provider_id: Optional specific provider ID to use
            provider_address: Optional specific provider address to use
            data_set_id: Optional specific dataset ID to use
            with_cdn: Whether to enable CDN services
            force_create_data_set: Force creation of new dataset
            metadata: Custom metadata for the dataset
            exclude_provider_ids: Provider IDs to exclude from selection
            on_provider_selected: Callback when provider is selected
            on_data_set_resolved: Callback when dataset is resolved
            
        Returns:
            Configured AsyncStorageContext
        """
        if self._warm_storage is None:
            raise ValueError("warm_storage required for smart context creation")
        if self._sp_registry is None:
            raise ValueError("sp_registry required for smart context creation")
        
        # Check if we can reuse the default context
        can_use_default = (
            provider_id is None
            and provider_address is None
            and data_set_id is None
            and not force_create_data_set
            and self._default_context is not None
        )
        
        if can_use_default:
            # Check if metadata matches
            from pynapse.utils.metadata import combine_metadata, metadata_matches
            requested_metadata = combine_metadata(metadata, with_cdn)
            if metadata_matches(self._default_context.data_set_metadata, requested_metadata):
                return self._default_context
        
        # Create new context using factory method
        options = AsyncStorageContextOptions(
            provider_id=provider_id,
            provider_address=provider_address,
            data_set_id=data_set_id,
            with_cdn=with_cdn,
            force_create_data_set=force_create_data_set,
            metadata=metadata,
            exclude_provider_ids=exclude_provider_ids,
            on_provider_selected=on_provider_selected,
            on_data_set_resolved=on_data_set_resolved,
        )
        
        context = await AsyncStorageContext.create(
            chain=self._chain,
            private_key=self._private_key,
            warm_storage=self._warm_storage,
            sp_registry=self._sp_registry,
            options=options,
        )
        
        # Cache as default if no specific options were provided
        if provider_id is None and provider_address is None and data_set_id is None:
            self._default_context = context
        
        return context

    async def get_contexts(
        self,
        count: int = 2,
        with_cdn: bool = False,
        force_create_data_set: bool = False,
        metadata: Optional[Dict[str, str]] = None,
        exclude_provider_ids: Optional[List[int]] = None,
        on_provider_selected: Optional[Callable] = None,
        on_data_set_resolved: Optional[Callable] = None,
    ) -> List[AsyncStorageContext]:
        """
        Get or create multiple async storage contexts for multi-provider redundancy.
        
        Args:
            count: Number of contexts to create (default: 2)
            with_cdn: Whether to enable CDN services
            force_create_data_set: Force creation of new datasets
            metadata: Custom metadata for datasets
            exclude_provider_ids: Provider IDs to exclude from selection
            on_provider_selected: Callback when provider is selected
            on_data_set_resolved: Callback when dataset is resolved
            
        Returns:
            List of configured AsyncStorageContext instances
        """
        if self._warm_storage is None:
            raise ValueError("warm_storage required for smart context creation")
        if self._sp_registry is None:
            raise ValueError("sp_registry required for smart context creation")
        
        options = AsyncStorageContextOptions(
            with_cdn=with_cdn,
            force_create_data_set=force_create_data_set,
            metadata=metadata,
            exclude_provider_ids=exclude_provider_ids,
            on_provider_selected=on_provider_selected,
            on_data_set_resolved=on_data_set_resolved,
        )
        
        return await AsyncStorageContext.create_contexts(
            chain=self._chain,
            private_key=self._private_key,
            warm_storage=self._warm_storage,
            sp_registry=self._sp_registry,
            count=count,
            options=options,
        )

    async def get_default_context(self) -> AsyncStorageContext:
        """
        Get the default async storage context, creating one if needed.
        
        Returns:
            The default AsyncStorageContext
        """
        if self._default_context is None:
            self._default_context = await self.get_context()
        return self._default_context

    async def select_providers(
        self,
        count: int = 1,
        filter: Optional[AsyncProviderFilter] = None,
    ) -> List[int]:
        """
        Select providers matching the given criteria.
        
        Args:
            count: Number of providers to select
            filter: Optional filter criteria
            
        Returns:
            List of provider IDs
        """
        if self._sp_registry is None:
            raise ValueError("sp_registry required for provider selection")
        
        filter = filter or AsyncProviderFilter()
        
        # If specific provider IDs requested, validate and return them
        if filter.provider_ids:
            return filter.provider_ids[:count]
        
        # Get all active providers
        providers = await self._sp_registry.get_all_active_providers()
        
        # Filter by exclusions
        if filter.exclude_provider_ids:
            providers = [p for p in providers if p.provider_id not in filter.exclude_provider_ids]
        
        selected = [p.provider_id for p in providers[:count]]
        return selected

    async def find_dataset(
        self,
        provider_id: int,
        metadata: Optional[Dict[str, str]] = None,
    ) -> Optional[AsyncDataSetMatch]:
        """
        Find an existing dataset for the given provider matching metadata.
        
        Args:
            provider_id: Provider to search datasets for
            metadata: Metadata criteria to match
            
        Returns:
            Matching dataset info or None
        """
        if self._warm_storage is None:
            return None
            
        # TODO: Implement dataset search by provider and metadata
        return None

    async def preflight(
        self,
        size_bytes: int,
        provider_count: int = 1,
        duration_epochs: int = 2880,  # ~1 day default
        filter: Optional[AsyncProviderFilter] = None,
        with_cdn: bool = False,
    ) -> AsyncPreflightInfo:
        """
        Estimate storage costs before upload.
        
        Args:
            size_bytes: Size of data to upload
            provider_count: Number of providers for redundancy
            duration_epochs: Storage duration in epochs
            filter: Optional provider filter criteria
            with_cdn: Whether to include CDN in cost estimation
            
        Returns:
            Preflight estimation including costs
        """
        # Select providers
        providers = await self.select_providers(count=provider_count, filter=filter)
        
        # Try to get actual pricing from warm storage
        if self._warm_storage is not None:
            try:
                pricing_rates = await self._warm_storage.get_current_pricing_rates()
                if isinstance(pricing_rates, (list, tuple)) and len(pricing_rates) >= 3:
                    price_per_tib_month = int(pricing_rates[1] if with_cdn else pricing_rates[0])
                    epochs_per_month = int(pricing_rates[2])
                    
                    # Calculate rate per epoch for this size
                    price_per_tib_epoch = price_per_tib_month // epochs_per_month if epochs_per_month else 0
                    estimated_rate = (size_bytes * price_per_tib_epoch * provider_count) // TIB
                    estimated_rate = max(1, estimated_rate)  # minimum 1 unit
                    estimated_total = estimated_rate * duration_epochs
                    
                    return AsyncPreflightInfo(
                        size_bytes=size_bytes,
                        estimated_cost_per_epoch=estimated_rate,
                        estimated_total_cost=estimated_total,
                        duration_epochs=duration_epochs,
                        provider_count=len(providers),
                        providers=providers,
                    )
            except Exception:
                pass
        
        # Fallback: simplified cost calculation
        estimated_rate = size_bytes * provider_count // TIB + 1  # minimum 1 unit
        estimated_total = estimated_rate * duration_epochs
        
        return AsyncPreflightInfo(
            size_bytes=size_bytes,
            estimated_cost_per_epoch=estimated_rate,
            estimated_total_cost=estimated_total,
            duration_epochs=duration_epochs,
            provider_count=len(providers),
            providers=providers,
        )

    async def preflight_upload(
        self,
        size_bytes: int,
        with_cdn: bool = False,
        payments_service=None,
    ) -> dict:
        """
        Comprehensive preflight check including cost estimation and allowance validation.
        
        This method checks:
        1. Storage costs per epoch/day/month
        2. Current service allowances (if payments_service provided)
        3. Whether allowances are sufficient
        
        Args:
            size_bytes: Size of data to upload in bytes
            with_cdn: Whether CDN is enabled
            payments_service: Optional AsyncPaymentsService for allowance checking
            
        Returns:
            Dict with estimated_cost, allowance_check, and any required actions
        """
        result = {
            "estimated_cost": {
                "per_epoch": 0,
                "per_day": 0,
                "per_month": 0,
            },
            "allowance_check": {
                "sufficient": True,
                "message": None,
            },
            "size_bytes": size_bytes,
            "with_cdn": with_cdn,
        }
        
        # Get pricing
        if self._warm_storage is not None:
            try:
                pricing_rates = await self._warm_storage.get_current_pricing_rates()
                if isinstance(pricing_rates, (list, tuple)) and len(pricing_rates) >= 3:
                    price_per_tib_month = int(pricing_rates[1] if with_cdn else pricing_rates[0])
                    epochs_per_month = int(pricing_rates[2])
                    
                    # Calculate costs
                    size_ratio = size_bytes / TIB
                    cost_per_month = int(price_per_tib_month * size_ratio)
                    cost_per_day = cost_per_month // DAYS_PER_MONTH
                    cost_per_epoch = cost_per_month // epochs_per_month if epochs_per_month else 0
                    
                    result["estimated_cost"] = {
                        "per_epoch": cost_per_epoch,
                        "per_day": cost_per_day,
                        "per_month": cost_per_month,
                    }
            except Exception:
                pass
        
        # Check allowances if payments service provided
        if payments_service is not None and self._chain is not None:
            try:
                approval = await payments_service.service_approval(
                    self._chain.contracts.warm_storage
                )
                
                rate_needed = result["estimated_cost"]["per_epoch"]
                # Lockup = rate * lockup_period (typically 10 days)
                lockup_period = EPOCHS_PER_DAY * 10
                lockup_needed = rate_needed * lockup_period
                
                rate_sufficient = approval.rate_allowance >= rate_needed
                lockup_sufficient = approval.lockup_allowance >= lockup_needed
                
                result["allowance_check"] = {
                    "sufficient": rate_sufficient and lockup_sufficient,
                    "is_approved": approval.is_approved,
                    "rate_allowance": approval.rate_allowance,
                    "lockup_allowance": approval.lockup_allowance,
                    "rate_needed": rate_needed,
                    "lockup_needed": lockup_needed,
                    "message": None if (rate_sufficient and lockup_sufficient) else (
                        f"Insufficient allowances: need rate={rate_needed}, lockup={lockup_needed}"
                    ),
                }
                if not approval.is_approved:
                    result["allowance_check"]["sufficient"] = False
                    result["allowance_check"]["message"] = (
                        "Warm Storage operator approval is missing. "
                        "Call payments.set_operator_approval(...) to approve."
                    )
            except Exception as e:
                result["allowance_check"]["message"] = f"Failed to check allowances: {e}"
        
        return result

    async def _preflight_upload_requirements(
        self,
        size_bytes: int,
        with_cdn: bool,
        payments_service=None,
    ) -> None:
        if payments_service is None:
            return
        info = await self.preflight_upload(
            size_bytes=size_bytes,
            with_cdn=with_cdn,
            payments_service=payments_service,
        )
        allowance = info.get("allowance_check", {})
        if not allowance.get("is_approved", True) or not allowance.get("sufficient", True):
            message = allowance.get("message") or "Insufficient allowances for upload."
            raise ValueError(message)

    async def upload(
        self, 
        data: bytes, 
        pdp_endpoint: Optional[str] = None,
        data_set_id: Optional[int] = None, 
        client_data_set_id: Optional[int] = None,
        provider_id: Optional[int] = None,
        metadata: Optional[Dict[str, str]] = None,
        context: Optional[AsyncStorageContext] = None,
        with_cdn: bool = False,
        auto_create_context: bool = True,
        payments_service=None,
    ) -> AsyncUploadResult:
        """
        Upload data to storage asynchronously.
        
        If warm_storage and sp_registry are configured, this method can
        auto-create a context with smart provider selection. Otherwise,
        explicit context parameters are required.
        
        Args:
            data: Bytes to upload
            pdp_endpoint: PDP server endpoint (required if no auto-create)
            data_set_id: Dataset ID (required if no auto-create)
            client_data_set_id: Client dataset ID (required if no auto-create)
            provider_id: Optional provider ID for selection/caching
            metadata: Optional piece metadata
            context: Explicit context to use (overrides other params)
            with_cdn: Enable CDN services (for auto-create)
            auto_create_context: Auto-create context if services available (default: True)
            payments_service: Optional AsyncPaymentsService for preflight allowance checks
            
        Returns:
            Upload result with piece CID and tx hash
        """
        effective_with_cdn = with_cdn
        if context is not None:
            effective_with_cdn = context.with_cdn
        await self._preflight_upload_requirements(
            size_bytes=len(data),
            with_cdn=effective_with_cdn,
            payments_service=payments_service,
        )

        if context is not None:
            return await context.upload(data, metadata=metadata)
        
        # Check for cached context
        if provider_id is not None and provider_id in self._context_cache:
            return await self._context_cache[provider_id].upload(data, metadata=metadata)
        
        # Try auto-create if services are available
        if auto_create_context and self._warm_storage is not None and self._sp_registry is not None:
            ctx = await self.get_context(
                provider_id=provider_id,
                with_cdn=with_cdn,
            )
            return await ctx.upload(data, metadata=metadata)
        
        # Fall back to explicit context creation
        if pdp_endpoint is None or data_set_id is None or client_data_set_id is None:
            raise ValueError(
                "pdp_endpoint, data_set_id, and client_data_set_id required "
                "(or configure warm_storage and sp_registry for auto-creation)"
            )
        
        ctx = self.create_context(pdp_endpoint, data_set_id, client_data_set_id, provider_id)
        return await ctx.upload(data, metadata=metadata)

    async def upload_multi(
        self,
        data: bytes,
        contexts: Sequence[AsyncStorageContext],
        metadata: Optional[Dict[str, str]] = None,
    ) -> List[AsyncUploadResult]:
        """
        Upload data to multiple storage providers for redundancy.
        
        All contexts receive the same data with the same piece CID.
        
        Args:
            data: Bytes to upload
            contexts: Storage contexts for each provider
            metadata: Optional piece metadata
            
        Returns:
            List of upload results (one per context)
        """
        # Upload concurrently to all contexts
        tasks = [ctx.upload(data, metadata=metadata) for ctx in contexts]
        results = await asyncio.gather(*tasks)
        return list(results)

    async def download(
        self, 
        piece_cid: str, 
        pdp_endpoint: Optional[str] = None,
        context: Optional[AsyncStorageContext] = None,
        provider_address: Optional[str] = None,
    ) -> bytes:
        """
        Download data by piece CID asynchronously.
        
        If a retriever is configured, this method can perform SP-agnostic
        downloads by querying the client's datasets to find providers.
        
        Args:
            piece_cid: The piece CID to download
            pdp_endpoint: PDP endpoint to download from (optional if retriever configured)
            context: Explicit context to use
            provider_address: Optional specific provider address for retriever
            
        Returns:
            Downloaded data bytes
        """
        if context is not None:
            return await context.download(piece_cid)
        
        # Try SP-agnostic download using retriever
        if self._retriever is not None:
            from eth_account import Account
            acct = Account.from_key(self._private_key)
            return await self._retriever.fetch_piece(
                piece_cid=piece_cid,
                client_address=acct.address,
                provider_address=provider_address,
            )
        
        # Fall back to explicit endpoint
        if pdp_endpoint is None:
            raise ValueError(
                "pdp_endpoint required (or configure retriever for SP-agnostic downloads)"
            )
        
        ctx = AsyncStorageContext(
            pdp_endpoint=pdp_endpoint,
            chain=self._chain,
            private_key=self._private_key,
            data_set_id=0,
            client_data_set_id=0,
        )
        return await ctx.download(piece_cid)

    async def find_datasets(self, client_address: Optional[str] = None) -> List[dict]:
        """
        Query datasets for a client with enhanced details.
        
        Args:
            client_address: Optional client address. If not provided,
                           uses the address derived from the private key.
                           
        Returns:
            List of enhanced dataset info dictionaries
        """
        if self._warm_storage is None:
            raise ValueError("warm_storage required for find_datasets")
        
        if client_address is None:
            from eth_account import Account
            acct = Account.from_key(self._private_key)
            client_address = acct.address
        
        datasets = await self._warm_storage.get_client_data_sets_with_details(client_address)
        return [
            {
                "data_set_id": ds.data_set_id,
                "client_data_set_id": ds.client_data_set_id,
                "provider_id": ds.provider_id,
                "service_provider": ds.service_provider,
                "payer": ds.payer,
                "payee": ds.payee,
                "active_piece_count": ds.active_piece_count,
                "is_live": ds.is_live,
                "is_managed": ds.is_managed,
                "with_cdn": ds.with_cdn,
                "metadata": ds.metadata,
                "pdp_end_epoch": ds.pdp_end_epoch,
            }
            for ds in datasets
        ]

    async def terminate_data_set(self, data_set_id: int) -> str:
        """
        Terminate a dataset. This also removes all pieces in the dataset.
        
        Args:
            data_set_id: The ID of the dataset to terminate
            
        Returns:
            Transaction hash
        """
        if self._warm_storage is None:
            raise ValueError("warm_storage required for terminate_data_set")
        
        from eth_account import Account
        acct = Account.from_key(self._private_key)
        return await self._warm_storage.terminate_data_set(acct.address, data_set_id)

    async def get_storage_info(self) -> AsyncStorageInfo:
        """
        Get comprehensive information about the storage service.
        
        Returns service pricing, approved providers, contract addresses,
        and configuration parameters.
        
        Returns:
            AsyncStorageInfo with pricing, providers, and service parameters
        """
        if self._warm_storage is None:
            raise ValueError("warm_storage required for get_storage_info")
        if self._sp_registry is None:
            raise ValueError("sp_registry required for get_storage_info")
        
        # Get pricing info
        pricing_rates = await self._warm_storage.get_current_pricing_rates()
        
        # Parse pricing - format may vary, handle common cases
        if isinstance(pricing_rates, (list, tuple)) and len(pricing_rates) >= 4:
            price_no_cdn = int(pricing_rates[0])
            price_with_cdn = int(pricing_rates[1])
            epochs_per_month = int(pricing_rates[2])
            token_address = pricing_rates[3]
        else:
            # Fallback to individual calls
            price_no_cdn = 0
            price_with_cdn = 0
            epochs_per_month = EPOCHS_PER_DAY * DAYS_PER_MONTH
            token_address = ""
        
        # Calculate per-epoch and per-day pricing
        pricing_no_cdn = AsyncStoragePricing(
            per_tib_per_month=price_no_cdn,
            per_tib_per_day=price_no_cdn // DAYS_PER_MONTH if price_no_cdn else 0,
            per_tib_per_epoch=price_no_cdn // epochs_per_month if price_no_cdn and epochs_per_month else 0,
        )
        pricing_with_cdn = AsyncStoragePricing(
            per_tib_per_month=price_with_cdn,
            per_tib_per_day=price_with_cdn // DAYS_PER_MONTH if price_with_cdn else 0,
            per_tib_per_epoch=price_with_cdn // epochs_per_month if price_with_cdn and epochs_per_month else 0,
        )
        
        # Get approved provider IDs
        try:
            approved_ids = await self._warm_storage.get_approved_provider_ids()
        except Exception:
            approved_ids = []
        
        # Get provider details
        providers = []
        for pid in approved_ids:
            try:
                provider = await self._sp_registry.get_provider(pid)
                if provider and provider.is_active:
                    providers.append({
                        "provider_id": provider.provider_id,
                        "service_provider": provider.service_provider,
                        "payee": provider.payee,
                        "name": provider.name,
                        "description": provider.description,
                        "is_active": provider.is_active,
                    })
            except Exception:
                continue
        
        return AsyncStorageInfo(
            pricing_no_cdn=pricing_no_cdn,
            pricing_with_cdn=pricing_with_cdn,
            token_address=str(token_address),
            token_symbol="USDFC",  # Standard token for Filecoin storage
            providers=providers,
            service_parameters=AsyncServiceParameters(
                epochs_per_month=epochs_per_month,
            ),
            approved_provider_ids=approved_ids,
        )
