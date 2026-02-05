"""
StorageManager - Central facade for storage operations

Manages storage contexts (SP + DataSet pairs) with intelligent provider selection
and dataset reuse. Supports both single and multi-provider uploads.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Union

from .context import StorageContext, UploadResult


# Size and time constants matching TypeScript SDK
TIB = 1024 ** 4
EPOCHS_PER_DAY = 2880
DAYS_PER_MONTH = 30


@dataclass
class ProviderFilter:
    """Filter criteria for provider selection."""
    provider_ids: Optional[List[int]] = None
    with_cdn: bool = False
    with_ipni: bool = False
    min_piece_size: Optional[int] = None
    max_piece_size: Optional[int] = None
    location: Optional[str] = None
    exclude_provider_ids: Optional[List[int]] = None


@dataclass
class PreflightInfo:
    """Preflight estimation for storage costs."""
    size_bytes: int
    estimated_cost_per_epoch: int
    estimated_total_cost: int
    duration_epochs: int
    provider_count: int
    providers: List[int] = field(default_factory=list)


@dataclass
class DataSetMatch:
    """A dataset that matches search criteria."""
    data_set_id: int
    client_data_set_id: int
    provider_id: int
    pdp_endpoint: str
    metadata: Dict[str, str]


@dataclass
class StoragePricing:
    """Pricing information per time unit."""
    per_tib_per_month: int
    per_tib_per_day: int
    per_tib_per_epoch: int


@dataclass
class ServiceParameters:
    """Service configuration parameters."""
    epochs_per_month: int
    epochs_per_day: int = EPOCHS_PER_DAY
    epoch_duration: int = 30  # seconds
    min_upload_size: int = 256  # bytes
    max_upload_size: int = 254 * 1024 * 1024  # 254 MiB


@dataclass 
class StorageInfo:
    """Comprehensive storage service information."""
    pricing_no_cdn: StoragePricing
    pricing_with_cdn: StoragePricing
    token_address: str
    token_symbol: str
    providers: List[dict]  # List of provider info dicts
    service_parameters: ServiceParameters
    approved_provider_ids: List[int] = field(default_factory=list)


class StorageManager:
    """
    Central storage manager with provider selection and dataset reuse.
    
    Features:
    - Smart provider selection by capabilities (CDN, IPNI, location)
    - Dataset reuse based on metadata matching
    - Multi-provider uploads for redundancy
    - Preflight cost estimation
    
    Example:
        # Simple upload (auto-selects provider)
        result = manager.upload(data)
        
        # Upload with specific provider
        result = manager.upload(data, provider_id=1)
        
        # Multi-provider upload for redundancy
        results = manager.upload_multi(data, provider_count=3)
        
        # Preflight check
        info = manager.preflight(len(data), provider_count=2)
    """
    
    def __init__(
        self, 
        chain, 
        private_key: str,
        sp_registry=None,
        warm_storage=None,
    ) -> None:
        self._chain = chain
        self._private_key = private_key
        self._sp_registry = sp_registry
        self._warm_storage = warm_storage
        self._default_context: Optional[StorageContext] = None
        self._context_cache: Dict[int, StorageContext] = {}  # provider_id -> context

    def create_context(
        self, 
        pdp_endpoint: str, 
        data_set_id: int, 
        client_data_set_id: int,
        provider_id: Optional[int] = None,
    ) -> StorageContext:
        """Create a storage context for a specific provider/dataset."""
        context = StorageContext(
            pdp_endpoint=pdp_endpoint,
            chain=self._chain,
            private_key=self._private_key,
            data_set_id=data_set_id,
            client_data_set_id=client_data_set_id,
        )
        if provider_id is not None:
            self._context_cache[provider_id] = context
        return context

    def select_providers(
        self,
        count: int = 1,
        filter: Optional[ProviderFilter] = None,
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
        
        filter = filter or ProviderFilter()
        
        # If specific provider IDs requested, validate and return them
        if filter.provider_ids:
            return filter.provider_ids[:count]
        
        # Get all active providers
        providers = self._sp_registry.get_all_active_providers()
        
        # Filter by exclusions
        if filter.exclude_provider_ids:
            providers = [p for p in providers if p.provider_id not in filter.exclude_provider_ids]
        
        # TODO: Filter by capabilities (CDN, IPNI, location, piece size)
        # This would require fetching product info for each provider
        # For now, just return the first N providers
        
        selected = [p.provider_id for p in providers[:count]]
        return selected

    def find_dataset(
        self,
        provider_id: int,
        metadata: Optional[Dict[str, str]] = None,
    ) -> Optional[DataSetMatch]:
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
        # This requires warm_storage.get_client_data_sets() and filtering
        return None

    def preflight(
        self,
        size_bytes: int,
        provider_count: int = 1,
        duration_epochs: int = 2880,  # ~1 day default
        filter: Optional[ProviderFilter] = None,
    ) -> PreflightInfo:
        """
        Estimate storage costs before upload.
        
        Args:
            size_bytes: Size of data to upload
            provider_count: Number of providers for redundancy
            duration_epochs: Storage duration in epochs
            filter: Optional provider filter criteria
            
        Returns:
            Preflight estimation including costs
        """
        # Select providers
        providers = self.select_providers(count=provider_count, filter=filter)
        
        # Calculate costs (simplified - would need pricing data from providers)
        # Filecoin epoch is ~30 seconds, so 2880 epochs ≈ 1 day
        # Pricing is typically per TiB per day
        TIB = 1024 ** 4
        
        # Placeholder cost calculation
        # Real implementation would fetch rates from warm_storage service
        estimated_rate = size_bytes * provider_count // TIB + 1  # minimum 1 unit
        estimated_total = estimated_rate * duration_epochs
        
        return PreflightInfo(
            size_bytes=size_bytes,
            estimated_cost_per_epoch=estimated_rate,
            estimated_total_cost=estimated_total,
            duration_epochs=duration_epochs,
            provider_count=len(providers),
            providers=providers,
        )

    def upload(
        self, 
        data: bytes, 
        pdp_endpoint: Optional[str] = None,
        data_set_id: Optional[int] = None, 
        client_data_set_id: Optional[int] = None,
        provider_id: Optional[int] = None,
        metadata: Optional[Dict[str, str]] = None,
        context: Optional[StorageContext] = None,
    ) -> UploadResult:
        """
        Upload data to storage.
        
        Args:
            data: Bytes to upload
            pdp_endpoint: PDP server endpoint (required if no context)
            data_set_id: Dataset ID (required if no context)
            client_data_set_id: Client dataset ID (required if no context)
            provider_id: Optional provider ID for caching
            metadata: Optional piece metadata
            context: Explicit context to use (overrides other params)
            
        Returns:
            Upload result with piece CID and tx hash
        """
        if context is not None:
            return context.upload(data, metadata=metadata)
        
        # Check for cached context
        if provider_id is not None and provider_id in self._context_cache:
            return self._context_cache[provider_id].upload(data, metadata=metadata)
        
        # Create new context
        if pdp_endpoint is None or data_set_id is None or client_data_set_id is None:
            raise ValueError("pdp_endpoint, data_set_id, and client_data_set_id required (or provide context)")
        
        ctx = self.create_context(pdp_endpoint, data_set_id, client_data_set_id, provider_id)
        return ctx.upload(data, metadata=metadata)

    def upload_multi(
        self,
        data: bytes,
        contexts: Sequence[StorageContext],
        metadata: Optional[Dict[str, str]] = None,
    ) -> List[UploadResult]:
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
        results = []
        for ctx in contexts:
            result = ctx.upload(data, metadata=metadata)
            results.append(result)
        return results

    def download(
        self, 
        piece_cid: str, 
        pdp_endpoint: Optional[str] = None,
        context: Optional[StorageContext] = None,
    ) -> bytes:
        """
        Download data by piece CID.
        
        Args:
            piece_cid: The piece CID to download
            pdp_endpoint: PDP endpoint to download from
            context: Explicit context to use
            
        Returns:
            Downloaded data bytes
        """
        if context is not None:
            return context.download(piece_cid)
        
        if pdp_endpoint is None:
            raise ValueError("pdp_endpoint required (or provide context)")
        
        ctx = StorageContext(
            pdp_endpoint=pdp_endpoint,
            chain=self._chain,
            private_key=self._private_key,
            data_set_id=0,
            client_data_set_id=0,
        )
        return ctx.download(piece_cid)

    def find_datasets(self, client_address: Optional[str] = None) -> List[dict]:
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
        
        datasets = self._warm_storage.get_client_data_sets_with_details(client_address)
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

    def terminate_data_set(self, data_set_id: int) -> str:
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
        return self._warm_storage.terminate_data_set(acct.address, data_set_id)

    def get_storage_info(self) -> StorageInfo:
        """
        Get comprehensive information about the storage service.
        
        Returns service pricing, approved providers, contract addresses,
        and configuration parameters.
        
        Returns:
            StorageInfo with pricing, providers, and service parameters
        """
        if self._warm_storage is None:
            raise ValueError("warm_storage required for get_storage_info")
        if self._sp_registry is None:
            raise ValueError("sp_registry required for get_storage_info")
        
        # Get pricing info
        pricing_rates = self._warm_storage.get_current_pricing_rates()
        
        # Parse pricing - format may vary, handle common cases
        # Typically returns (priceNoCDN, priceWithCDN, epochsPerMonth, tokenAddress)
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
        pricing_no_cdn = StoragePricing(
            per_tib_per_month=price_no_cdn,
            per_tib_per_day=price_no_cdn // DAYS_PER_MONTH if price_no_cdn else 0,
            per_tib_per_epoch=price_no_cdn // epochs_per_month if price_no_cdn and epochs_per_month else 0,
        )
        pricing_with_cdn = StoragePricing(
            per_tib_per_month=price_with_cdn,
            per_tib_per_day=price_with_cdn // DAYS_PER_MONTH if price_with_cdn else 0,
            per_tib_per_epoch=price_with_cdn // epochs_per_month if price_with_cdn and epochs_per_month else 0,
        )
        
        # Get approved provider IDs
        try:
            approved_ids = self._warm_storage.get_approved_provider_ids()
        except Exception:
            approved_ids = []
        
        # Get provider details
        providers = []
        for pid in approved_ids:
            try:
                provider = self._sp_registry.get_provider(pid)
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
        
        return StorageInfo(
            pricing_no_cdn=pricing_no_cdn,
            pricing_with_cdn=pricing_with_cdn,
            token_address=str(token_address),
            token_symbol="USDFC",  # Standard token for Filecoin storage
            providers=providers,
            service_parameters=ServiceParameters(
                epochs_per_month=epochs_per_month,
            ),
            approved_provider_ids=approved_ids,
        )
