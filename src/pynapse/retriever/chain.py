"""
ChainRetriever - Queries on-chain data to find and retrieve pieces.

This retriever uses the Warm Storage service to find service providers
that have the requested piece, then attempts to download from them.
"""
from __future__ import annotations

import concurrent.futures
from typing import List, Optional, Protocol, TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from pynapse.sp_registry import ProviderInfo
    from pynapse.warm_storage import SyncWarmStorageService


class PieceRetriever(Protocol):
    """Protocol for piece retrieval implementations."""
    
    def fetch_piece(
        self,
        piece_cid: str,
        client_address: str,
        provider_address: Optional[str] = None,
    ) -> bytes:
        """Fetch a piece by CID."""
        ...


class ChainRetriever:
    """
    Retriever that queries on-chain data to find providers with the piece.
    
    This is the standard retriever that:
    1. Looks up the client's datasets to find active providers
    2. Tries to download from each provider until one succeeds
    
    Example:
        retriever = ChainRetriever(warm_storage, sp_registry)
        data = retriever.fetch_piece(piece_cid, client_address)
    """
    
    def __init__(
        self,
        warm_storage: "SyncWarmStorageService",
        sp_registry,
        fallback_retriever: Optional[PieceRetriever] = None,
        timeout: float = 30.0,
    ) -> None:
        self._warm_storage = warm_storage
        self._sp_registry = sp_registry
        self._fallback = fallback_retriever
        self._timeout = timeout
        self._client = httpx.Client(timeout=timeout)

    def _get_pdp_endpoint(self, provider_id: int) -> Optional[str]:
        """Get PDP service URL for a provider."""
        try:
            product = self._sp_registry.get_provider_with_product(provider_id, 1)
            for i, key in enumerate(product.product.capability_keys):
                if key == "serviceURL" and i < len(product.product_capability_values):
                    return product.product_capability_values[i]
        except Exception:
            pass
        return None

    def _find_providers(
        self,
        client_address: str,
        provider_address: Optional[str] = None,
    ) -> List["ProviderInfo"]:
        """Find providers that can serve pieces for a client."""
        
        if provider_address is not None:
            # Direct provider case
            provider = self._sp_registry.get_provider_by_address(provider_address)
            if provider is None:
                raise ValueError(f"Provider {provider_address} not found in registry")
            return [provider]
        
        # Get client's datasets with details
        datasets = self._warm_storage.get_client_data_sets_with_details(client_address)
        
        # Filter for live datasets with pieces
        valid_datasets = [
            ds for ds in datasets
            if ds.is_live and ds.active_piece_count > 0
        ]
        
        if not valid_datasets:
            raise ValueError(f"No active datasets with data found for client {client_address}")
        
        # Get unique provider IDs
        unique_provider_ids = list(set(ds.provider_id for ds in valid_datasets))
        
        # Fetch provider info for each
        providers = []
        for pid in unique_provider_ids:
            try:
                provider = self._sp_registry.get_provider(pid)
                if provider and provider.is_active:
                    providers.append(provider)
            except Exception:
                continue
        
        if not providers:
            raise ValueError("No valid providers found")
        
        return providers

    def _try_fetch_from_provider(
        self,
        provider: "ProviderInfo",
        piece_cid: str,
    ) -> Optional[bytes]:
        """Try to fetch a piece from a specific provider."""
        endpoint = self._get_pdp_endpoint(provider.provider_id)
        if not endpoint:
            return None
        
        try:
            # First check if provider has the piece
            find_resp = self._client.get(
                f"{endpoint.rstrip('/')}/pdp/piece",
                params={"pieceCid": piece_cid},
            )
            if find_resp.status_code != 200:
                return None
            
            # Download the piece
            download_resp = self._client.get(
                f"{endpoint.rstrip('/')}/pdp/piece/{piece_cid}",
            )
            if download_resp.status_code == 200:
                return download_resp.content
            
        except Exception:
            pass
        
        return None

    def fetch_piece(
        self,
        piece_cid: str,
        client_address: str,
        provider_address: Optional[str] = None,
        parallel: bool = True,
    ) -> bytes:
        """
        Fetch a piece by CID, trying multiple providers if needed.
        
        Args:
            piece_cid: The piece CID to fetch
            client_address: The client address to look up datasets for
            provider_address: Optional specific provider to use
            parallel: Whether to try providers in parallel (default: True)
            
        Returns:
            The piece data as bytes
            
        Raises:
            ValueError: If piece cannot be found on any provider
        """
        try:
            providers = self._find_providers(client_address, provider_address)
        except ValueError as e:
            if self._fallback:
                return self._fallback.fetch_piece(piece_cid, client_address, provider_address)
            raise
        
        if parallel and len(providers) > 1:
            # Try all providers in parallel, return first success
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(providers)) as executor:
                futures = {
                    executor.submit(self._try_fetch_from_provider, p, piece_cid): p
                    for p in providers
                }
                
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    if result is not None:
                        # Cancel remaining futures
                        for f in futures:
                            f.cancel()
                        return result
        else:
            # Try providers sequentially
            for provider in providers:
                result = self._try_fetch_from_provider(provider, piece_cid)
                if result is not None:
                    return result
        
        # All providers failed
        if self._fallback:
            return self._fallback.fetch_piece(piece_cid, client_address, provider_address)
        
        raise ValueError(f"Failed to retrieve piece {piece_cid} from any provider")

    def close(self) -> None:
        """Close the HTTP client."""
        self._client.close()

    def __enter__(self) -> "ChainRetriever":
        return self

    def __exit__(self, *args) -> None:
        self.close()
