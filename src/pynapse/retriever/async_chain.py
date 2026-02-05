"""
AsyncChainRetriever - Async queries to on-chain data for piece retrieval.

This retriever uses the Async Warm Storage service to find service providers
that have the requested piece, then attempts to download from them.
"""
from __future__ import annotations

import asyncio
from typing import List, Optional, Protocol, TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from pynapse.sp_registry import AsyncSPRegistryService, ProviderInfo
    from pynapse.warm_storage import AsyncWarmStorageService


class AsyncPieceRetriever(Protocol):
    """Protocol for async piece retrieval implementations."""
    
    async def fetch_piece(
        self,
        piece_cid: str,
        client_address: str,
        provider_address: Optional[str] = None,
    ) -> bytes:
        """Fetch a piece by CID asynchronously."""
        ...


class AsyncChainRetriever:
    """
    Async retriever that queries on-chain data to find providers with the piece.
    
    This is the standard async retriever that:
    1. Looks up the client's datasets to find active providers
    2. Tries to download from each provider until one succeeds
    
    Example:
        retriever = AsyncChainRetriever(warm_storage, sp_registry)
        data = await retriever.fetch_piece(piece_cid, client_address)
    """
    
    def __init__(
        self,
        warm_storage: "AsyncWarmStorageService",
        sp_registry: "AsyncSPRegistryService",
        fallback_retriever: Optional[AsyncPieceRetriever] = None,
        timeout: float = 30.0,
    ) -> None:
        self._warm_storage = warm_storage
        self._sp_registry = sp_registry
        self._fallback = fallback_retriever
        self._timeout = timeout

    async def _get_pdp_endpoint(self, provider_id: int) -> Optional[str]:
        """Get PDP service URL for a provider."""
        try:
            product = await self._sp_registry.get_provider_with_product(provider_id, 1)
            for i, key in enumerate(product.product.capability_keys):
                if key == "serviceURL" and i < len(product.product_capability_values):
                    return product.product_capability_values[i]
        except Exception:
            pass
        return None

    async def _find_providers(
        self,
        client_address: str,
        provider_address: Optional[str] = None,
    ) -> List["ProviderInfo"]:
        """Find providers that can serve pieces for a client."""
        
        if provider_address is not None:
            # Direct provider case
            provider = await self._sp_registry.get_provider_by_address(provider_address)
            if provider is None:
                raise ValueError(f"Provider {provider_address} not found in registry")
            return [provider]
        
        # Get client's datasets with details
        datasets = await self._warm_storage.get_client_data_sets_with_details(client_address)
        
        # Filter for live datasets with pieces
        valid_datasets = [
            ds for ds in datasets
            if ds.is_live and ds.active_piece_count > 0
        ]
        
        if not valid_datasets:
            raise ValueError(f"No active datasets with data found for client {client_address}")
        
        # Get unique provider IDs
        unique_provider_ids = list(set(ds.provider_id for ds in valid_datasets))
        
        # Fetch provider info for each concurrently
        async def get_provider_if_active(pid: int) -> Optional["ProviderInfo"]:
            try:
                provider = await self._sp_registry.get_provider(pid)
                if provider and provider.is_active:
                    return provider
            except Exception:
                pass
            return None
        
        results = await asyncio.gather(*[get_provider_if_active(pid) for pid in unique_provider_ids])
        providers = [p for p in results if p is not None]
        
        if not providers:
            raise ValueError("No valid providers found")
        
        return providers

    async def _try_fetch_from_provider(
        self,
        provider: "ProviderInfo",
        piece_cid: str,
    ) -> Optional[bytes]:
        """Try to fetch a piece from a specific provider."""
        endpoint = await self._get_pdp_endpoint(provider.provider_id)
        if not endpoint:
            return None
        
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                # First check if provider has the piece
                find_resp = await client.get(
                    f"{endpoint.rstrip('/')}/pdp/piece",
                    params={"pieceCid": piece_cid},
                )
                if find_resp.status_code != 200:
                    return None
                
                # Download the piece
                download_resp = await client.get(
                    f"{endpoint.rstrip('/')}/pdp/piece/{piece_cid}",
                )
                if download_resp.status_code == 200:
                    return download_resp.content
            
        except Exception:
            pass
        
        return None

    async def fetch_piece(
        self,
        piece_cid: str,
        client_address: str,
        provider_address: Optional[str] = None,
        parallel: bool = True,
    ) -> bytes:
        """
        Fetch a piece by CID.
        
        Args:
            piece_cid: The piece CID to fetch
            client_address: The client address to look up datasets for
            provider_address: Optional specific provider address
            parallel: Whether to try providers in parallel (default: True)
            
        Returns:
            The piece data as bytes
            
        Raises:
            ValueError: If piece cannot be found
        """
        providers = await self._find_providers(client_address, provider_address)
        
        if parallel:
            # Try all providers in parallel, return first success
            async def try_provider(provider: "ProviderInfo") -> Optional[bytes]:
                return await self._try_fetch_from_provider(provider, piece_cid)
            
            # Use asyncio.as_completed for first success
            tasks = [asyncio.create_task(try_provider(p)) for p in providers]
            
            for completed_task in asyncio.as_completed(tasks):
                try:
                    result = await completed_task
                    if result is not None:
                        # Cancel remaining tasks
                        for task in tasks:
                            task.cancel()
                        return result
                except Exception:
                    continue
            
        else:
            # Try providers sequentially
            for provider in providers:
                data = await self._try_fetch_from_provider(provider, piece_cid)
                if data is not None:
                    return data
        
        # Try fallback if configured
        if self._fallback is not None:
            return await self._fallback.fetch_piece(piece_cid, client_address, provider_address)
        
        raise ValueError(f"Piece {piece_cid} not found on any provider")

    async def fetch_pieces(
        self,
        piece_cids: List[str],
        client_address: str,
        provider_address: Optional[str] = None,
    ) -> List[bytes]:
        """
        Fetch multiple pieces concurrently.
        
        Args:
            piece_cids: List of piece CIDs to fetch
            client_address: The client address to look up datasets for
            provider_address: Optional specific provider address
            
        Returns:
            List of piece data as bytes (in same order as input CIDs)
            
        Raises:
            ValueError: If any piece cannot be found
        """
        tasks = [
            self.fetch_piece(cid, client_address, provider_address)
            for cid in piece_cids
        ]
        return list(await asyncio.gather(*tasks))
