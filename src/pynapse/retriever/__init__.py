"""
PieceRetriever implementations for flexible piece fetching.

This module provides different strategies for retrieving pieces:
- ChainRetriever: Queries on-chain data to find providers (sync)
- AsyncChainRetriever: Async version for Python async/await patterns
"""

from .chain import ChainRetriever
from .async_chain import AsyncChainRetriever

__all__ = ["ChainRetriever", "AsyncChainRetriever"]
