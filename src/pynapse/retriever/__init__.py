"""
PieceRetriever implementations for flexible piece fetching.

This module provides different strategies for retrieving pieces:
- ChainRetriever: Queries on-chain data to find providers
"""

from .chain import ChainRetriever

__all__ = ["ChainRetriever"]
