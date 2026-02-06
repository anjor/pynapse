"""
LangChain integration for pynapse.

This module provides LangChain-compatible components for storing and loading
documents on Filecoin via the Synapse SDK.

Installation:
    pip install synapse-filecoin-sdk[langchain]

Usage:
    from pynapse.integrations.langchain import FilecoinDocumentLoader, FilecoinStorageTool
    
    # Load documents from Filecoin
    loader = FilecoinDocumentLoader(
        rpc_url="https://api.node.glif.io/rpc/v1",
        chain="mainnet",
        private_key=PRIVATE_KEY
    )
    docs = await loader.aload(piece_cid="baga6ea4seaq...")
    
    # Store documents on Filecoin (as a LangChain tool for agents)
    tool = FilecoinStorageTool(
        rpc_url="https://api.node.glif.io/rpc/v1",
        chain="mainnet",
        private_key=PRIVATE_KEY
    )
    result = await tool._arun(content="Hello, Filecoin!")
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Type

try:
    from langchain_core.documents import Document
    from langchain_core.document_loaders import BaseLoader
    from langchain_core.tools import BaseTool
    from pydantic import BaseModel, Field
except ImportError as e:
    raise ImportError(
        "LangChain dependencies not installed. "
        "Install with: pip install synapse-filecoin-sdk[langchain]"
    ) from e

from pynapse import AsyncSynapse


class FilecoinDocumentLoader(BaseLoader):
    """Load documents from Filecoin using pynapse.
    
    This loader retrieves data stored on Filecoin by Piece CID and converts
    it into LangChain Document objects.
    
    Example:
        loader = FilecoinDocumentLoader(
            rpc_url="https://api.node.glif.io/rpc/v1",
            chain="mainnet",
            private_key="0x..."
        )
        docs = await loader.aload(piece_cid="baga6ea4seaq...")
    """
    
    def __init__(
        self,
        rpc_url: str,
        chain: str = "mainnet",
        private_key: Optional[str] = None,
    ):
        """Initialize the Filecoin document loader.
        
        Args:
            rpc_url: RPC URL for Filecoin node
            chain: Network name ("mainnet" or "calibration")
            private_key: Wallet private key (optional for read-only operations)
        """
        self.rpc_url = rpc_url
        self.chain = chain
        self.private_key = private_key
        self._synapse: Optional[AsyncSynapse] = None
    
    async def _get_synapse(self) -> AsyncSynapse:
        """Get or create AsyncSynapse instance."""
        if self._synapse is None:
            self._synapse = await AsyncSynapse.create(
                rpc_url=self.rpc_url,
                chain=self.chain,
                private_key=self.private_key,
            )
        return self._synapse
    
    def load(self) -> List[Document]:
        """Synchronous load - not supported, use aload instead."""
        raise NotImplementedError(
            "FilecoinDocumentLoader is async-only. Use aload() instead."
        )
    
    async def aload(self, piece_cid: str) -> List[Document]:
        """Load a document from Filecoin by Piece CID.
        
        Args:
            piece_cid: The Piece CID of the stored data
            
        Returns:
            List containing a single Document with the retrieved content
        """
        synapse = await self._get_synapse()
        ctx = await synapse.storage.get_context()
        
        # Download the data
        data = await ctx.download(piece_cid)
        
        # Try to decode as text, fall back to repr for binary
        try:
            content = data.decode("utf-8")
        except UnicodeDecodeError:
            content = repr(data)
        
        return [
            Document(
                page_content=content,
                metadata={
                    "source": f"filecoin://{piece_cid}",
                    "piece_cid": piece_cid,
                    "chain": self.chain,
                    "size": len(data),
                }
            )
        ]


class FilecoinStorageInput(BaseModel):
    """Input schema for FilecoinStorageTool."""
    content: str = Field(description="The text content to store on Filecoin")


class FilecoinStorageTool(BaseTool):
    """LangChain tool for storing data on Filecoin.
    
    This tool allows LangChain agents to store arbitrary text content on
    Filecoin and returns the Piece CID for later retrieval.
    
    Example:
        tool = FilecoinStorageTool(
            rpc_url="https://api.node.glif.io/rpc/v1",
            chain="mainnet",
            private_key="0x..."
        )
        # Use in an agent or call directly
        result = await tool._arun(content="Store this on Filecoin!")
    """
    
    name: str = "filecoin_storage"
    description: str = (
        "Store text content on Filecoin decentralized storage. "
        "Returns the Piece CID which can be used to retrieve the content later. "
        "Use this when you need to permanently store data on decentralized storage."
    )
    args_schema: Type[BaseModel] = FilecoinStorageInput
    
    rpc_url: str
    chain: str = "mainnet"
    private_key: str
    _synapse: Optional[AsyncSynapse] = None
    
    class Config:
        arbitrary_types_allowed = True
    
    async def _get_synapse(self) -> AsyncSynapse:
        """Get or create AsyncSynapse instance."""
        if self._synapse is None:
            self._synapse = await AsyncSynapse.create(
                rpc_url=self.rpc_url,
                chain=self.chain,
                private_key=self.private_key,
            )
        return self._synapse
    
    def _run(self, content: str) -> str:
        """Synchronous run - wraps async implementation."""
        return asyncio.run(self._arun(content=content))
    
    async def _arun(self, content: str) -> str:
        """Store content on Filecoin and return the Piece CID.
        
        Args:
            content: Text content to store
            
        Returns:
            JSON string with piece_cid, size, and tx_hash
        """
        synapse = await self._get_synapse()
        ctx = await synapse.storage.get_context()
        
        # Encode content and ensure minimum size (256 bytes)
        data = content.encode("utf-8")
        if len(data) < 256:
            data = data + b'\x00' * (256 - len(data))
        
        # Upload to Filecoin
        result = await ctx.upload(data)
        
        return (
            f'{{"piece_cid": "{result.piece_cid}", '
            f'"size": {result.size}, '
            f'"tx_hash": "{result.tx_hash}"}}'
        )


__all__ = [
    "FilecoinDocumentLoader",
    "FilecoinStorageTool",
    "FilecoinStorageInput",
]
