from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from eth_account import Account
from web3 import AsyncWeb3, Web3

from pynapse.core.chains import CALIBRATION, MAINNET, Chain, as_chain
from pynapse.evm import AsyncEVMClient, SyncEVMClient
from pynapse.payments import AsyncPaymentsService, SyncPaymentsService
from pynapse.filbeam import FilBeamService
from pynapse.retriever import ChainRetriever, AsyncChainRetriever
from pynapse.session import AsyncSessionKeyRegistry, SyncSessionKeyRegistry
from pynapse.storage import StorageManager, AsyncStorageManager
from pynapse.sp_registry import AsyncSPRegistryService, SyncSPRegistryService
from pynapse.warm_storage import AsyncWarmStorageService, SyncWarmStorageService


class Synapse:
    def __init__(self, web3: Web3, chain: Chain, account_address: str, private_key: Optional[str] = None) -> None:
        self._web3 = web3
        self._chain = chain
        self._account = account_address
        self._private_key = private_key
        self._payments = SyncPaymentsService(web3, chain, account_address, private_key)
        self._providers = SyncSPRegistryService(web3, chain, private_key)
        self._warm_storage = SyncWarmStorageService(web3, chain, private_key)
        # Create retriever for SP-agnostic downloads
        self._retriever = ChainRetriever(self._warm_storage, self._providers)
        # Wire up storage manager with warm_storage, sp_registry, and retriever
        self._storage = StorageManager(
            chain=chain,
            private_key=private_key,
            sp_registry=self._providers,
            warm_storage=self._warm_storage,
            retriever=self._retriever,
        )
        self._session_registry = SyncSessionKeyRegistry(web3, chain, private_key)
        self._filbeam = FilBeamService(chain)

    @classmethod
    def create(cls, rpc_url: str, chain: Chain | str | int = CALIBRATION, private_key: Optional[str] = None) -> "Synapse":
        client = SyncEVMClient.from_rpc_url(rpc_url)
        chain_obj = as_chain(chain)
        if private_key is None:
            raise ValueError("private_key required to create Synapse")
        account = Account.from_key(private_key)
        return cls(client.web3, chain_obj, account.address, private_key)

    @property
    def web3(self) -> Web3:
        return self._web3

    @property
    def chain(self) -> Chain:
        return self._chain

    @property
    def account(self) -> str:
        return self._account

    @property
    def payments(self) -> SyncPaymentsService:
        return self._payments

    @property
    def providers(self) -> SyncSPRegistryService:
        return self._providers

    @property
    def warm_storage(self) -> SyncWarmStorageService:
        return self._warm_storage

    @property
    def storage(self) -> StorageManager:
        return self._storage

    @property
    def session_registry(self) -> SyncSessionKeyRegistry:
        return self._session_registry

    @property
    def filbeam(self) -> FilBeamService:
        return self._filbeam

    @property
    def retriever(self) -> ChainRetriever:
        return self._retriever


class AsyncSynapse:
    """
    Async Synapse client for Filecoin Onchain Cloud.
    
    Provides full async/await support for Python async applications.
    
    Example:
        synapse = await AsyncSynapse.create(rpc_url, chain, private_key)
        
        # Upload data
        result = await synapse.storage.upload(data)
        
        # Download data
        data = await synapse.storage.download(piece_cid)
    """
    
    def __init__(self, web3: AsyncWeb3, chain: Chain, account_address: str, private_key: Optional[str] = None) -> None:
        self._web3 = web3
        self._chain = chain
        self._account = account_address
        self._private_key = private_key
        self._payments = AsyncPaymentsService(web3, chain, account_address, private_key)
        self._providers = AsyncSPRegistryService(web3, chain, private_key)
        self._warm_storage = AsyncWarmStorageService(web3, chain, private_key)
        # Create async retriever for SP-agnostic downloads
        self._retriever = AsyncChainRetriever(self._warm_storage, self._providers)
        # Wire up async storage manager with warm_storage, sp_registry, and retriever
        self._storage = AsyncStorageManager(
            chain=chain,
            private_key=private_key,
            sp_registry=self._providers,
            warm_storage=self._warm_storage,
            retriever=self._retriever,
        )
        self._session_registry = AsyncSessionKeyRegistry(web3, chain, private_key)
        self._filbeam = FilBeamService(chain)

    @classmethod
    async def create(
        cls, rpc_url: str, chain: Chain | str | int = CALIBRATION, private_key: Optional[str] = None
    ) -> "AsyncSynapse":
        """
        Create an async Synapse client.
        
        Args:
            rpc_url: RPC URL for the Filecoin chain
            chain: Chain configuration (CALIBRATION, MAINNET, or chain ID)
            private_key: Private key for signing transactions
            
        Returns:
            Configured AsyncSynapse instance
        """
        client = AsyncEVMClient.from_rpc_url(rpc_url)
        chain_obj = as_chain(chain)
        if private_key is None:
            raise ValueError("private_key required to create AsyncSynapse")
        account = Account.from_key(private_key)
        return cls(client.web3, chain_obj, account.address, private_key)

    @property
    def web3(self) -> AsyncWeb3:
        return self._web3

    @property
    def chain(self) -> Chain:
        return self._chain

    @property
    def account(self) -> str:
        return self._account

    @property
    def payments(self) -> AsyncPaymentsService:
        return self._payments

    @property
    def providers(self) -> AsyncSPRegistryService:
        return self._providers

    @property
    def warm_storage(self) -> AsyncWarmStorageService:
        return self._warm_storage

    @property
    def storage(self) -> AsyncStorageManager:
        """Get the async storage manager for upload/download operations."""
        return self._storage

    @property
    def session_registry(self) -> AsyncSessionKeyRegistry:
        return self._session_registry

    @property
    def filbeam(self) -> FilBeamService:
        return self._filbeam

    @property
    def retriever(self) -> AsyncChainRetriever:
        """Get the async retriever for SP-agnostic downloads."""
        return self._retriever
