from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from eth_account import Account
from web3 import AsyncWeb3, Web3

from pynapse.core.chains import CALIBRATION, MAINNET, Chain, as_chain
from pynapse.evm import AsyncEVMClient, SyncEVMClient
from pynapse.payments import AsyncPaymentsService, SyncPaymentsService


class Synapse:
    def __init__(self, web3: Web3, chain: Chain, account_address: str, private_key: Optional[str] = None) -> None:
        self._web3 = web3
        self._chain = chain
        self._account = account_address
        self._private_key = private_key
        self._payments = SyncPaymentsService(web3, chain, account_address, private_key)

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


class AsyncSynapse:
    def __init__(self, web3: AsyncWeb3, chain: Chain, account_address: str, private_key: Optional[str] = None) -> None:
        self._web3 = web3
        self._chain = chain
        self._account = account_address
        self._private_key = private_key
        self._payments = AsyncPaymentsService(web3, chain, account_address, private_key)

    @classmethod
    async def create(
        cls, rpc_url: str, chain: Chain | str | int = CALIBRATION, private_key: Optional[str] = None
    ) -> "AsyncSynapse":
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
