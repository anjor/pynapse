from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from web3 import AsyncWeb3, Web3
from web3.providers.async_rpc import AsyncHTTPProvider
from web3.providers.rpc import HTTPProvider


@dataclass
class SyncEVMClient:
    web3: Web3

    @classmethod
    def from_rpc_url(cls, rpc_url: str) -> "SyncEVMClient":
        return cls(web3=Web3(HTTPProvider(rpc_url)))


@dataclass
class AsyncEVMClient:
    web3: AsyncWeb3

    @classmethod
    def from_rpc_url(cls, rpc_url: str) -> "AsyncEVMClient":
        return cls(web3=AsyncWeb3(AsyncHTTPProvider(rpc_url)))
