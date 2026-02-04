from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from eth_account import Account
from web3 import AsyncWeb3, Web3

from pynapse.contracts import SESSION_KEY_REGISTRY_ABI
from pynapse.core.chains import Chain
from .permissions import SESSION_KEY_PERMISSIONS


class SyncSessionKeyRegistry:
    def __init__(self, web3: Web3, chain: Chain, private_key: Optional[str] = None) -> None:
        self._web3 = web3
        self._chain = chain
        self._private_key = private_key
        self._contract = web3.eth.contract(address=chain.contracts.session_key_registry, abi=SESSION_KEY_REGISTRY_ABI)

    def authorization_expiry(self, address: str, session_key_address: str, permission: str) -> int:
        perm = SESSION_KEY_PERMISSIONS[permission]
        return int(self._contract.functions.authorizationExpiry(address, session_key_address, perm).call())

    def login(self, account: str, session_key_address: str, expires_at: int, permissions: Iterable[str], origin: str) -> str:
        if not self._private_key:
            raise ValueError("private_key required")
        perm_hashes = [SESSION_KEY_PERMISSIONS[p] for p in permissions]
        txn = self._contract.functions.login(session_key_address, expires_at, perm_hashes, origin).build_transaction(
            {
                "from": account,
                "nonce": self._web3.eth.get_transaction_count(account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    def revoke(self, account: str, session_key_address: str, permissions: Iterable[str], origin: str) -> str:
        if not self._private_key:
            raise ValueError("private_key required")
        perm_hashes = [SESSION_KEY_PERMISSIONS[p] for p in permissions]
        txn = self._contract.functions.revoke(session_key_address, perm_hashes, origin).build_transaction(
            {
                "from": account,
                "nonce": self._web3.eth.get_transaction_count(account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()


class AsyncSessionKeyRegistry:
    def __init__(self, web3: AsyncWeb3, chain: Chain, private_key: Optional[str] = None) -> None:
        self._web3 = web3
        self._chain = chain
        self._private_key = private_key
        self._contract = web3.eth.contract(address=chain.contracts.session_key_registry, abi=SESSION_KEY_REGISTRY_ABI)

    async def authorization_expiry(self, address: str, session_key_address: str, permission: str) -> int:
        perm = SESSION_KEY_PERMISSIONS[permission]
        return int(await self._contract.functions.authorizationExpiry(address, session_key_address, perm).call())

    async def login(self, account: str, session_key_address: str, expires_at: int, permissions: Iterable[str], origin: str) -> str:
        if not self._private_key:
            raise ValueError("private_key required")
        perm_hashes = [SESSION_KEY_PERMISSIONS[p] for p in permissions]
        txn = await self._contract.functions.login(session_key_address, expires_at, perm_hashes, origin).build_transaction(
            {
                "from": account,
                "nonce": await self._web3.eth.get_transaction_count(account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    async def revoke(self, account: str, session_key_address: str, permissions: Iterable[str], origin: str) -> str:
        if not self._private_key:
            raise ValueError("private_key required")
        perm_hashes = [SESSION_KEY_PERMISSIONS[p] for p in permissions]
        txn = await self._contract.functions.revoke(session_key_address, perm_hashes, origin).build_transaction(
            {
                "from": account,
                "nonce": await self._web3.eth.get_transaction_count(account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()
