from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from eth_account import Account
from web3 import AsyncWeb3, Web3

from pynapse.contracts import ERC20_ABI, PAYMENTS_ABI
from pynapse.core.chains import Chain
from pynapse.utils.constants import TOKENS


@dataclass
class AccountInfo:
    funds: int
    lockup_current: int
    lockup_rate: int
    lockup_last_settled_at: int
    funded_until_epoch: int
    available_funds: int
    current_lockup_rate: int


class SyncPaymentsService:
    def __init__(self, web3: Web3, chain: Chain, account_address: str, private_key: Optional[str] = None) -> None:
        self._web3 = web3
        self._chain = chain
        self._account = account_address
        self._private_key = private_key
        self._payments = self._web3.eth.contract(address=chain.contracts.payments, abi=PAYMENTS_ABI)
        self._erc20 = self._web3.eth.contract(address=chain.contracts.usdfc, abi=ERC20_ABI)

    def balance(self, token: str = TOKENS["USDFC"]) -> int:
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for payments contract balance")
        funds, _, _, _ = self._payments.functions.accounts(self._chain.contracts.usdfc, self._account).call()
        return int(funds)

    def account_info(self, token: str = TOKENS["USDFC"]) -> AccountInfo:
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for payments contract account info")
        funds, lockup_current, lockup_rate, lockup_last = self._payments.functions.accounts(
            self._chain.contracts.usdfc, self._account
        ).call()
        funded_until, _, available, current_lockup_rate = self._payments.functions.getAccountInfoIfSettled(
            self._chain.contracts.usdfc, self._account
        ).call()
        return AccountInfo(
            funds=int(funds),
            lockup_current=int(lockup_current),
            lockup_rate=int(lockup_rate),
            lockup_last_settled_at=int(lockup_last),
            funded_until_epoch=int(funded_until),
            available_funds=int(available),
            current_lockup_rate=int(current_lockup_rate),
        )

    def wallet_balance(self, token: Optional[str] = None) -> int:
        if token is None or token == TOKENS["FIL"]:
            return int(self._web3.eth.get_balance(self._account))
        if token == TOKENS["USDFC"]:
            return int(self._erc20.functions.balanceOf(self._account).call())
        raise ValueError(f"Unsupported token {token}")

    def allowance(self, spender: str, token: str = TOKENS["USDFC"]) -> int:
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for allowance")
        return int(self._erc20.functions.allowance(self._account, spender).call())

    def approve(self, spender: str, amount: int, token: str = TOKENS["USDFC"]) -> str:
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for approve")
        if not self._private_key:
            raise ValueError("private_key required for approve")
        txn = self._erc20.functions.approve(spender, amount).build_transaction(
            {
                "from": self._account,
                "nonce": self._web3.eth.get_transaction_count(self._account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    def deposit(self, amount: int, to: Optional[str] = None, token: str = TOKENS["USDFC"]) -> str:
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for deposit")
        if not self._private_key:
            raise ValueError("private_key required for deposit")
        to_addr = to or self._account
        txn = self._payments.functions.deposit(self._chain.contracts.usdfc, to_addr, amount).build_transaction(
            {
                "from": self._account,
                "nonce": self._web3.eth.get_transaction_count(self._account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    def withdraw(self, amount: int, token: str = TOKENS["USDFC"]) -> str:
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for withdraw")
        if not self._private_key:
            raise ValueError("private_key required for withdraw")
        txn = self._payments.functions.withdraw(self._chain.contracts.usdfc, amount).build_transaction(
            {
                "from": self._account,
                "nonce": self._web3.eth.get_transaction_count(self._account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()


class AsyncPaymentsService:
    def __init__(self, web3: AsyncWeb3, chain: Chain, account_address: str, private_key: Optional[str] = None) -> None:
        self._web3 = web3
        self._chain = chain
        self._account = account_address
        self._private_key = private_key
        self._payments = self._web3.eth.contract(address=chain.contracts.payments, abi=PAYMENTS_ABI)
        self._erc20 = self._web3.eth.contract(address=chain.contracts.usdfc, abi=ERC20_ABI)

    async def balance(self, token: str = TOKENS["USDFC"]) -> int:
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for payments contract balance")
        funds, _, _, _ = await self._payments.functions.accounts(self._chain.contracts.usdfc, self._account).call()
        return int(funds)

    async def account_info(self, token: str = TOKENS["USDFC"]) -> AccountInfo:
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for payments contract account info")
        funds, lockup_current, lockup_rate, lockup_last = await self._payments.functions.accounts(
            self._chain.contracts.usdfc, self._account
        ).call()
        funded_until, _, available, current_lockup_rate = await self._payments.functions.getAccountInfoIfSettled(
            self._chain.contracts.usdfc, self._account
        ).call()
        return AccountInfo(
            funds=int(funds),
            lockup_current=int(lockup_current),
            lockup_rate=int(lockup_rate),
            lockup_last_settled_at=int(lockup_last),
            funded_until_epoch=int(funded_until),
            available_funds=int(available),
            current_lockup_rate=int(current_lockup_rate),
        )

    async def wallet_balance(self, token: Optional[str] = None) -> int:
        if token is None or token == TOKENS["FIL"]:
            return int(await self._web3.eth.get_balance(self._account))
        if token == TOKENS["USDFC"]:
            return int(await self._erc20.functions.balanceOf(self._account).call())
        raise ValueError(f"Unsupported token {token}")

    async def allowance(self, spender: str, token: str = TOKENS["USDFC"]) -> int:
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for allowance")
        return int(await self._erc20.functions.allowance(self._account, spender).call())

    async def approve(self, spender: str, amount: int, token: str = TOKENS["USDFC"]) -> str:
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for approve")
        if not self._private_key:
            raise ValueError("private_key required for approve")
        txn = await self._erc20.functions.approve(spender, amount).build_transaction(
            {
                "from": self._account,
                "nonce": await self._web3.eth.get_transaction_count(self._account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    async def deposit(self, amount: int, to: Optional[str] = None, token: str = TOKENS["USDFC"]) -> str:
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for deposit")
        if not self._private_key:
            raise ValueError("private_key required for deposit")
        to_addr = to or self._account
        txn = await self._payments.functions.deposit(self._chain.contracts.usdfc, to_addr, amount).build_transaction(
            {
                "from": self._account,
                "nonce": await self._web3.eth.get_transaction_count(self._account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    async def withdraw(self, amount: int, token: str = TOKENS["USDFC"]) -> str:
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for withdraw")
        if not self._private_key:
            raise ValueError("private_key required for withdraw")
        txn = await self._payments.functions.withdraw(self._chain.contracts.usdfc, amount).build_transaction(
            {
                "from": self._account,
                "nonce": await self._web3.eth.get_transaction_count(self._account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()
