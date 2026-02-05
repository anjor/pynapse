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


@dataclass
class ServiceApproval:
    """Operator approval status and allowances."""
    is_approved: bool
    rate_allowance: int
    lockup_allowance: int
    max_lockup_period: int
    rate_usage: int = 0
    lockup_usage: int = 0


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

    def service_approval(self, service: str, token: str = TOKENS["USDFC"]) -> ServiceApproval:
        """
        Get the operator approval status and allowances for a service.
        
        Args:
            service: The service contract address to check
            token: The token to check approval for (defaults to USDFC)
            
        Returns:
            ServiceApproval with approval status and allowances
        """
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for service_approval")
        
        result = self._payments.functions.operatorApprovals(
            self._chain.contracts.usdfc, self._account, service
        ).call()
        
        # Result format: (isApproved, rateAllowance, lockupAllowance, maxLockupPeriod)
        return ServiceApproval(
            is_approved=bool(result[0]),
            rate_allowance=int(result[1]),
            lockup_allowance=int(result[2]),
            max_lockup_period=int(result[3]),
        )

    def approve_service(
        self,
        service: str,
        rate_allowance: int,
        lockup_allowance: int,
        max_lockup_period: int,
        token: str = TOKENS["USDFC"],
    ) -> str:
        """
        Approve a service contract to act as an operator for payment rails.
        
        Args:
            service: The service contract address to approve
            rate_allowance: Maximum payment rate per epoch the operator can set
            lockup_allowance: Maximum lockup amount the operator can set
            max_lockup_period: Maximum lockup period in epochs the operator can set
            token: The token to approve for (defaults to USDFC)
            
        Returns:
            Transaction hash
        """
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for approve_service")
        if not self._private_key:
            raise ValueError("private_key required for approve_service")
        
        txn = self._payments.functions.setOperatorApproval(
            self._chain.contracts.usdfc,
            service,
            True,  # approve
            rate_allowance,
            lockup_allowance,
            max_lockup_period,
        ).build_transaction(
            {
                "from": self._account,
                "nonce": self._web3.eth.get_transaction_count(self._account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    def revoke_service(self, service: str, token: str = TOKENS["USDFC"]) -> str:
        """
        Revoke a service contract's operator approval.
        
        Args:
            service: The service contract address to revoke
            token: The token to revoke approval for (defaults to USDFC)
            
        Returns:
            Transaction hash
        """
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for revoke_service")
        if not self._private_key:
            raise ValueError("private_key required for revoke_service")
        
        txn = self._payments.functions.setOperatorApproval(
            self._chain.contracts.usdfc,
            service,
            False,  # revoke
            0,  # rate_allowance (ignored for revoke)
            0,  # lockup_allowance (ignored for revoke)
            0,  # max_lockup_period (ignored for revoke)
        ).build_transaction(
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

    async def service_approval(self, service: str, token: str = TOKENS["USDFC"]) -> ServiceApproval:
        """
        Get the operator approval status and allowances for a service.
        
        Args:
            service: The service contract address to check
            token: The token to check approval for (defaults to USDFC)
            
        Returns:
            ServiceApproval with approval status and allowances
        """
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for service_approval")
        
        result = await self._payments.functions.operatorApprovals(
            self._chain.contracts.usdfc, self._account, service
        ).call()
        
        return ServiceApproval(
            is_approved=bool(result[0]),
            rate_allowance=int(result[1]),
            lockup_allowance=int(result[2]),
            max_lockup_period=int(result[3]),
        )

    async def approve_service(
        self,
        service: str,
        rate_allowance: int,
        lockup_allowance: int,
        max_lockup_period: int,
        token: str = TOKENS["USDFC"],
    ) -> str:
        """
        Approve a service contract to act as an operator for payment rails.
        
        Args:
            service: The service contract address to approve
            rate_allowance: Maximum payment rate per epoch the operator can set
            lockup_allowance: Maximum lockup amount the operator can set
            max_lockup_period: Maximum lockup period in epochs the operator can set
            token: The token to approve for (defaults to USDFC)
            
        Returns:
            Transaction hash
        """
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for approve_service")
        if not self._private_key:
            raise ValueError("private_key required for approve_service")
        
        txn = await self._payments.functions.setOperatorApproval(
            self._chain.contracts.usdfc,
            service,
            True,
            rate_allowance,
            lockup_allowance,
            max_lockup_period,
        ).build_transaction(
            {
                "from": self._account,
                "nonce": await self._web3.eth.get_transaction_count(self._account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    async def revoke_service(self, service: str, token: str = TOKENS["USDFC"]) -> str:
        """
        Revoke a service contract's operator approval.
        
        Args:
            service: The service contract address to revoke
            token: The token to revoke approval for (defaults to USDFC)
            
        Returns:
            Transaction hash
        """
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for revoke_service")
        if not self._private_key:
            raise ValueError("private_key required for revoke_service")
        
        txn = await self._payments.functions.setOperatorApproval(
            self._chain.contracts.usdfc,
            service,
            False,
            0,
            0,
            0,
        ).build_transaction(
            {
                "from": self._account,
                "nonce": await self._web3.eth.get_transaction_count(self._account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()
