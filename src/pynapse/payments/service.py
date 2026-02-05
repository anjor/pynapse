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


@dataclass
class RailInfo:
    """Information about a payment rail."""
    rail_id: int
    token: str
    from_address: str
    to_address: str
    operator: str
    validator: str
    payment_rate: int
    lockup_period: int
    lockup_fixed: int
    settled_up_to: int
    end_epoch: int
    commission_rate_bps: int
    service_fee_recipient: str


@dataclass
class SettlementResult:
    """Result of a settlement operation."""
    total_settled_amount: int
    total_net_payee_amount: int
    total_operator_commission: int
    total_network_fee: int
    final_settled_epoch: int
    note: int


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

    def get_rail(self, rail_id: int) -> RailInfo:
        """
        Get detailed information about a specific rail.
        
        Args:
            rail_id: The rail ID to query
            
        Returns:
            Rail information including all parameters and current state
        """
        result = self._payments.functions.getRail(rail_id).call()
        return RailInfo(
            rail_id=rail_id,
            token=result[0],
            from_address=result[1],
            to_address=result[2],
            operator=result[3],
            validator=result[4],
            payment_rate=int(result[5]),
            lockup_period=int(result[6]),
            lockup_fixed=int(result[7]),
            settled_up_to=int(result[8]),
            end_epoch=int(result[9]),
            commission_rate_bps=int(result[10]),
            service_fee_recipient=result[11],
        )

    def settle(self, rail_id: int, until_epoch: Optional[int] = None, token: str = TOKENS["USDFC"]) -> str:
        """
        Settle a payment rail up to a specific epoch.
        
        Args:
            rail_id: The rail ID to settle
            until_epoch: The epoch to settle up to (defaults to current block number)
            token: The token to settle (defaults to USDFC)
            
        Returns:
            Transaction hash
        """
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for settle")
        if not self._private_key:
            raise ValueError("private_key required for settle")
        
        _until_epoch = until_epoch if until_epoch is not None else self._web3.eth.block_number
        
        txn = self._payments.functions.settleRail(
            self._chain.contracts.usdfc, rail_id, _until_epoch
        ).build_transaction(
            {
                "from": self._account,
                "nonce": self._web3.eth.get_transaction_count(self._account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    def settle_terminated_rail(self, rail_id: int, token: str = TOKENS["USDFC"]) -> str:
        """
        Emergency settlement for terminated rails only.
        
        Bypasses service contract validation. Can only be called by the client
        after the max settlement epoch has passed.
        
        Args:
            rail_id: The rail ID to settle
            token: The token to settle (defaults to USDFC)
            
        Returns:
            Transaction hash
        """
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for settle_terminated_rail")
        if not self._private_key:
            raise ValueError("private_key required for settle_terminated_rail")
        
        txn = self._payments.functions.settleTerminatedRailWithoutValidation(
            self._chain.contracts.usdfc, rail_id
        ).build_transaction(
            {
                "from": self._account,
                "nonce": self._web3.eth.get_transaction_count(self._account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    def settle_auto(self, rail_id: int, until_epoch: Optional[int] = None, token: str = TOKENS["USDFC"]) -> str:
        """
        Automatically settle a rail, detecting whether it's terminated or active.
        
        For terminated rails: calls settle_terminated_rail()
        For active rails: calls settle() with optional until_epoch
        
        Args:
            rail_id: The rail ID to settle
            until_epoch: The epoch to settle up to (ignored for terminated rails)
            token: The token to settle (defaults to USDFC)
            
        Returns:
            Transaction hash
        """
        rail = self.get_rail(rail_id)
        
        if rail.end_epoch > 0:
            # Rail is terminated
            return self.settle_terminated_rail(rail_id, token)
        else:
            # Rail is active
            return self.settle(rail_id, until_epoch, token)

    def get_rails_as_payer(self, token: str = TOKENS["USDFC"]) -> list:
        """
        Get all rails where the wallet is the payer.
        
        Args:
            token: The token to filter by (defaults to USDFC)
            
        Returns:
            List of RailInfo objects
        """
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for get_rails_as_payer")
        
        results, has_more = self._payments.functions.getRailsForPayerAndToken(
            self._chain.contracts.usdfc, self._account, 0, 100  # offset, limit
        ).call()
        
        rails = []
        for r in results:
            rails.append(RailInfo(
                rail_id=int(r[0]),
                token=r[1],
                from_address=r[2],
                to_address=r[3],
                operator=r[4],
                validator=r[5],
                payment_rate=int(r[6]),
                lockup_period=int(r[7]),
                lockup_fixed=int(r[8]),
                settled_up_to=int(r[9]),
                end_epoch=int(r[10]),
                commission_rate_bps=int(r[11]),
                service_fee_recipient=r[12],
            ))
        return rails

    def get_rails_as_payee(self, token: str = TOKENS["USDFC"]) -> list:
        """
        Get all rails where the wallet is the payee.
        
        Args:
            token: The token to filter by (defaults to USDFC)
            
        Returns:
            List of RailInfo objects
        """
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for get_rails_as_payee")
        
        results, has_more = self._payments.functions.getRailsForPayeeAndToken(
            self._chain.contracts.usdfc, self._account, 0, 100  # offset, limit
        ).call()
        
        rails = []
        for r in results:
            rails.append(RailInfo(
                rail_id=int(r[0]),
                token=r[1],
                from_address=r[2],
                to_address=r[3],
                operator=r[4],
                validator=r[5],
                payment_rate=int(r[6]),
                lockup_period=int(r[7]),
                lockup_fixed=int(r[8]),
                settled_up_to=int(r[9]),
                end_epoch=int(r[10]),
                commission_rate_bps=int(r[11]),
                service_fee_recipient=r[12],
            ))
        return rails


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

    async def get_rail(self, rail_id: int) -> RailInfo:
        """
        Get detailed information about a specific rail.
        
        Args:
            rail_id: The rail ID to query
            
        Returns:
            Rail information including all parameters and current state
        """
        result = await self._payments.functions.getRail(rail_id).call()
        return RailInfo(
            rail_id=rail_id,
            token=result[0],
            from_address=result[1],
            to_address=result[2],
            operator=result[3],
            validator=result[4],
            payment_rate=int(result[5]),
            lockup_period=int(result[6]),
            lockup_fixed=int(result[7]),
            settled_up_to=int(result[8]),
            end_epoch=int(result[9]),
            commission_rate_bps=int(result[10]),
            service_fee_recipient=result[11],
        )

    async def settle(self, rail_id: int, until_epoch: Optional[int] = None, token: str = TOKENS["USDFC"]) -> str:
        """
        Settle a payment rail up to a specific epoch.
        
        Args:
            rail_id: The rail ID to settle
            until_epoch: The epoch to settle up to (defaults to current block number)
            token: The token to settle (defaults to USDFC)
            
        Returns:
            Transaction hash
        """
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for settle")
        if not self._private_key:
            raise ValueError("private_key required for settle")
        
        _until_epoch = until_epoch if until_epoch is not None else await self._web3.eth.block_number
        
        txn = await self._payments.functions.settleRail(
            self._chain.contracts.usdfc, rail_id, _until_epoch
        ).build_transaction(
            {
                "from": self._account,
                "nonce": await self._web3.eth.get_transaction_count(self._account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    async def settle_terminated_rail(self, rail_id: int, token: str = TOKENS["USDFC"]) -> str:
        """
        Emergency settlement for terminated rails only.
        
        Bypasses service contract validation. Can only be called by the client
        after the max settlement epoch has passed.
        
        Args:
            rail_id: The rail ID to settle
            token: The token to settle (defaults to USDFC)
            
        Returns:
            Transaction hash
        """
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for settle_terminated_rail")
        if not self._private_key:
            raise ValueError("private_key required for settle_terminated_rail")
        
        txn = await self._payments.functions.settleTerminatedRailWithoutValidation(
            self._chain.contracts.usdfc, rail_id
        ).build_transaction(
            {
                "from": self._account,
                "nonce": await self._web3.eth.get_transaction_count(self._account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    async def settle_auto(self, rail_id: int, until_epoch: Optional[int] = None, token: str = TOKENS["USDFC"]) -> str:
        """
        Automatically settle a rail, detecting whether it's terminated or active.
        
        For terminated rails: calls settle_terminated_rail()
        For active rails: calls settle() with optional until_epoch
        
        Args:
            rail_id: The rail ID to settle
            until_epoch: The epoch to settle up to (ignored for terminated rails)
            token: The token to settle (defaults to USDFC)
            
        Returns:
            Transaction hash
        """
        rail = await self.get_rail(rail_id)
        
        if rail.end_epoch > 0:
            # Rail is terminated
            return await self.settle_terminated_rail(rail_id, token)
        else:
            # Rail is active
            return await self.settle(rail_id, until_epoch, token)

    async def get_rails_as_payer(self, token: str = TOKENS["USDFC"]) -> list:
        """
        Get all rails where the wallet is the payer.
        
        Args:
            token: The token to filter by (defaults to USDFC)
            
        Returns:
            List of RailInfo objects
        """
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for get_rails_as_payer")
        
        results, has_more = await self._payments.functions.getRailsForPayerAndToken(
            self._chain.contracts.usdfc, self._account, 0, 100  # offset, limit
        ).call()
        
        rails = []
        for r in results:
            rails.append(RailInfo(
                rail_id=int(r[0]),
                token=r[1],
                from_address=r[2],
                to_address=r[3],
                operator=r[4],
                validator=r[5],
                payment_rate=int(r[6]),
                lockup_period=int(r[7]),
                lockup_fixed=int(r[8]),
                settled_up_to=int(r[9]),
                end_epoch=int(r[10]),
                commission_rate_bps=int(r[11]),
                service_fee_recipient=r[12],
            ))
        return rails

    async def get_rails_as_payee(self, token: str = TOKENS["USDFC"]) -> list:
        """
        Get all rails where the wallet is the payee.
        
        Args:
            token: The token to filter by (defaults to USDFC)
            
        Returns:
            List of RailInfo objects
        """
        if token != TOKENS["USDFC"]:
            raise ValueError("Only USDFC is supported for get_rails_as_payee")
        
        results, has_more = await self._payments.functions.getRailsForPayeeAndToken(
            self._chain.contracts.usdfc, self._account, 0, 100  # offset, limit
        ).call()
        
        rails = []
        for r in results:
            rails.append(RailInfo(
                rail_id=int(r[0]),
                token=r[1],
                from_address=r[2],
                to_address=r[3],
                operator=r[4],
                validator=r[5],
                payment_rate=int(r[6]),
                lockup_period=int(r[7]),
                lockup_fixed=int(r[8]),
                settled_up_to=int(r[9]),
                end_epoch=int(r[10]),
                commission_rate_bps=int(r[11]),
                service_fee_recipient=r[12],
            ))
        return rails
