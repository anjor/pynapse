from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from eth_account import Account
from web3 import AsyncWeb3, Web3

from pynapse.contracts import FWSS_ABI, FWSS_VIEW_ABI
from pynapse.core.chains import Chain


@dataclass
class DataSetInfo:
    pdp_rail_id: int
    cache_miss_rail_id: int
    cdn_rail_id: int
    payer: str
    payee: str
    service_provider: str
    commission_bps: int
    client_data_set_id: int
    pdp_end_epoch: int
    provider_id: int
    data_set_id: int


@dataclass
class EnhancedDataSetInfo(DataSetInfo):
    """Extended dataset info with live status, management info, and metadata."""
    active_piece_count: int = 0
    is_live: bool = False
    is_managed: bool = False
    with_cdn: bool = False
    metadata: Dict[str, str] = field(default_factory=dict)


class SyncWarmStorageService:
    def __init__(self, web3: Web3, chain: Chain, private_key: Optional[str] = None) -> None:
        self._web3 = web3
        self._chain = chain
        self._private_key = private_key
        self._fwss = web3.eth.contract(address=chain.contracts.warm_storage, abi=FWSS_ABI)
        self._view = web3.eth.contract(address=chain.contracts.warm_storage_state_view, abi=FWSS_VIEW_ABI)

    def get_data_set(self, data_set_id: int) -> DataSetInfo:
        info = self._view.functions.getDataSet(data_set_id).call()[0]
        if int(info[0]) == 0:
            raise ValueError(f"Data set {data_set_id} does not exist")
        return DataSetInfo(
            pdp_rail_id=int(info[0]),
            cache_miss_rail_id=int(info[1]),
            cdn_rail_id=int(info[2]),
            payer=info[3],
            payee=info[4],
            service_provider=info[5],
            commission_bps=int(info[6]),
            client_data_set_id=int(info[7]),
            pdp_end_epoch=int(info[8]),
            provider_id=int(info[9]),
            data_set_id=int(info[10]),
        )

    def get_client_data_sets(self, client_address: str) -> List[DataSetInfo]:
        data_sets = self._view.functions.getClientDataSets(client_address).call()
        return [self.get_data_set(int(ds[10])) for ds in data_sets]

    def get_all_data_set_metadata(self, data_set_id: int) -> Dict[str, str]:
        entries = self._view.functions.getAllDataSetMetadata(data_set_id).call()
        return {key: value for key, value in entries}

    def get_data_set_metadata(self, data_set_id: int, key: str) -> Optional[str]:
        exists, value = self._view.functions.getDataSetMetadata(data_set_id, key).call()
        return value if exists else None

    def get_all_piece_metadata(self, data_set_id: int) -> List[Dict[str, str]]:
        entries = self._view.functions.getAllPieceMetadata(data_set_id).call()
        return [dict(entry) for entry in entries]

    def get_piece_metadata(self, data_set_id: int, piece_id: int, key: str) -> Optional[str]:
        exists, value = self._view.functions.getPieceMetadata(data_set_id, piece_id, key).call()
        return value if exists else None

    def get_data_set_status(self, data_set_id: int) -> int:
        return int(self._view.functions.getDataSetStatus(data_set_id).call())

    def get_data_set_size_in_bytes(self, leaf_count: int) -> int:
        return int(self._view.functions.getDataSetSizeInBytes(leaf_count).call())

    def get_pdp_config(self):
        return self._view.functions.getPDPConfig().call()

    def get_service_price(self, provider_id: int, token: str) -> int:
        return int(self._fwss.functions.getServicePrice(provider_id, token).call())

    def get_effective_rates(self):
        return self._fwss.functions.getEffectiveRates().call()

    def calculate_rate_per_epoch(self, total_bytes: int) -> int:
        return int(self._fwss.functions.calculateRatePerEpoch(total_bytes).call())

    def get_proving_period_for_epoch(self, data_set_id: int, epoch: int) -> int:
        return int(self._fwss.functions.getProvingPeriodForEpoch(data_set_id, epoch).call())

    def get_current_pricing_rates(self):
        return self._view.functions.getCurrentPricingRates().call()

    def next_pdp_challenge_window_start(self, data_set_id: int) -> int:
        return int(self._view.functions.nextPDPChallengeWindowStart(data_set_id).call())

    def proving_deadline(self, data_set_id: int) -> int:
        return int(self._view.functions.provingDeadline(data_set_id).call())

    def get_approved_providers(self, data_set_id: int) -> List[int]:
        providers = self._view.functions.getApprovedProviders(data_set_id).call()
        return [int(pid) for pid in providers]

    def is_provider_approved(self, data_set_id: int, provider_id: int) -> bool:
        return bool(self._view.functions.isProviderApproved(data_set_id, provider_id).call())

    def add_approved_provider(self, account: str, provider_id: int) -> str:
        if not self._private_key:
            raise ValueError("private_key required")
        txn = self._fwss.functions.addApprovedProvider(provider_id).build_transaction(
            {
                "from": account,
                "nonce": self._web3.eth.get_transaction_count(account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    def remove_approved_provider(self, account: str, provider_id: int) -> str:
        if not self._private_key:
            raise ValueError("private_key required")
        txn = self._fwss.functions.removeApprovedProvider(provider_id).build_transaction(
            {
                "from": account,
                "nonce": self._web3.eth.get_transaction_count(account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    def get_approved_provider_ids(self) -> List[int]:
        """Get list of all approved provider IDs for the warm storage service."""
        provider_ids = self._fwss.functions.getApprovedProviderIds().call()
        return [int(pid) for pid in provider_ids]

    def get_active_piece_count(self, data_set_id: int) -> int:
        """Get count of active pieces in a dataset (excludes removed pieces)."""
        from pynapse.pdp import SyncPDPVerifier
        verifier = SyncPDPVerifier(self._web3, self._chain)
        return verifier.get_active_piece_count(data_set_id)

    def data_set_live(self, data_set_id: int) -> bool:
        """Check if a dataset is live."""
        from pynapse.pdp import SyncPDPVerifier
        verifier = SyncPDPVerifier(self._web3, self._chain)
        return verifier.data_set_live(data_set_id)

    def get_data_set_listener(self, data_set_id: int) -> str:
        """Get the listener address for a dataset."""
        from pynapse.pdp import SyncPDPVerifier
        verifier = SyncPDPVerifier(self._web3, self._chain)
        return verifier.get_data_set_listener(data_set_id)

    def validate_data_set(self, data_set_id: int) -> None:
        """
        Validate that a dataset is live and managed by this WarmStorage contract.
        
        Raises:
            ValueError: If dataset is not live or not managed by this contract.
        """
        if not self.data_set_live(data_set_id):
            raise ValueError(f"Data set {data_set_id} does not exist or is not live")
        
        listener = self.get_data_set_listener(data_set_id)
        if listener.lower() != self._chain.contracts.warm_storage.lower():
            raise ValueError(
                f"Data set {data_set_id} is not managed by this WarmStorage contract "
                f"({self._chain.contracts.warm_storage}), managed by {listener}"
            )

    def terminate_data_set(self, account: str, data_set_id: int) -> str:
        """
        Terminate a dataset. This also removes all pieces in the dataset.
        
        Args:
            account: The account address to send from
            data_set_id: The ID of the dataset to terminate
            
        Returns:
            Transaction hash
        """
        if not self._private_key:
            raise ValueError("private_key required")
        txn = self._fwss.functions.terminateDataSet(data_set_id).build_transaction(
            {
                "from": account,
                "nonce": self._web3.eth.get_transaction_count(account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    def get_client_data_sets_with_details(self, client_address: str) -> List[EnhancedDataSetInfo]:
        """
        Get all datasets for a client with enhanced details.
        
        Includes live status, management info, metadata, and piece counts.
        
        Args:
            client_address: The client address to query
            
        Returns:
            List of enhanced dataset info
        """
        from pynapse.pdp import SyncPDPVerifier
        verifier = SyncPDPVerifier(self._web3, self._chain)
        
        data_sets = self.get_client_data_sets(client_address)
        enhanced: List[EnhancedDataSetInfo] = []
        
        for ds in data_sets:
            try:
                is_live = verifier.data_set_live(ds.data_set_id)
                listener = verifier.get_data_set_listener(ds.data_set_id) if is_live else ""
                is_managed = listener.lower() == self._chain.contracts.warm_storage.lower() if listener else False
                metadata = self.get_all_data_set_metadata(ds.data_set_id) if is_live else {}
                active_piece_count = verifier.get_active_piece_count(ds.data_set_id) if is_live else 0
                with_cdn = ds.cdn_rail_id > 0 and "withCDN" in metadata
                
                enhanced.append(EnhancedDataSetInfo(
                    pdp_rail_id=ds.pdp_rail_id,
                    cache_miss_rail_id=ds.cache_miss_rail_id,
                    cdn_rail_id=ds.cdn_rail_id,
                    payer=ds.payer,
                    payee=ds.payee,
                    service_provider=ds.service_provider,
                    commission_bps=ds.commission_bps,
                    client_data_set_id=ds.client_data_set_id,
                    pdp_end_epoch=ds.pdp_end_epoch,
                    provider_id=ds.provider_id,
                    data_set_id=ds.data_set_id,
                    active_piece_count=active_piece_count,
                    is_live=is_live,
                    is_managed=is_managed,
                    with_cdn=with_cdn,
                    metadata=metadata,
                ))
            except Exception as e:
                # Skip datasets that fail to load details
                continue
        
        return enhanced


class AsyncWarmStorageService:
    def __init__(self, web3: AsyncWeb3, chain: Chain, private_key: Optional[str] = None) -> None:
        self._web3 = web3
        self._chain = chain
        self._private_key = private_key
        self._fwss = web3.eth.contract(address=chain.contracts.warm_storage, abi=FWSS_ABI)
        self._view = web3.eth.contract(address=chain.contracts.warm_storage_state_view, abi=FWSS_VIEW_ABI)

    async def get_data_set(self, data_set_id: int) -> DataSetInfo:
        info = (await self._view.functions.getDataSet(data_set_id).call())[0]
        if int(info[0]) == 0:
            raise ValueError(f"Data set {data_set_id} does not exist")
        return DataSetInfo(
            pdp_rail_id=int(info[0]),
            cache_miss_rail_id=int(info[1]),
            cdn_rail_id=int(info[2]),
            payer=info[3],
            payee=info[4],
            service_provider=info[5],
            commission_bps=int(info[6]),
            client_data_set_id=int(info[7]),
            pdp_end_epoch=int(info[8]),
            provider_id=int(info[9]),
            data_set_id=int(info[10]),
        )

    async def get_client_data_sets(self, client_address: str) -> List[DataSetInfo]:
        data_sets = await self._view.functions.getClientDataSets(client_address).call()
        return [await self.get_data_set(int(ds[10])) for ds in data_sets]

    async def get_all_data_set_metadata(self, data_set_id: int) -> Dict[str, str]:
        entries = await self._view.functions.getAllDataSetMetadata(data_set_id).call()
        return {key: value for key, value in entries}

    async def get_data_set_metadata(self, data_set_id: int, key: str) -> Optional[str]:
        exists, value = await self._view.functions.getDataSetMetadata(data_set_id, key).call()
        return value if exists else None

    async def get_all_piece_metadata(self, data_set_id: int) -> List[Dict[str, str]]:
        entries = await self._view.functions.getAllPieceMetadata(data_set_id).call()
        return [dict(entry) for entry in entries]

    async def get_piece_metadata(self, data_set_id: int, piece_id: int, key: str) -> Optional[str]:
        exists, value = await self._view.functions.getPieceMetadata(data_set_id, piece_id, key).call()
        return value if exists else None

    async def get_data_set_status(self, data_set_id: int) -> int:
        return int(await self._view.functions.getDataSetStatus(data_set_id).call())

    async def get_data_set_size_in_bytes(self, leaf_count: int) -> int:
        return int(await self._view.functions.getDataSetSizeInBytes(leaf_count).call())

    async def get_pdp_config(self):
        return await self._view.functions.getPDPConfig().call()

    async def get_service_price(self, provider_id: int, token: str) -> int:
        return int(await self._fwss.functions.getServicePrice(provider_id, token).call())

    async def get_effective_rates(self):
        return await self._fwss.functions.getEffectiveRates().call()

    async def calculate_rate_per_epoch(self, total_bytes: int) -> int:
        return int(await self._fwss.functions.calculateRatePerEpoch(total_bytes).call())

    async def get_proving_period_for_epoch(self, data_set_id: int, epoch: int) -> int:
        return int(await self._fwss.functions.getProvingPeriodForEpoch(data_set_id, epoch).call())

    async def get_current_pricing_rates(self):
        return await self._view.functions.getCurrentPricingRates().call()

    async def next_pdp_challenge_window_start(self, data_set_id: int) -> int:
        return int(await self._view.functions.nextPDPChallengeWindowStart(data_set_id).call())

    async def proving_deadline(self, data_set_id: int) -> int:
        return int(await self._view.functions.provingDeadline(data_set_id).call())

    async def get_approved_providers(self, data_set_id: int) -> List[int]:
        providers = await self._view.functions.getApprovedProviders(data_set_id).call()
        return [int(pid) for pid in providers]

    async def is_provider_approved(self, data_set_id: int, provider_id: int) -> bool:
        return bool(await self._view.functions.isProviderApproved(data_set_id, provider_id).call())

    async def add_approved_provider(self, account: str, provider_id: int) -> str:
        if not self._private_key:
            raise ValueError("private_key required")
        txn = await self._fwss.functions.addApprovedProvider(provider_id).build_transaction(
            {
                "from": account,
                "nonce": await self._web3.eth.get_transaction_count(account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    async def remove_approved_provider(self, account: str, provider_id: int) -> str:
        if not self._private_key:
            raise ValueError("private_key required")
        txn = await self._fwss.functions.removeApprovedProvider(provider_id).build_transaction(
            {
                "from": account,
                "nonce": await self._web3.eth.get_transaction_count(account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    async def get_approved_provider_ids(self) -> List[int]:
        """Get list of all approved provider IDs for the warm storage service."""
        provider_ids = await self._fwss.functions.getApprovedProviderIds().call()
        return [int(pid) for pid in provider_ids]

    async def get_active_piece_count(self, data_set_id: int) -> int:
        """Get count of active pieces in a dataset (excludes removed pieces)."""
        from pynapse.pdp import AsyncPDPVerifier
        verifier = AsyncPDPVerifier(self._web3, self._chain)
        return await verifier.get_active_piece_count(data_set_id)

    async def data_set_live(self, data_set_id: int) -> bool:
        """Check if a dataset is live."""
        from pynapse.pdp import AsyncPDPVerifier
        verifier = AsyncPDPVerifier(self._web3, self._chain)
        return await verifier.data_set_live(data_set_id)

    async def get_data_set_listener(self, data_set_id: int) -> str:
        """Get the listener address for a dataset."""
        from pynapse.pdp import AsyncPDPVerifier
        verifier = AsyncPDPVerifier(self._web3, self._chain)
        return await verifier.get_data_set_listener(data_set_id)

    async def validate_data_set(self, data_set_id: int) -> None:
        """
        Validate that a dataset is live and managed by this WarmStorage contract.
        
        Raises:
            ValueError: If dataset is not live or not managed by this contract.
        """
        if not await self.data_set_live(data_set_id):
            raise ValueError(f"Data set {data_set_id} does not exist or is not live")
        
        listener = await self.get_data_set_listener(data_set_id)
        if listener.lower() != self._chain.contracts.warm_storage.lower():
            raise ValueError(
                f"Data set {data_set_id} is not managed by this WarmStorage contract "
                f"({self._chain.contracts.warm_storage}), managed by {listener}"
            )

    async def terminate_data_set(self, account: str, data_set_id: int) -> str:
        """
        Terminate a dataset. This also removes all pieces in the dataset.
        
        Args:
            account: The account address to send from
            data_set_id: The ID of the dataset to terminate
            
        Returns:
            Transaction hash
        """
        if not self._private_key:
            raise ValueError("private_key required")
        txn = await self._fwss.functions.terminateDataSet(data_set_id).build_transaction(
            {
                "from": account,
                "nonce": await self._web3.eth.get_transaction_count(account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    async def get_client_data_sets_with_details(self, client_address: str) -> List[EnhancedDataSetInfo]:
        """
        Get all datasets for a client with enhanced details.
        
        Includes live status, management info, metadata, and piece counts.
        
        Args:
            client_address: The client address to query
            
        Returns:
            List of enhanced dataset info
        """
        from pynapse.pdp import AsyncPDPVerifier
        verifier = AsyncPDPVerifier(self._web3, self._chain)
        
        data_sets = await self.get_client_data_sets(client_address)
        enhanced: List[EnhancedDataSetInfo] = []
        
        for ds in data_sets:
            try:
                is_live = await verifier.data_set_live(ds.data_set_id)
                listener = await verifier.get_data_set_listener(ds.data_set_id) if is_live else ""
                is_managed = listener.lower() == self._chain.contracts.warm_storage.lower() if listener else False
                metadata = await self.get_all_data_set_metadata(ds.data_set_id) if is_live else {}
                active_piece_count = await verifier.get_active_piece_count(ds.data_set_id) if is_live else 0
                with_cdn = ds.cdn_rail_id > 0 and "withCDN" in metadata
                
                enhanced.append(EnhancedDataSetInfo(
                    pdp_rail_id=ds.pdp_rail_id,
                    cache_miss_rail_id=ds.cache_miss_rail_id,
                    cdn_rail_id=ds.cdn_rail_id,
                    payer=ds.payer,
                    payee=ds.payee,
                    service_provider=ds.service_provider,
                    commission_bps=ds.commission_bps,
                    client_data_set_id=ds.client_data_set_id,
                    pdp_end_epoch=ds.pdp_end_epoch,
                    provider_id=ds.provider_id,
                    data_set_id=ds.data_set_id,
                    active_piece_count=active_piece_count,
                    is_live=is_live,
                    is_managed=is_managed,
                    with_cdn=with_cdn,
                    metadata=metadata,
                ))
            except Exception as e:
                # Skip datasets that fail to load details
                continue
        
        return enhanced
