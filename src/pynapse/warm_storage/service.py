from __future__ import annotations

from dataclasses import dataclass
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

    def get_all_piece_metadata(self, data_set_id: int) -> List[Dict[str, str]]:
        entries = self._view.functions.getAllPieceMetadata(data_set_id).call()
        return [dict(entry) for entry in entries]

    def get_service_price(self, provider_id: int, token: str) -> int:
        return int(self._fwss.functions.getServicePrice(provider_id, token).call())

    def add_approved_provider(self, provider_id: int) -> str:
        if not self._private_key:
            raise ValueError("private_key required")
        txn = self._fwss.functions.addApprovedProvider(provider_id).build_transaction(
            {
                "from": self._web3.eth.default_account,
                "nonce": self._web3.eth.get_transaction_count(self._web3.eth.default_account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    def remove_approved_provider(self, provider_id: int) -> str:
        if not self._private_key:
            raise ValueError("private_key required")
        txn = self._fwss.functions.removeApprovedProvider(provider_id).build_transaction(
            {
                "from": self._web3.eth.default_account,
                "nonce": self._web3.eth.get_transaction_count(self._web3.eth.default_account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()


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

    async def get_all_piece_metadata(self, data_set_id: int) -> List[Dict[str, str]]:
        entries = await self._view.functions.getAllPieceMetadata(data_set_id).call()
        return [dict(entry) for entry in entries]

    async def get_service_price(self, provider_id: int, token: str) -> int:
        return int(await self._fwss.functions.getServicePrice(provider_id, token).call())

    async def add_approved_provider(self, provider_id: int) -> str:
        if not self._private_key:
            raise ValueError("private_key required")
        txn = await self._fwss.functions.addApprovedProvider(provider_id).build_transaction(
            {
                "from": self._web3.eth.default_account,
                "nonce": await self._web3.eth.get_transaction_count(self._web3.eth.default_account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    async def remove_approved_provider(self, provider_id: int) -> str:
        if not self._private_key:
            raise ValueError("private_key required")
        txn = await self._fwss.functions.removeApprovedProvider(provider_id).build_transaction(
            {
                "from": self._web3.eth.default_account,
                "nonce": await self._web3.eth.get_transaction_count(self._web3.eth.default_account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()
