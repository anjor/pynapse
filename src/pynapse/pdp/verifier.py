from __future__ import annotations

from web3 import AsyncWeb3, Web3

from pynapse.contracts import PDP_VERIFIER_ABI
from pynapse.core.chains import Chain


class SyncPDPVerifier:
    def __init__(self, web3: Web3, chain: Chain) -> None:
        self._web3 = web3
        self._chain = chain
        self._contract = web3.eth.contract(address=chain.contracts.pdp, abi=PDP_VERIFIER_ABI)

    def data_set_live(self, data_set_id: int) -> bool:
        return bool(self._contract.functions.dataSetLive(data_set_id).call())

    def get_active_piece_count(self, data_set_id: int) -> int:
        return int(self._contract.functions.getActivePieceCount(data_set_id).call())

    def get_active_pieces(self, data_set_id: int, offset: int, limit: int):
        return self._contract.functions.getActivePieces(data_set_id, offset, limit).call()

    def get_data_set_leaf_count(self, data_set_id: int) -> int:
        return int(self._contract.functions.getDataSetLeafCount(data_set_id).call())

    def get_data_set_listener(self, data_set_id: int) -> str:
        return self._contract.functions.getDataSetListener(data_set_id).call()

    def get_data_set_storage_provider(self, data_set_id: int) -> str:
        return self._contract.functions.getDataSetStorageProvider(data_set_id).call()

    def get_next_piece_id(self, data_set_id: int) -> int:
        return int(self._contract.functions.getNextPieceId(data_set_id).call())

    def get_scheduled_removals(self, data_set_id: int):
        return self._contract.functions.getScheduledRemovals(data_set_id).call()


class AsyncPDPVerifier:
    def __init__(self, web3: AsyncWeb3, chain: Chain) -> None:
        self._web3 = web3
        self._chain = chain
        self._contract = web3.eth.contract(address=chain.contracts.pdp, abi=PDP_VERIFIER_ABI)

    async def data_set_live(self, data_set_id: int) -> bool:
        return bool(await self._contract.functions.dataSetLive(data_set_id).call())

    async def get_active_piece_count(self, data_set_id: int) -> int:
        return int(await self._contract.functions.getActivePieceCount(data_set_id).call())

    async def get_active_pieces(self, data_set_id: int, offset: int, limit: int):
        return await self._contract.functions.getActivePieces(data_set_id, offset, limit).call()

    async def get_data_set_leaf_count(self, data_set_id: int) -> int:
        return int(await self._contract.functions.getDataSetLeafCount(data_set_id).call())

    async def get_data_set_listener(self, data_set_id: int) -> str:
        return await self._contract.functions.getDataSetListener(data_set_id).call()

    async def get_data_set_storage_provider(self, data_set_id: int) -> str:
        return await self._contract.functions.getDataSetStorageProvider(data_set_id).call()

    async def get_next_piece_id(self, data_set_id: int) -> int:
        return int(await self._contract.functions.getNextPieceId(data_set_id).call())

    async def get_scheduled_removals(self, data_set_id: int):
        return await self._contract.functions.getScheduledRemovals(data_set_id).call()
