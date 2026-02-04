from __future__ import annotations

from typing import Dict, Optional

from .context import StorageContext, UploadResult


class StorageManager:
    def __init__(self, chain, private_key: str) -> None:
        self._chain = chain
        self._private_key = private_key

    def create_context(self, pdp_endpoint: str, data_set_id: int, client_data_set_id: int) -> StorageContext:
        return StorageContext(
            pdp_endpoint=pdp_endpoint,
            chain=self._chain,
            private_key=self._private_key,
            data_set_id=data_set_id,
            client_data_set_id=client_data_set_id,
        )

    def upload(self, data: bytes, pdp_endpoint: str, data_set_id: int, client_data_set_id: int) -> UploadResult:
        context = self.create_context(pdp_endpoint, data_set_id, client_data_set_id)
        return context.upload(data)

    def download(self, piece_cid: str, pdp_endpoint: str) -> bytes:
        context = StorageContext(
            pdp_endpoint=pdp_endpoint,
            chain=self._chain,
            private_key=self._private_key,
            data_set_id=0,
            client_data_set_id=0,
        )
        return context.download(piece_cid)
