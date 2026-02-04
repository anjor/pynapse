from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence

from pynapse.core.piece import PieceCidInfo, calculate_piece_cid
from pynapse.core.typed_data import sign_add_pieces_extra_data
from pynapse.pdp import PDPServer
from pynapse.utils.metadata import combine_metadata


@dataclass
class UploadResult:
    piece_cid: str
    size: int
    tx_hash: Optional[str] = None


class StorageContext:
    def __init__(
        self,
        pdp_endpoint: str,
        chain,
        private_key: str,
        data_set_id: int,
        client_data_set_id: int,
    ) -> None:
        self._pdp = PDPServer(pdp_endpoint)
        self._chain = chain
        self._private_key = private_key
        self._data_set_id = data_set_id
        self._client_data_set_id = client_data_set_id

    @property
    def data_set_id(self) -> int:
        return self._data_set_id

    @property
    def client_data_set_id(self) -> int:
        return self._client_data_set_id

    def upload(self, data: bytes, metadata: Optional[Dict[str, str]] = None) -> UploadResult:
        info = calculate_piece_cid(data)
        self._pdp.upload_piece(data, info.piece_cid)
        pieces = [(info.piece_cid, [{"key": k, "value": v} for k, v in (metadata or {}).items()])]
        extra_data = sign_add_pieces_extra_data(
            private_key=self._private_key,
            chain=self._chain,
            client_data_set_id=self._client_data_set_id,
            pieces=pieces,
        )
        add_resp = self._pdp.add_pieces(self._data_set_id, [info.piece_cid], extra_data)
        return UploadResult(piece_cid=info.piece_cid, size=info.payload_size, tx_hash=add_resp.tx_hash)

    def download(self, piece_cid: str) -> bytes:
        return self._pdp.download_piece(piece_cid)
