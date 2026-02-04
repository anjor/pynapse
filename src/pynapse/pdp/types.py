from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class CreateDataSetResponse:
    tx_hash: str
    status_url: str


@dataclass
class AddPiecesResponse:
    message: str
    tx_hash: str
    status_url: str


@dataclass
class UploadPieceResponse:
    piece_cid: str
    size: int


@dataclass
class DataSetCreationStatus:
    data_set_created: bool
    data_set_id: Optional[int]
    message: Optional[str] = None


@dataclass
class PieceAdditionStatus:
    add_message_ok: Optional[bool]
    piece_count: Optional[int]
    confirmed_piece_ids: Optional[List[int]]
    message: Optional[str] = None
