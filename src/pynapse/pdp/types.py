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


# -----------------------------------------------------------------------------
# SP-to-SP pull types (mirrors upstream sp/pull-pieces.ts)
# -----------------------------------------------------------------------------

# Per-piece status as returned by ``POST /pdp/piece/pull``. Overall response
# status is the worst-case across all pieces:
#   failed > retrying > inProgress > pending > complete
PULL_STATUS_PENDING = "pending"
PULL_STATUS_IN_PROGRESS = "inProgress"
PULL_STATUS_RETRYING = "retrying"
PULL_STATUS_COMPLETE = "complete"
PULL_STATUS_FAILED = "failed"


@dataclass
class PullPieceInput:
    """Input piece for an SP-to-SP pull request."""
    piece_cid: str
    source_url: str  # HTTPS URL to pull the piece from


@dataclass
class PullPieceStatus:
    """Status of a single piece in a pull response."""
    piece_cid: str
    status: str


@dataclass
class PullPiecesResponse:
    """Response from ``POST /pdp/piece/pull``."""
    status: str
    pieces: List[PullPieceStatus]
