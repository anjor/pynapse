"""Tests for the SP-to-SP pull-pieces client (part of #544)."""

from __future__ import annotations

from pynapse.pdp import (
    PULL_STATUS_COMPLETE,
    PULL_STATUS_FAILED,
    PULL_STATUS_IN_PROGRESS,
    PULL_STATUS_PENDING,
    PULL_STATUS_RETRYING,
    PDPServer,
    PullPieceInput,
    PullPiecesResponse,
)


def test_pull_status_constants_match_upstream_strings():
    assert PULL_STATUS_PENDING == "pending"
    assert PULL_STATUS_IN_PROGRESS == "inProgress"
    assert PULL_STATUS_RETRYING == "retrying"
    assert PULL_STATUS_COMPLETE == "complete"
    assert PULL_STATUS_FAILED == "failed"


def test_pdp_server_has_pull_methods():
    assert hasattr(PDPServer, "pull_pieces")
    assert hasattr(PDPServer, "wait_for_pull_pieces")


def test_pull_piece_input_round_trip():
    piece = PullPieceInput(
        piece_cid="bafkzcib" + "a" * 50,
        source_url="https://sp.example/pdp/piece/bafkzcib",
    )
    resp = PullPiecesResponse(
        status=PULL_STATUS_COMPLETE,
        pieces=[],
    )
    assert piece.piece_cid.startswith("bafkzcib")
    assert resp.status == "complete"
