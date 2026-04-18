"""Tests for PDPVerifier.find_piece_ids_by_cid (#718)."""

from __future__ import annotations

from pynapse.core.typed_data import _piece_cid_bytes
from pynapse.pdp.verifier import AsyncPDPVerifier, SyncPDPVerifier


def test_find_piece_ids_by_cid_method_exists():
    assert hasattr(SyncPDPVerifier, "find_piece_ids_by_cid")
    assert hasattr(AsyncPDPVerifier, "find_piece_ids_by_cid")


def test_piece_cid_bytes_roundtrip_known_v1():
    # Sample CommP PieceCIDv1 from upstream fixtures. The important property
    # is that the helper can decode it into the bytes payload expected by
    # the on-chain Cids.Cid tuple without throwing.
    piece_cid_v1 = "baga6ea4seaqlwzed5tgjxpcnlxz2ilhpgitfhuodvgfhc6e6kroivbxmsjpesbi"
    data = _piece_cid_bytes(piece_cid_v1)
    # CIDv1 with fil-commitment-unsealed codec and 32-byte SHA256: always
    # starts with 0x01 version, 0x81 0xe2 0x03 codec varint.
    assert data[0] == 0x01
    assert data[1:4] == b"\x81\xe2\x03"
    # 32-byte digest at the tail
    assert len(data) >= 32
