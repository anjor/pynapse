import os
import shutil
from pathlib import Path

import pytest

from pynapse.core.piece import calculate_piece_cid


@pytest.mark.skipif(
    not (shutil.which("stream-commp") or os.environ.get("PYNAPSE_COMMP_HELPER")),
    reason="stream-commp helper not available",
)
def test_calculate_piece_cid_zero_block():
    data = b"\x00" * 128
    info = calculate_piece_cid(data)
    assert info.payload_size == len(data)
    assert info.piece_cid
    assert info.padded_piece_size >= info.unpadded_piece_size
