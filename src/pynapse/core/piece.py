from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Optional, Union

from .errors import create_error


DEFAULT_STREAM_COMMP_PATH = Path(
    "/Users/anjor/repos/filecoin-project/go-fil-commp-hashhash/cmd/stream-commp/stream-commp"
)


@dataclass(frozen=True)
class PieceCidInfo:
    piece_cid: str
    payload_size: int
    unpadded_piece_size: int
    padded_piece_size: int


def _resolve_commp_helper() -> Path:
    override = os.environ.get("PYNAPSE_COMMP_HELPER")
    if override:
        return Path(override)

    if DEFAULT_STREAM_COMMP_PATH.exists():
        return DEFAULT_STREAM_COMMP_PATH

    found = shutil.which("stream-commp")
    if found:
        return Path(found)

    raise create_error(
        "piece",
        "calculate",
        "stream-commp helper not found. Set PYNAPSE_COMMP_HELPER or install stream-commp.",
    )


def _parse_stream_commp_output(output: str) -> PieceCidInfo:
    commp_match = re.search(r"CommPCid:\s+(\S+)", output)
    payload_match = re.search(r"Payload:\s+(\d+)\s+bytes", output)
    unpadded_match = re.search(r"Unpadded piece:\s+(\d+)\s+bytes", output)
    padded_match = re.search(r"Padded piece:\s+(\d+)\s+bytes", output)

    if not (commp_match and payload_match and unpadded_match and padded_match):
        raise create_error("piece", "parse", "Failed to parse stream-commp output")

    return PieceCidInfo(
        piece_cid=commp_match.group(1),
        payload_size=int(payload_match.group(1)),
        unpadded_piece_size=int(unpadded_match.group(1)),
        padded_piece_size=int(padded_match.group(1)),
    )


def calculate_piece_cid(data: Union[bytes, BinaryIO, Path]) -> PieceCidInfo:
    helper = _resolve_commp_helper()

    if isinstance(data, Path):
        stream = data.open("rb")
        close_stream = True
    elif hasattr(data, "read"):
        stream = data  # type: ignore[assignment]
        close_stream = False
    else:
        stream = None
        close_stream = False

    try:
        if stream is not None:
            proc = subprocess.run(
                [str(helper)],
                stdin=stream,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
        else:
            proc = subprocess.run(
                [str(helper)],
                input=data,  # type: ignore[arg-type]
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
    finally:
        if close_stream:
            stream.close()

    if proc.returncode != 0:
        raise create_error(
            "piece",
            "calculate",
            f"stream-commp failed with code {proc.returncode}",
        )

    stderr_text = proc.stderr.decode("utf-8", errors="replace")
    return _parse_stream_commp_output(stderr_text)
