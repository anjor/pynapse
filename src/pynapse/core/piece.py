from __future__ import annotations

import math
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Optional, Union

from .errors import create_error

# Multicodec constants
RAW_CODEC = 0x55  # raw codec for PieceCIDv2
FIL_COMMITMENT_UNSEALED = 0xf101  # fil-commitment-unsealed for PieceCIDv1
SHA2_256_TRUNC254_PADDED = 0x1012  # sha2-256-trunc254-padded for PieceCIDv1
FR32_SHA2_256_TRUNC254_PADBINTREE = 0x1011  # fr32-sha2-256-trunc254-padded-binary-tree for PieceCIDv2

# Node size in bytes (32 bytes for SHA256)
NODE_SIZE = 32


def _encode_varint(value: int) -> bytes:
    """Encode an unsigned integer as a varint (unsigned LEB128)."""
    result = bytearray()
    while value >= 0x80:
        result.append((value & 0x7f) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def _decode_multibase_base32(cid_str: str) -> bytes:
    """Decode a base32lower multibase CID string to bytes."""
    import base64
    # Remove 'b' prefix (base32lower multibase prefix)
    if cid_str.startswith('baga') or cid_str.startswith('bafk'):
        # This is base32lower - need to decode
        # Add padding if necessary
        raw = cid_str[1:]  # Remove 'b' prefix
        # base32lower uses lowercase RFC 4648 alphabet
        # Python's base64.b32decode expects uppercase
        raw_upper = raw.upper()
        # Add padding
        padding = (8 - len(raw_upper) % 8) % 8
        raw_padded = raw_upper + '=' * padding
        return base64.b32decode(raw_padded)
    raise ValueError(f"Unsupported CID format: {cid_str}")


def _encode_multibase_base32(data: bytes) -> str:
    """Encode bytes to base32lower multibase CID string."""
    import base64
    encoded = base64.b32encode(data).decode('ascii').lower().rstrip('=')
    return 'b' + encoded


def _extract_root_hash_from_pieceCIDv1(piece_cid_v1: str) -> bytes:
    """Extract the 32-byte root hash from a PieceCIDv1 CID."""
    cid_bytes = _decode_multibase_base32(piece_cid_v1)
    
    # CIDv1 structure: version (varint) + codec (varint) + multihash
    # version = 1
    # codec = 0xf101 (fil-commitment-unsealed) = 2 bytes varint
    # multihash = code (varint) + length (varint) + digest
    
    idx = 0
    # Skip version (1 = single byte varint)
    idx += 1
    
    # Skip codec (0xf101 = 2 bytes as varint: 0x81 0xe2 0x03)
    while cid_bytes[idx] & 0x80:
        idx += 1
    idx += 1
    
    # Skip multihash code (0x1012 = 2 bytes as varint)
    while cid_bytes[idx] & 0x80:
        idx += 1
    idx += 1
    
    # Skip multihash length (32 = single byte)
    idx += 1
    
    # The rest is the 32-byte digest
    return cid_bytes[idx:idx + 32]


def _create_pieceCIDv2(root_hash: bytes, payload_size: int, padded_piece_size: int) -> str:
    """Create a PieceCIDv2 CID from root hash and size information.
    
    PieceCIDv2 uses:
    - Raw codec (0x55)
    - fr32-sha2-256-trunc254-padded-binary-tree multihash (0x1011)
    
    The multihash digest format (per FRC-0069):
    - varint: padding (padded_piece_size - payload_size after fr32 expansion)
    - 1 byte: tree height (log2 of number of leaves)
    - 32 bytes: root hash
    """
    # Calculate tree height
    # For fr32, the padded size determines the number of leaves
    # Each leaf is 32 bytes (NODE_SIZE)
    num_leaves = padded_piece_size // NODE_SIZE
    tree_height = int(math.log2(num_leaves)) if num_leaves > 0 else 0
    
    # Calculate padding
    # In fr32 encoding, padding = padded_piece_size - unpadded_piece_size
    # But for PieceCIDv2, we need the payload padding which accounts for fr32 expansion
    # unpadded_piece_size is payload_size * 128 / 127 (fr32 expansion), rounded up to power of 2
    # padding = unpadded_piece_size - payload_size
    
    # Actually, looking at the TypeScript SDK more carefully:
    # The padding in PieceCIDv2 multihash is the number of zero bytes added to pad to a power of 2
    # before fr32 encoding. This is: unpadded_piece_size - payload_size
    
    # From the sizes:
    # padded_piece_size = unpadded_piece_size * 128 / 127 (fr32 expansion)
    # So: unpadded_piece_size = padded_piece_size * 127 / 128
    unpadded_piece_size = (padded_piece_size * 127) // 128
    padding = unpadded_piece_size - payload_size
    
    # Build the multihash digest
    digest = bytearray()
    digest.extend(_encode_varint(padding))
    digest.append(tree_height)
    digest.extend(root_hash)
    
    # Build the full multihash (code + length + digest)
    multihash = bytearray()
    multihash.extend(_encode_varint(FR32_SHA2_256_TRUNC254_PADBINTREE))
    multihash.extend(_encode_varint(len(digest)))
    multihash.extend(digest)
    
    # Build CIDv1 (version + codec + multihash)
    cid = bytearray()
    cid.append(1)  # CID version 1
    cid.extend(_encode_varint(RAW_CODEC))
    cid.extend(multihash)
    
    return _encode_multibase_base32(bytes(cid))


def convert_to_pieceCIDv2(piece_cid_v1: str, payload_size: int, padded_piece_size: int) -> str:
    """Convert a PieceCIDv1 (CommP) to PieceCIDv2 format.
    
    PieceCIDv2 encodes the size information within the CID itself,
    as per FRC-0069 specification.
    
    Args:
        piece_cid_v1: The PieceCIDv1 string (e.g., "baga6ea4seaq...")
        payload_size: Original data size in bytes
        padded_piece_size: Padded piece size in bytes (power of 2)
        
    Returns:
        PieceCIDv2 string (e.g., "bafkzcib...")
    """
    root_hash = _extract_root_hash_from_pieceCIDv1(piece_cid_v1)
    return _create_pieceCIDv2(root_hash, payload_size, padded_piece_size)


@dataclass(frozen=True)
class PieceCidInfo:
    piece_cid: str  # PieceCIDv2 format
    piece_cid_v1: str  # Original PieceCIDv1 (CommP) from stream-commp
    payload_size: int
    unpadded_piece_size: int
    padded_piece_size: int


def _resolve_commp_helper() -> Path:
    override = os.environ.get("PYNAPSE_COMMP_HELPER")
    if override:
        return Path(override)

    found = shutil.which("stream-commp")
    if found:
        return Path(found)

    raise create_error(
        "piece",
        "calculate",
        (
            "stream-commp helper not found. Install stream-commp and ensure it is on PATH, "
            "or set PYNAPSE_COMMP_HELPER=/absolute/path/to/stream-commp."
        ),
    )


def _parse_stream_commp_output(output: str) -> PieceCidInfo:
    commp_match = re.search(r"CommPCid:\s+(\S+)", output)
    payload_match = re.search(r"Payload:\s+(\d+)\s+bytes", output)
    unpadded_match = re.search(r"Unpadded piece:\s+(\d+)\s+bytes", output)
    padded_match = re.search(r"Padded piece:\s+(\d+)\s+bytes", output)

    if not (commp_match and payload_match and unpadded_match and padded_match):
        raise create_error("piece", "parse", "Failed to parse stream-commp output")

    piece_cid_v1 = commp_match.group(1)
    payload_size = int(payload_match.group(1))
    unpadded_piece_size = int(unpadded_match.group(1))
    padded_piece_size = int(padded_match.group(1))
    
    # Convert to PieceCIDv2 format (required by PDP servers per FRC-0069)
    piece_cid_v2 = convert_to_pieceCIDv2(piece_cid_v1, payload_size, padded_piece_size)

    return PieceCidInfo(
        piece_cid=piece_cid_v2,
        piece_cid_v1=piece_cid_v1,
        payload_size=payload_size,
        unpadded_piece_size=unpadded_piece_size,
        padded_piece_size=padded_piece_size,
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
