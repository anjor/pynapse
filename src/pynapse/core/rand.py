from __future__ import annotations

import secrets
from typing import Optional


def rand_u256() -> int:
    return secrets.randbits(256)


def rand_index(length: int) -> int:
    if length <= 0:
        raise ValueError("length must be > 0")
    return secrets.randbelow(length)
