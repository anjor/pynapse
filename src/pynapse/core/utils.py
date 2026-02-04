from __future__ import annotations

from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Union

NumberLike = Union[int, float, str, Decimal]


def parse_units(value: NumberLike, decimals: int = 18) -> int:
    if decimals < 0:
        raise ValueError("decimals must be >= 0")

    if isinstance(value, Decimal):
        dec = value
    else:
        dec = Decimal(str(value))

    scale = Decimal(10) ** decimals
    scaled = (dec * scale).quantize(Decimal(1), rounding=ROUND_DOWN)
    return int(scaled)


def format_units(value: int, decimals: int = 18) -> str:
    if decimals < 0:
        raise ValueError("decimals must be >= 0")

    getcontext().prec = max(28, decimals + 10)
    scale = Decimal(10) ** decimals
    dec = Decimal(value) / scale
    return format(dec.normalize(), "f")
