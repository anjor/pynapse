from __future__ import annotations

import json
from pathlib import Path
from typing import Any, List


def _load_json(name: str) -> List[Any]:
    path = Path(__file__).with_name(name)
    return json.loads(path.read_text())


ERC20_ABI = _load_json("erc20_abi.json")
PAYMENTS_ABI = _load_json("payments_abi.json")

__all__ = ["ERC20_ABI", "PAYMENTS_ABI"]
