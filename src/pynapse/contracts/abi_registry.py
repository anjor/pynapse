from __future__ import annotations

import json
from pathlib import Path
from typing import Any, List

_BASE = Path(__file__).parent


def load(name: str) -> List[Any]:
    return json.loads((_BASE / name).read_text())
