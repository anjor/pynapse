from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


_BASE = Path(__file__).parent


def _load(name: str) -> Any:
    return json.loads((_BASE / name).read_text())


ERRORS_ABI = _load("errorsAbi.json")
FILECOIN_PAY_V1_ABI = _load("filecoinPayV1Abi.json")
FWSS_ABI = _load("filecoinWarmStorageServiceAbi.json")
FWSS_VIEW_ABI = _load("filecoinWarmStorageServiceStateViewAbi.json")
PDP_VERIFIER_ABI = _load("pdpVerifierAbi.json")
PROVIDER_ID_SET_ABI = _load("providerIdSetAbi.json")
SERVICE_PROVIDER_REGISTRY_ABI = _load("serviceProviderRegistryAbi.json")
SESSION_KEY_REGISTRY_ABI = _load("sessionKeyRegistryAbi.json")
ADDRESSES = _load("addresses.json")

__all__ = [
    "ERRORS_ABI",
    "FILECOIN_PAY_V1_ABI",
    "FWSS_ABI",
    "FWSS_VIEW_ABI",
    "PDP_VERIFIER_ABI",
    "PROVIDER_ID_SET_ABI",
    "SERVICE_PROVIDER_REGISTRY_ABI",
    "SESSION_KEY_REGISTRY_ABI",
    "ADDRESSES",
]
