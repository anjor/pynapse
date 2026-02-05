from __future__ import annotations

from .abi_registry import load
from .generated import FILECOIN_PAY_V1_ABI

ERC20_ABI = load("erc20_abi.json")
# Use the complete FilecoinPayV1 ABI (payments_abi.json was a minimal stub)
PAYMENTS_ABI = FILECOIN_PAY_V1_ABI

from .generated import (
    ADDRESSES,
    ERRORS_ABI,
    FILECOIN_PAY_V1_ABI,
    FWSS_ABI,
    FWSS_VIEW_ABI,
    PDP_VERIFIER_ABI,
    PROVIDER_ID_SET_ABI,
    SERVICE_PROVIDER_REGISTRY_ABI,
    SESSION_KEY_REGISTRY_ABI,
)

__all__ = [
    "ERC20_ABI",
    "PAYMENTS_ABI",
    "ADDRESSES",
    "ERRORS_ABI",
    "FILECOIN_PAY_V1_ABI",
    "FWSS_ABI",
    "FWSS_VIEW_ABI",
    "PDP_VERIFIER_ABI",
    "PROVIDER_ID_SET_ABI",
    "SERVICE_PROVIDER_REGISTRY_ABI",
    "SESSION_KEY_REGISTRY_ABI",
]
