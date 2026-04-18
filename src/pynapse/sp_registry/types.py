from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class ProviderInfo:
    provider_id: int
    service_provider: str
    payee: str
    name: str
    description: str
    is_active: bool


@dataclass
class ServiceProduct:
    product_type: int
    capability_keys: List[str]
    is_active: bool


@dataclass
class ProviderWithProduct:
    provider_id: int
    provider_info: ProviderInfo
    product: ServiceProduct
    product_capability_values: List[bytes]


@dataclass
class PDPOffering:
    service_url: str
    min_piece_size_in_bytes: int
    max_piece_size_in_bytes: int
    storage_price_per_tib_per_day: int
    min_proving_period_in_epochs: int
    location: str
    payment_token_address: str
    ipni_piece: bool = False
    ipni_ipfs: bool = False
    ipni_peer_id: Optional[str] = None
    # Non-standard capabilities returned by the SP. Preserved so downstream
    # consumers can read SP-specific signals (e.g. "serviceStatus"). Mirrors
    # FilOzone/synapse-sdk#687.
    extra_capabilities: Dict[str, str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.extra_capabilities is None:
            # Use field default cautiously — dataclass doesn't allow dict
            # factories when mixing with inherited fields downstream; set
            # here instead.
            object.__setattr__(self, "extra_capabilities", {})


@dataclass
class ProviderRegistrationInfo:
    payee: str
    name: str
    description: str
    pdp_offering: PDPOffering
    capabilities: Optional[Dict[str, str]] = None
