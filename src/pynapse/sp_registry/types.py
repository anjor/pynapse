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


@dataclass
class ProviderRegistrationInfo:
    payee: str
    name: str
    description: str
    pdp_offering: PDPOffering
    capabilities: Optional[Dict[str, str]] = None
