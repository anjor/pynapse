from __future__ import annotations

from typing import Dict, List, Tuple

from multiformats import multibase
from web3 import Web3

from .capabilities import capabilities_list_to_object, decode_address_capability
from .types import PDPOffering, ProviderWithProduct

CAP_SERVICE_URL = "serviceURL"
CAP_MIN_PIECE_SIZE = "minPieceSizeInBytes"
CAP_MAX_PIECE_SIZE = "maxPieceSizeInBytes"
CAP_STORAGE_PRICE = "storagePricePerTibPerDay"
CAP_MIN_PROVING_PERIOD = "minProvingPeriodInEpochs"
CAP_LOCATION = "location"
CAP_PAYMENT_TOKEN = "paymentTokenAddress"
CAP_IPNI_PIECE = "ipniPiece"
CAP_IPNI_IPFS = "ipniIpfs"
CAP_IPNI_PEER_ID = "ipniPeerId"
CAP_IPNI_PEER_ID_LEGACY = "IPNIPeerID"


def decode_pdp_offering(provider: ProviderWithProduct) -> PDPOffering:
    capabilities = capabilities_list_to_object(provider.product.capability_keys, provider.product_capability_values)
    return decode_pdp_capabilities(capabilities)


def _hex_to_int(hex_value: str) -> int:
    return int(hex_value, 16)


def _hex_to_str(hex_value: str) -> str:
    return Web3.to_text(hexstr=hex_value)


def decode_pdp_capabilities(capabilities: Dict[str, str]) -> PDPOffering:
    required = {
        "service_url": _hex_to_str(capabilities[CAP_SERVICE_URL]),
        "min_piece_size_in_bytes": _hex_to_int(capabilities[CAP_MIN_PIECE_SIZE]),
        "max_piece_size_in_bytes": _hex_to_int(capabilities[CAP_MAX_PIECE_SIZE]),
        "storage_price_per_tib_per_day": _hex_to_int(capabilities[CAP_STORAGE_PRICE]),
        "min_proving_period_in_epochs": _hex_to_int(capabilities[CAP_MIN_PROVING_PERIOD]),
        "location": _hex_to_str(capabilities[CAP_LOCATION]),
        "payment_token_address": decode_address_capability(capabilities[CAP_PAYMENT_TOKEN]),
    }
    ipni_piece = capabilities.get(CAP_IPNI_PIECE) == "0x01"
    ipni_ipfs = capabilities.get(CAP_IPNI_IPFS) == "0x01"
    peer_hex = capabilities.get(CAP_IPNI_PEER_ID) or capabilities.get(CAP_IPNI_PEER_ID_LEGACY)
    ipni_peer_id = None
    if peer_hex:
        try:
            ipni_peer_id = multibase.encode("base58btc", Web3.to_bytes(hexstr=peer_hex)).decode()
        except Exception:
            ipni_peer_id = None
    return PDPOffering(
        ipni_piece=ipni_piece,
        ipni_ipfs=ipni_ipfs,
        ipni_peer_id=ipni_peer_id,
        **required,
    )


def encode_pdp_capabilities(pdp_offering: PDPOffering, capabilities: Dict[str, str] | None = None) -> Tuple[List[str], List[bytes]]:
    keys: List[str] = []
    values: List[bytes] = []

    def add(key: str, value: bytes):
        keys.append(key)
        values.append(value)

    add(CAP_SERVICE_URL, Web3.to_bytes(text=pdp_offering.service_url))
    add(CAP_MIN_PIECE_SIZE, pdp_offering.min_piece_size_in_bytes.to_bytes(32, "big"))
    add(CAP_MAX_PIECE_SIZE, pdp_offering.max_piece_size_in_bytes.to_bytes(32, "big"))
    if pdp_offering.ipni_piece:
        add(CAP_IPNI_PIECE, b"\x01")
    if pdp_offering.ipni_ipfs:
        add(CAP_IPNI_IPFS, b"\x01")
    add(CAP_STORAGE_PRICE, pdp_offering.storage_price_per_tib_per_day.to_bytes(32, "big"))
    add(CAP_MIN_PROVING_PERIOD, pdp_offering.min_proving_period_in_epochs.to_bytes(32, "big"))
    add(CAP_LOCATION, Web3.to_bytes(text=pdp_offering.location))
    add(CAP_PAYMENT_TOKEN, Web3.to_bytes(hexstr=pdp_offering.payment_token_address))

    if pdp_offering.ipni_peer_id:
        try:
            peer_bytes = multibase.decode(pdp_offering.ipni_peer_id)
            add(CAP_IPNI_PEER_ID, peer_bytes)
        except Exception:
            pass

    if capabilities:
        for key, value in capabilities.items():
            keys.append(key)
            if value is None or value == "":
                values.append(b"\x01")
            else:
                if value.startswith("0x"):
                    values.append(Web3.to_bytes(hexstr=value))
                else:
                    values.append(Web3.to_bytes(text=value))

    return keys, values
