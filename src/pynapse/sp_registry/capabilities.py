from __future__ import annotations

from typing import Dict, List

from web3 import Web3


def capabilities_list_to_object(keys: List[str], values: List[bytes]) -> Dict[str, str]:
    capabilities: Dict[str, str] = {}
    for key, value in zip(keys, values):
        capabilities[key] = Web3.to_hex(value)
    return capabilities


def decode_address_capability(capability_value: bytes | str) -> str:
    if isinstance(capability_value, str):
        hex_value = capability_value
    else:
        hex_value = Web3.to_hex(capability_value)

    if len(hex_value) > 42:
        return "0x" + hex_value[-40:]
    if len(hex_value) < 42:
        return Web3.to_checksum_address(hex_value.rjust(42, "0"))
    return Web3.to_checksum_address(hex_value)
