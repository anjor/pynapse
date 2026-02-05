from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from pynapse.contracts.generated import ADDRESSES


@dataclass(frozen=True)
class ChainContracts:
    multicall3: str
    usdfc: str
    payments: str
    warm_storage: str
    warm_storage_state_view: str
    sp_registry: str
    session_key_registry: str
    pdp_verifier: str


@dataclass(frozen=True)
class Chain:
    id: int
    name: str
    rpc_url: str
    genesis_timestamp: int
    contracts: ChainContracts
    filbeam_domain: Optional[str] = None


NETWORK_MAINNET = "mainnet"
NETWORK_CALIBRATION = "calibration"

CHAIN_ID_MAINNET = 314
CHAIN_ID_CALIBRATION = 314159

RPC_URLS: Dict[str, str] = {
    NETWORK_MAINNET: "https://api.node.glif.io/rpc/v1",
    NETWORK_CALIBRATION: "https://api.calibration.node.glif.io/rpc/v1",
}

GENESIS_TIMESTAMPS: Dict[str, int] = {
    NETWORK_MAINNET: 1598306400,
    NETWORK_CALIBRATION: 1667326380,
}

CONTRACTS_BY_NETWORK: Dict[str, ChainContracts] = {
    NETWORK_MAINNET: ChainContracts(
        multicall3="0xcA11bde05977b3631167028862bE2a173976CA11",
        usdfc="0x80B98d3aa09ffff255c3ba4A241111Ff1262F045",
        payments="0xC8C3c94AA8c60E0DfF060d4C3aCbA0BC16e4e0eC",
        warm_storage="0x8408502033C418E1bbC97cE9ac48E5528F371A9f",
        warm_storage_state_view="0x9e4e6699d8F67dFc883d6b0A7344Bd56F7E80B46",
        sp_registry="0xf55dDbf63F1b55c3F1D4FA7e339a68AB7b64A5eB",
        session_key_registry=ADDRESSES["sessionKeyRegistryAddress"]["314"],
        pdp_verifier=ADDRESSES["pdpVerifierAddress"]["314"],
    ),
    NETWORK_CALIBRATION: ChainContracts(
        multicall3="0xcA11bde05977b3631167028862bE2a173976CA11",
        usdfc="0xb3042734b608a1B16e9e86B374A3f3e389B4cDf0",
        payments="0xd58AF75a0f6eD91e8D416cab72EBAe40E05ECD44",
        warm_storage="0x02925630df557F957f70E112bA06e50965417CA0",
        warm_storage_state_view="0xA5D87b04086B1d591026cCE10255351B5AA4689B",
        sp_registry="0x839e5c9988e4e9977d40708d0094103c0839Ac9D",
        session_key_registry=ADDRESSES["sessionKeyRegistryAddress"]["314159"],
        pdp_verifier=ADDRESSES["pdpVerifierAddress"]["314159"],
    ),
}


MAINNET = Chain(
    id=CHAIN_ID_MAINNET,
    name="Filecoin Mainnet",
    rpc_url=RPC_URLS[NETWORK_MAINNET],
    genesis_timestamp=GENESIS_TIMESTAMPS[NETWORK_MAINNET],
    contracts=CONTRACTS_BY_NETWORK[NETWORK_MAINNET],
    filbeam_domain="filbeam.io",
)

CALIBRATION = Chain(
    id=CHAIN_ID_CALIBRATION,
    name="Filecoin Calibration",
    rpc_url=RPC_URLS[NETWORK_CALIBRATION],
    genesis_timestamp=GENESIS_TIMESTAMPS[NETWORK_CALIBRATION],
    contracts=CONTRACTS_BY_NETWORK[NETWORK_CALIBRATION],
    filbeam_domain=None,
)


def as_chain(chain: Chain | str | int) -> Chain:
    if isinstance(chain, Chain):
        return chain
    if chain in (NETWORK_MAINNET, CHAIN_ID_MAINNET):
        return MAINNET
    if chain in (NETWORK_CALIBRATION, CHAIN_ID_CALIBRATION):
        return CALIBRATION
    raise ValueError(f"Unsupported chain: {chain}")
