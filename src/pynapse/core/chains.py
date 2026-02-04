from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class ChainContracts:
    multicall3: str
    usdfc: str
    payments: str
    warm_storage: str
    warm_storage_state_view: str
    sp_registry: str


@dataclass(frozen=True)
class Chain:
    id: int
    name: str
    rpc_url: str
    genesis_timestamp: int
    contracts: ChainContracts


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
        payments="0xC8C3C94aa8C60E0dFF060D4c3acBa0bC16e4e0ec",
        warm_storage="0x8408502033C418E1bbC97cE9ac48E5528F371A9f",
        warm_storage_state_view="0x9e4e6699d8F67dFc883d6b0A7344Bd56F7E80B46",
        sp_registry="0xf55dDbf63F1b55c3F1D4FA7e339a68AB7b64A5eB",
    ),
    NETWORK_CALIBRATION: ChainContracts(
        multicall3="0xcA11bde05977b3631167028862bE2a173976CA11",
        usdfc="0xb3042734b608a1B16e9e86B374A3f3e389B4cDf0",
        payments="0xD58af75a0F6ed91E8d416CAB72Ebae40E05ecD44",
        warm_storage="0x02925630df557F957f70E112bA06e50965417CA0",
        warm_storage_state_view="0xA5D87b04086B1d591026cCE10255351B5AA4689B",
        sp_registry="0x839e5c9988e4e9977d40708d0094103c0839Ac9D",
    ),
}


MAINNET = Chain(
    id=CHAIN_ID_MAINNET,
    name="Filecoin Mainnet",
    rpc_url=RPC_URLS[NETWORK_MAINNET],
    genesis_timestamp=GENESIS_TIMESTAMPS[NETWORK_MAINNET],
    contracts=CONTRACTS_BY_NETWORK[NETWORK_MAINNET],
)

CALIBRATION = Chain(
    id=CHAIN_ID_CALIBRATION,
    name="Filecoin Calibration",
    rpc_url=RPC_URLS[NETWORK_CALIBRATION],
    genesis_timestamp=GENESIS_TIMESTAMPS[NETWORK_CALIBRATION],
    contracts=CONTRACTS_BY_NETWORK[NETWORK_CALIBRATION],
)


def as_chain(chain: Chain | str | int) -> Chain:
    if isinstance(chain, Chain):
        return chain
    if chain in (NETWORK_MAINNET, CHAIN_ID_MAINNET):
        return MAINNET
    if chain in (NETWORK_CALIBRATION, CHAIN_ID_CALIBRATION):
        return CALIBRATION
    raise ValueError(f"Unsupported chain: {chain}")
