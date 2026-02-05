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
        payments=ADDRESSES["filecoinPayV1Address"]["314"],
        warm_storage=ADDRESSES["filecoinWarmStorageServiceAddress"]["314"],
        warm_storage_state_view=ADDRESSES["filecoinWarmStorageServiceStateViewAddress"]["314"],
        sp_registry=ADDRESSES["serviceProviderRegistryAddress"]["314"],
        session_key_registry=ADDRESSES["sessionKeyRegistryAddress"]["314"],
        pdp_verifier=ADDRESSES["pdpVerifierAddress"]["314"],
    ),
    NETWORK_CALIBRATION: ChainContracts(
        multicall3="0xcA11bde05977b3631167028862bE2a173976CA11",
        usdfc="0xb3042734b608a1B16e9e86B374A3f3e389B4cDf0",
        payments=ADDRESSES["filecoinPayV1Address"]["314159"],
        warm_storage=ADDRESSES["filecoinWarmStorageServiceAddress"]["314159"],
        warm_storage_state_view=ADDRESSES["filecoinWarmStorageServiceStateViewAddress"]["314159"],
        sp_registry=ADDRESSES["serviceProviderRegistryAddress"]["314159"],
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
