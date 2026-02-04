from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from eth_abi import encode as abi_encode
from eth_account import Account
from eth_account.messages import encode_structured_data
from eth_utils import to_bytes
from multiformats import CID

from .chains import Chain
from .rand import rand_u256

EIP712_TYPES: Dict[str, List[Dict[str, str]]] = {
    "EIP712Domain": [
        {"name": "name", "type": "string"},
        {"name": "version", "type": "string"},
        {"name": "chainId", "type": "uint256"},
        {"name": "verifyingContract", "type": "address"},
    ],
    "MetadataEntry": [
        {"name": "key", "type": "string"},
        {"name": "value", "type": "string"},
    ],
    "CreateDataSet": [
        {"name": "clientDataSetId", "type": "uint256"},
        {"name": "payee", "type": "address"},
        {"name": "metadata", "type": "MetadataEntry[]"},
    ],
    "Cid": [
        {"name": "data", "type": "bytes"},
    ],
    "PieceMetadata": [
        {"name": "pieceIndex", "type": "uint256"},
        {"name": "metadata", "type": "MetadataEntry[]"},
    ],
    "AddPieces": [
        {"name": "clientDataSetId", "type": "uint256"},
        {"name": "nonce", "type": "uint256"},
        {"name": "pieceData", "type": "Cid[]"},
        {"name": "pieceMetadata", "type": "PieceMetadata[]"},
    ],
    "SchedulePieceRemovals": [
        {"name": "clientDataSetId", "type": "uint256"},
        {"name": "pieceIds", "type": "uint256[]"},
    ],
    "DeleteDataSet": [
        {"name": "clientDataSetId", "type": "uint256"},
    ],
    "Permit": [
        {"name": "owner", "type": "address"},
        {"name": "spender", "type": "address"},
        {"name": "value", "type": "uint256"},
        {"name": "nonce", "type": "uint256"},
        {"name": "deadline", "type": "uint256"},
    ],
}


def get_storage_domain(chain: Chain, verifying_contract: Optional[str] = None) -> Dict[str, Any]:
    return {
        "name": "FilecoinWarmStorageService",
        "version": "1",
        "chainId": chain.id,
        "verifyingContract": verifying_contract or chain.contracts.warm_storage,
    }


def _sign_typed_data(private_key: str, domain: Dict[str, Any], primary_type: str, message: Dict[str, Any]) -> str:
    typed = {
        "types": EIP712_TYPES,
        "primaryType": primary_type,
        "domain": domain,
        "message": message,
    }
    msg = encode_structured_data(typed)
    signed = Account.sign_message(msg, private_key=private_key)
    return signed.signature.hex()


def _piece_cid_bytes(piece_cid: str) -> bytes:
    cid = CID.decode(piece_cid)
    return bytes(cid)


def sign_create_dataset(
    private_key: str,
    chain: Chain,
    client_data_set_id: int,
    payee: str,
    metadata: Sequence[Dict[str, str]],
    verifying_contract: Optional[str] = None,
) -> str:
    domain = get_storage_domain(chain, verifying_contract)
    message = {
        "clientDataSetId": int(client_data_set_id),
        "payee": payee,
        "metadata": list(metadata),
    }
    return _sign_typed_data(private_key, domain, "CreateDataSet", message)


def sign_schedule_piece_removals(
    private_key: str,
    chain: Chain,
    client_data_set_id: int,
    piece_ids: Sequence[int],
    verifying_contract: Optional[str] = None,
) -> str:
    domain = get_storage_domain(chain, verifying_contract)
    message = {
        "clientDataSetId": int(client_data_set_id),
        "pieceIds": [int(pid) for pid in piece_ids],
    }
    return _sign_typed_data(private_key, domain, "SchedulePieceRemovals", message)


def sign_add_pieces_extra_data(
    private_key: str,
    chain: Chain,
    client_data_set_id: int,
    pieces: Sequence[Tuple[str, Sequence[Dict[str, str]]]],
    nonce: Optional[int] = None,
    verifying_contract: Optional[str] = None,
) -> str:
    use_nonce = nonce if nonce is not None else rand_u256()

    piece_data = [{"data": _piece_cid_bytes(piece_cid)} for piece_cid, _ in pieces]
    piece_metadata = []
    for idx, (_, metadata) in enumerate(pieces):
        piece_metadata.append(
            {
                "pieceIndex": int(idx),
                "metadata": list(metadata),
            }
        )

    domain = get_storage_domain(chain, verifying_contract)
    message = {
        "clientDataSetId": int(client_data_set_id),
        "nonce": int(use_nonce),
        "pieceData": piece_data,
        "pieceMetadata": piece_metadata,
    }

    signature = _sign_typed_data(private_key, domain, "AddPieces", message)

    metadata_keys = [[entry["key"] for entry in metadata] for _, metadata in pieces]
    metadata_values = [[entry["value"] for entry in metadata] for _, metadata in pieces]

    encoded = abi_encode(
        ["uint256", "string[][]", "string[][]", "bytes"],
        [use_nonce, metadata_keys, metadata_values, bytes.fromhex(signature[2:])],
    )
    return "0x" + encoded.hex()


def sign_erc20_permit(
    private_key: str,
    name: str,
    version: str,
    chain_id: int,
    verifying_contract: str,
    owner: str,
    spender: str,
    value: int,
    nonce: int,
    deadline: int,
) -> str:
    domain = {
        "name": name,
        "version": version,
        "chainId": chain_id,
        "verifyingContract": verifying_contract,
    }
    message = {
        "owner": owner,
        "spender": spender,
        "value": int(value),
        "nonce": int(nonce),
        "deadline": int(deadline),
    }
    return _sign_typed_data(private_key, domain, "Permit", message)
