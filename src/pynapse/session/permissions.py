from __future__ import annotations

from typing import Dict, List

from eth_utils import keccak

from pynapse.core.typed_data import EIP712_TYPES

SessionKeyPermission = str

ALL_PERMISSIONS: List[SessionKeyPermission] = [
    "CreateDataSet",
    "AddPieces",
    "SchedulePieceRemovals",
    "DeleteDataSet",
]


def _dependencies(types: Dict[str, List[Dict[str, str]]], primary: str) -> List[str]:
    deps = []
    for field in types[primary]:
        t = field["type"].replace("[]", "")
        if t in types and t not in deps and t != primary:
            deps.append(t)
            deps.extend([d for d in _dependencies(types, t) if d not in deps])
    return deps


def _encode_type(types: Dict[str, List[Dict[str, str]]], primary: str) -> str:
    deps = _dependencies(types, primary)
    deps = sorted(deps)
    type_list = [primary] + deps
    parts = []
    for t in type_list:
        fields = ",".join([f"{f['type']} {f['name']}" for f in types[t]])
        parts.append(f"{t}({fields})")
    return "".join(parts)


def type_hash(primary_type: str) -> str:
    encoded = _encode_type(EIP712_TYPES, primary_type)
    return "0x" + keccak(text=encoded).hex()


SESSION_KEY_PERMISSIONS: Dict[SessionKeyPermission, str] = {
    "CreateDataSet": type_hash("CreateDataSet"),
    "AddPieces": type_hash("AddPieces"),
    "SchedulePieceRemovals": type_hash("SchedulePieceRemovals"),
    "DeleteDataSet": type_hash("DeleteDataSet"),
}


def get_permission_from_type_hash(type_hash_value: str) -> SessionKeyPermission:
    for perm, h in SESSION_KEY_PERMISSIONS.items():
        if h.lower() == type_hash_value.lower():
            return perm
    raise ValueError(f"Permission not found for type hash: {type_hash_value}")
