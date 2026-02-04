from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

from pynapse.core.chains import Chain
from .permissions import ALL_PERMISSIONS
from .registry import SyncSessionKeyRegistry


class SessionKey:
    def __init__(self, chain: Chain, registry: SyncSessionKeyRegistry, owner_address: str, session_key_address: str) -> None:
        self._chain = chain
        self._registry = registry
        self._owner_address = owner_address
        self._session_key_address = session_key_address

    def fetch_expiries(self, permissions: Iterable[str] = ALL_PERMISSIONS) -> Dict[str, int]:
        expiries: Dict[str, int] = {}
        for permission in permissions:
            expiries[permission] = self._registry.authorization_expiry(
                self._owner_address, self._session_key_address, permission
            )
        return expiries

    def login(self, expiry: int, permissions: Iterable[str] = ALL_PERMISSIONS, origin: str = "unknown") -> str:
        return self._registry.login(self._owner_address, self._session_key_address, expiry, permissions, origin)

    def revoke(self, permissions: Iterable[str] = ALL_PERMISSIONS, origin: str = "unknown") -> str:
        return self._registry.revoke(self._owner_address, self._session_key_address, permissions, origin)
