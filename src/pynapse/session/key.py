from __future__ import annotations

import time
from typing import Dict, Iterable

from pynapse.core.chains import Chain
from .permissions import ALL_PERMISSIONS, DEFAULT_FWSS_PERMISSIONS
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

    def has_permission(self, permission: str, *, now: int | None = None) -> bool:
        """Return True if this session key is authorized for ``permission``.

        ``permission`` may be either a name (e.g. ``"CreateDataSet"``) or the
        corresponding EIP-712 type hash. Mirrors ``sessionKey.hasPermission``
        introduced in FilOzone/synapse-sdk#618.
        """
        from .permissions import get_permission_from_type_hash

        perm_name = permission
        if permission.startswith("0x"):
            try:
                perm_name = get_permission_from_type_hash(permission)
            except ValueError:
                return False

        expiry = self._registry.authorization_expiry(
            self._owner_address, self._session_key_address, perm_name
        )
        current = now if now is not None else int(time.time())
        return expiry > current

    def has_permissions(self, permissions: Iterable[str] = DEFAULT_FWSS_PERMISSIONS) -> bool:
        """Return True only when every permission in ``permissions`` is held."""
        return all(self.has_permission(p) for p in permissions)

    def login(self, expiry: int, permissions: Iterable[str] = ALL_PERMISSIONS, origin: str = "unknown") -> str:
        return self._registry.login(self._owner_address, self._session_key_address, expiry, permissions, origin)

    def revoke(self, permissions: Iterable[str] = ALL_PERMISSIONS, origin: str = "unknown") -> str:
        return self._registry.revoke(self._owner_address, self._session_key_address, permissions, origin)
