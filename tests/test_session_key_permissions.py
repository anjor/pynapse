"""Tests for session-key permission helpers (portion of #618)."""

from __future__ import annotations

from pynapse.session import (
    ADD_PIECES_PERMISSION,
    CREATE_DATA_SET_PERMISSION,
    DEFAULT_FWSS_PERMISSIONS,
    DELETE_DATA_SET_PERMISSION,
    SCHEDULE_PIECE_REMOVALS_PERMISSION,
    SessionKey,
    get_permission_from_type_hash,
)
from pynapse.session.permissions import SESSION_KEY_PERMISSIONS


def test_permission_constants_match_session_key_permissions_map():
    assert CREATE_DATA_SET_PERMISSION == SESSION_KEY_PERMISSIONS["CreateDataSet"]
    assert ADD_PIECES_PERMISSION == SESSION_KEY_PERMISSIONS["AddPieces"]
    assert SCHEDULE_PIECE_REMOVALS_PERMISSION == SESSION_KEY_PERMISSIONS[
        "SchedulePieceRemovals"
    ]
    assert DELETE_DATA_SET_PERMISSION == SESSION_KEY_PERMISSIONS["DeleteDataSet"]


def test_default_fwss_permissions_covers_all_four():
    assert set(DEFAULT_FWSS_PERMISSIONS) == {
        "CreateDataSet",
        "AddPieces",
        "SchedulePieceRemovals",
        "DeleteDataSet",
    }


def test_type_hash_roundtrip():
    assert get_permission_from_type_hash(CREATE_DATA_SET_PERMISSION) == "CreateDataSet"


class _FakeRegistry:
    def __init__(self, expiries):
        self._expiries = expiries

    def authorization_expiry(self, owner, session, permission):
        return self._expiries.get(permission, 0)


def test_has_permission_accepts_name_and_hash():
    registry = _FakeRegistry({"CreateDataSet": 1000, "AddPieces": 100})
    key = SessionKey(
        chain=object(),
        registry=registry,  # type: ignore[arg-type]
        owner_address="0x" + "11" * 20,
        session_key_address="0x" + "22" * 20,
    )
    assert key.has_permission("CreateDataSet", now=500) is True
    assert key.has_permission(CREATE_DATA_SET_PERMISSION, now=500) is True
    assert key.has_permission("AddPieces", now=500) is False  # expired
    assert key.has_permission("DeleteDataSet", now=500) is False  # not granted


def test_has_permissions_returns_false_when_any_missing():
    registry = _FakeRegistry({"CreateDataSet": 2**32})  # only one permission granted
    key = SessionKey(
        chain=object(),
        registry=registry,  # type: ignore[arg-type]
        owner_address="0x" + "11" * 20,
        session_key_address="0x" + "22" * 20,
    )
    assert key.has_permissions() is False
    assert key.has_permission("CreateDataSet") is True
