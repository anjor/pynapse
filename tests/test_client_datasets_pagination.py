"""Tests for paginated client dataset queries (#717) — surface-level."""

from __future__ import annotations

from pynapse.warm_storage.service import AsyncWarmStorageService, SyncWarmStorageService


def test_paginated_methods_exist_on_sync():
    for name in (
        "get_client_data_sets",
        "get_client_data_set_ids",
        "get_client_data_sets_length",
    ):
        assert hasattr(SyncWarmStorageService, name)


def test_paginated_methods_exist_on_async():
    for name in (
        "get_client_data_sets",
        "get_client_data_set_ids",
        "get_client_data_sets_length",
    ):
        assert hasattr(AsyncWarmStorageService, name)


def test_get_client_data_sets_accepts_offset_and_limit():
    import inspect

    params = inspect.signature(
        SyncWarmStorageService.get_client_data_sets
    ).parameters
    assert "offset" in params and "limit" in params
    assert params["offset"].default == 0
    assert params["limit"].default == 0
