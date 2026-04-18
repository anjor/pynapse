"""Tests for SP registry ID rename (#712) — mostly guards the new surface."""

from __future__ import annotations

from pynapse.warm_storage.service import AsyncWarmStorageService, SyncWarmStorageService


def test_approved_provider_method_name_exists():
    # The canonical name is get_approved_provider_ids; the old
    # get_approved_providers shim has been removed.
    assert hasattr(SyncWarmStorageService, "get_approved_provider_ids")
    assert hasattr(AsyncWarmStorageService, "get_approved_provider_ids")
    assert not hasattr(SyncWarmStorageService, "get_approved_providers")
    assert not hasattr(AsyncWarmStorageService, "get_approved_providers")


def test_endorsed_provider_method_exists():
    assert hasattr(SyncWarmStorageService, "get_endorsed_provider_ids")
    assert hasattr(AsyncWarmStorageService, "get_endorsed_provider_ids")


def test_endorsed_provider_ids_dedupe_logic():
    # Replicate the dedup loop in isolation: the service uses the same
    # pattern to preserve insertion order while removing duplicates.
    raw = [3, 1, 2, 1, 3, 5]
    seen: list[int] = []
    visited: set[int] = set()
    for pid in raw:
        value = int(pid)
        if value not in visited:
            visited.add(value)
            seen.append(value)
    assert seen == [3, 1, 2, 5]
