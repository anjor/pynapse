"""Tests for the small-fixes batch (#615, #687, #701)."""

from __future__ import annotations

import pytest

from pynapse.sp_registry.pdp_capabilities import decode_pdp_capabilities
from pynapse.sp_registry.types import PDPOffering
from pynapse.storage.async_manager import AsyncStorageManager
from pynapse.storage.manager import StorageManager
from pynapse.utils.metadata import combine_metadata


def test_combine_metadata_rejects_non_string_value():
    # Mirrors upstream #615 — a non-string value raises with the key name.
    with pytest.raises(TypeError, match="foo"):
        combine_metadata({"foo": 123})  # type: ignore[dict-item]


def test_decode_pdp_capabilities_preserves_unknown_keys():
    # #687 — preserve non-standard capabilities (serviceStatus etc.).
    capabilities = {
        "serviceURL": "0x" + "https://sp.example".encode().hex(),
        "minPieceSizeInBytes": "0x" + (1024).to_bytes(32, "big").hex(),
        "maxPieceSizeInBytes": "0x" + (10 * 1024 * 1024).to_bytes(32, "big").hex(),
        "storagePricePerTibPerDay": "0x" + (1).to_bytes(32, "big").hex(),
        "minProvingPeriodInEpochs": "0x" + (1).to_bytes(32, "big").hex(),
        "location": "0x" + "us-east".encode().hex(),
        "paymentTokenAddress": "0x" + "00" * 32,
        # Non-standard capability that downstream dealbot etc. read.
        "serviceStatus": "0x" + "ready".encode().hex(),
        "customFlag": "0x01",
    }
    offering: PDPOffering = decode_pdp_capabilities(capabilities)
    assert offering.extra_capabilities == {
        "serviceStatus": "0x" + "ready".encode().hex(),
        "customFlag": "0x01",
    }


class _DummyChain:
    pass


def test_storage_manager_exposes_source_and_with_cdn_getters():
    # #701 — source and with_cdn should be readable on StorageManager.
    manager = StorageManager(
        chain=_DummyChain(),
        private_key="0x" + "11" * 32,
        source="my-app",
        with_cdn=True,
    )
    assert manager.source == "my-app"
    assert manager.with_cdn is True


def test_async_storage_manager_exposes_source_and_with_cdn_getters():
    manager = AsyncStorageManager(
        chain=_DummyChain(),
        private_key="0x" + "11" * 32,
        source="other-app",
        with_cdn=False,
    )
    assert manager.source == "other-app"
    assert manager.with_cdn is False
