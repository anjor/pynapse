"""Tests for dataset namespace isolation via the ``source`` metadata key."""

from __future__ import annotations

from pynapse.utils.constants import METADATA_KEYS
from pynapse.utils.metadata import combine_metadata, metadata_matches


def test_combine_metadata_adds_source_when_present():
    result = combine_metadata({}, False, "my-app")
    assert result == {METADATA_KEYS["SOURCE"]: "my-app"}


def test_combine_metadata_skips_empty_source():
    assert combine_metadata({}, False, None) == {}
    assert combine_metadata({}, False, "") == {}


def test_combine_metadata_preserves_existing_source():
    base = {METADATA_KEYS["SOURCE"]: "explicit"}
    assert combine_metadata(base, False, "override") == base


def test_combine_metadata_with_cdn_and_source():
    result = combine_metadata({}, True, "my-app")
    assert result == {
        METADATA_KEYS["WITH_CDN"]: "",
        METADATA_KEYS["SOURCE"]: "my-app",
    }


def test_combine_metadata_does_not_mutate_input():
    base = {"foo": "bar"}
    combine_metadata(base, True, "my-app")
    assert base == {"foo": "bar"}


def test_metadata_matches_treats_different_sources_as_distinct():
    a = {METADATA_KEYS["SOURCE"]: "app-a"}
    b = {METADATA_KEYS["SOURCE"]: "app-b"}
    assert not metadata_matches(a, b)
    assert metadata_matches(a, a)
