from __future__ import annotations

from typing import Dict

from .constants import METADATA_KEYS


def metadata_matches(data_set_metadata: Dict[str, str], requested_metadata: Dict[str, str]) -> bool:
    if len(data_set_metadata) != len(requested_metadata):
        return False
    if not requested_metadata:
        return True
    for key, value in requested_metadata.items():
        if data_set_metadata.get(key) != value:
            return False
    return True


def combine_metadata(metadata: Dict[str, str] | None = None, with_cdn: bool | None = None) -> Dict[str, str]:
    metadata = metadata or {}
    if with_cdn is None or METADATA_KEYS["WITH_CDN"] in metadata:
        return metadata
    if with_cdn:
        combined = dict(metadata)
        combined[METADATA_KEYS["WITH_CDN"]] = ""
        return combined
    return metadata
