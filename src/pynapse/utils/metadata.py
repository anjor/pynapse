from __future__ import annotations

from typing import Dict, Optional

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


def combine_metadata(
    metadata: Dict[str, str] | None = None,
    with_cdn: bool | None = None,
    source: Optional[str] = None,
) -> Dict[str, str]:
    """Combine user metadata with SDK-managed keys (``withCDN``, ``source``).

    Each managed key is added only when its option is active AND the key is
    not already present in ``metadata`` — explicit user metadata wins.
    Non-string metadata values raise ``TypeError`` with the offending key
    (mirrors FilOzone/synapse-sdk#615).
    """
    result = dict(metadata or {})
    for key, value in result.items():
        if not isinstance(value, str):
            raise TypeError(
                f"Metadata value for key {key!r} must be a string, got {type(value).__name__}"
            )

    if with_cdn and METADATA_KEYS["WITH_CDN"] not in result:
        result[METADATA_KEYS["WITH_CDN"]] = ""

    if source and METADATA_KEYS["SOURCE"] not in result:
        result[METADATA_KEYS["SOURCE"]] = source

    return result


def metadata_array_to_object(entries: list[tuple[str, str]]) -> Dict[str, str]:
    return {key: value for key, value in entries}


def metadata_object_to_entries(metadata: Dict[str, str]) -> list[dict[str, str]]:
    return [{"key": key, "value": value} for key, value in metadata.items()]
