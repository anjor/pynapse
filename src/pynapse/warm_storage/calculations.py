"""Lockup and rate calculations mirroring the FilecoinWarmStorageService contract."""

from __future__ import annotations

from dataclasses import dataclass

from ..core.constants import (
    CDN_FIXED_LOCKUP,
    LOCKUP_PERIOD,
    SIZE_CONSTANTS,
    TIME_CONSTANTS,
    USDFC_SYBIL_FEE,
)


@dataclass(frozen=True)
class EffectiveRate:
    rate_per_epoch: int
    rate_per_month: int


@dataclass(frozen=True)
class AdditionalLockup:
    rate_delta_per_epoch: int
    rate_lockup_delta: int
    cdn_fixed_lockup: int
    sybil_fee: int
    total: int


def calculate_effective_rate(
    *,
    size_in_bytes: int,
    price_per_tib_per_month: int,
    minimum_price_per_month: int,
    epochs_per_month: int,
) -> EffectiveRate:
    """Mirror the contract's ``_calculateStorageRate`` with floor pricing.

    ``rate_per_epoch`` matches the on-chain rail rate and must be used for
    lockup math. ``rate_per_month`` preserves precision and scales linearly
    with size, so use it for display and cost comparisons.
    """
    tib = SIZE_CONSTANTS["TiB"]

    natural_per_month = (price_per_tib_per_month * size_in_bytes) // tib
    natural_per_epoch = (price_per_tib_per_month * size_in_bytes) // (
        tib * epochs_per_month
    )
    minimum_per_epoch = minimum_price_per_month // epochs_per_month

    rate_per_month = max(natural_per_month, minimum_price_per_month)
    rate_per_epoch = max(natural_per_epoch, minimum_per_epoch)

    return EffectiveRate(rate_per_epoch=rate_per_epoch, rate_per_month=rate_per_month)


def calculate_additional_lockup_required(
    *,
    data_size: int,
    current_data_set_size: int,
    price_per_tib_per_month: int,
    minimum_price_per_month: int,
    is_new_data_set: bool,
    with_cdn: bool,
    epochs_per_month: int = TIME_CONSTANTS["EPOCHS_PER_MONTH"],
    lockup_epochs: int = LOCKUP_PERIOD,
) -> AdditionalLockup:
    """Compute the additional lockup required for an upload.

    Handles floor-to-floor transitions: when both the current size and the new
    total are below the pricing floor, the rate delta is zero. New dataset
    creations incur the fixed CDN lockup (if CDN is enabled) and the USDFC
    sybil fee.
    """
    rate_params = {
        "price_per_tib_per_month": price_per_tib_per_month,
        "minimum_price_per_month": minimum_price_per_month,
        "epochs_per_month": epochs_per_month,
    }

    if current_data_set_size > 0 and not is_new_data_set:
        new_rate = calculate_effective_rate(
            size_in_bytes=current_data_set_size + data_size, **rate_params
        )
        current_rate = calculate_effective_rate(
            size_in_bytes=current_data_set_size, **rate_params
        )
        rate_delta_per_epoch = new_rate.rate_per_epoch - current_rate.rate_per_epoch
        if rate_delta_per_epoch < 0:
            rate_delta_per_epoch = 0
    else:
        new_rate = calculate_effective_rate(size_in_bytes=data_size, **rate_params)
        rate_delta_per_epoch = new_rate.rate_per_epoch

    rate_lockup_delta = rate_delta_per_epoch * lockup_epochs
    cdn_fixed_lockup = CDN_FIXED_LOCKUP["total"] if (is_new_data_set and with_cdn) else 0
    sybil_fee = USDFC_SYBIL_FEE if is_new_data_set else 0

    return AdditionalLockup(
        rate_delta_per_epoch=rate_delta_per_epoch,
        rate_lockup_delta=rate_lockup_delta,
        cdn_fixed_lockup=cdn_fixed_lockup,
        sybil_fee=sybil_fee,
        total=rate_lockup_delta + cdn_fixed_lockup + sybil_fee,
    )
