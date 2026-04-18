"""Tests mirroring upstream calculate-additional-lockup-required.test.ts."""

from __future__ import annotations

from pynapse.core.constants import SIZE_CONSTANTS, USDFC_SYBIL_FEE
from pynapse.warm_storage import (
    calculate_additional_lockup_required,
    calculate_effective_rate,
)

PRICING = {
    "price_per_tib_per_month": 2_500_000_000_000_000_000,
    "minimum_price_per_month": 60_000_000_000_000_000,
    "epochs_per_month": 86400,
}

LOCKUP_EPOCHS = 86400


def test_new_dataset_without_cdn_has_no_cdn_fixed_lockup():
    result = calculate_additional_lockup_required(
        data_size=1000,
        current_data_set_size=0,
        **PRICING,
        lockup_epochs=LOCKUP_EPOCHS,
        is_new_data_set=True,
        with_cdn=False,
    )

    minimum_per_epoch = PRICING["minimum_price_per_month"] // PRICING["epochs_per_month"]
    assert result.cdn_fixed_lockup == 0
    assert result.sybil_fee == USDFC_SYBIL_FEE
    assert result.rate_delta_per_epoch == minimum_per_epoch
    assert result.rate_lockup_delta == minimum_per_epoch * LOCKUP_EPOCHS
    assert result.total == result.rate_lockup_delta + result.sybil_fee


def test_new_dataset_with_cdn_includes_cdn_fixed_lockup_of_1_usdfc():
    result = calculate_additional_lockup_required(
        data_size=1000,
        current_data_set_size=0,
        **PRICING,
        lockup_epochs=LOCKUP_EPOCHS,
        is_new_data_set=True,
        with_cdn=True,
    )

    cdn_fixed_lockup = 1_000_000_000_000_000_000
    assert result.cdn_fixed_lockup == cdn_fixed_lockup
    assert result.sybil_fee == USDFC_SYBIL_FEE
    assert (
        result.total
        == result.rate_lockup_delta + cdn_fixed_lockup + result.sybil_fee
    )


def test_existing_dataset_floor_to_floor_yields_zero_delta():
    result = calculate_additional_lockup_required(
        data_size=100,
        current_data_set_size=100,
        **PRICING,
        lockup_epochs=LOCKUP_EPOCHS,
        is_new_data_set=False,
        with_cdn=False,
    )

    assert result.rate_delta_per_epoch == 0
    assert result.rate_lockup_delta == 0
    assert result.cdn_fixed_lockup == 0
    assert result.sybil_fee == 0
    assert result.total == 0


def test_existing_dataset_crossing_floor_threshold_yields_positive_delta():
    tib = SIZE_CONSTANTS["TiB"]
    result = calculate_additional_lockup_required(
        data_size=tib,
        current_data_set_size=1,
        **PRICING,
        lockup_epochs=LOCKUP_EPOCHS,
        is_new_data_set=False,
        with_cdn=False,
    )

    assert result.rate_delta_per_epoch > 0
    assert result.rate_lockup_delta == result.rate_delta_per_epoch * LOCKUP_EPOCHS
    assert result.cdn_fixed_lockup == 0
    assert result.sybil_fee == 0
    assert result.total == result.rate_lockup_delta


def test_effective_rate_applies_floor_pricing():
    rate = calculate_effective_rate(size_in_bytes=1000, **PRICING)
    assert rate.rate_per_month == PRICING["minimum_price_per_month"]
    assert (
        rate.rate_per_epoch
        == PRICING["minimum_price_per_month"] // PRICING["epochs_per_month"]
    )


def test_effective_rate_scales_above_floor():
    tib = SIZE_CONSTANTS["TiB"]
    rate = calculate_effective_rate(size_in_bytes=tib, **PRICING)
    assert rate.rate_per_month == PRICING["price_per_tib_per_month"]
