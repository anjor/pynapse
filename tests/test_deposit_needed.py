"""Tests mirroring upstream calculate-deposit-needed.test.ts."""

from __future__ import annotations

from pynapse.payments.calculations import MAX_UINT256
from pynapse.warm_storage.deposit import (
    calculate_buffer_amount,
    calculate_deposit_needed,
    calculate_runway_amount,
)

PRICING = {
    "price_per_tib_per_month": 2_500_000_000_000_000_000,
    "minimum_price_per_month": 60_000_000_000_000_000,
    "epochs_per_month": 86400,
}


def test_runway_amount():
    assert (
        calculate_runway_amount(net_rate_after_upload=15, extra_runway_epochs=100)
        == 1500
    )


def test_buffer_with_positive_raw_deposit():
    result = calculate_buffer_amount(
        raw_deposit_needed=100,
        net_rate_after_upload=15,
        funded_until_epoch=500,
        current_epoch=100,
        available_funds=200,
        buffer_epochs=20,
    )
    assert result == 300


def test_buffer_with_positive_raw_no_delta():
    result = calculate_buffer_amount(
        raw_deposit_needed=100,
        net_rate_after_upload=10,
        funded_until_epoch=500,
        current_epoch=100,
        available_funds=200,
        buffer_epochs=20,
    )
    assert result == 200


def test_buffer_negative_raw_within_buffer_window():
    result = calculate_buffer_amount(
        raw_deposit_needed=-50,
        net_rate_after_upload=15,
        funded_until_epoch=110,
        current_epoch=100,
        available_funds=50,
        buffer_epochs=20,
    )
    assert result == 250


def test_buffer_negative_raw_beyond_buffer_window():
    result = calculate_buffer_amount(
        raw_deposit_needed=-50,
        net_rate_after_upload=15,
        funded_until_epoch=500,
        current_epoch=100,
        available_funds=200,
        buffer_epochs=20,
    )
    assert result == 0


def test_deposit_healthy_account_returns_zero():
    assert (
        calculate_deposit_needed(
            data_size=1000,
            current_data_set_size=0,
            **PRICING,
            lockup_epochs=86400,
            is_new_data_set=True,
            with_cdn=False,
            current_lockup_rate=0,
            extra_runway_epochs=0,
            debt=0,
            available_funds=100_000_000_000_000_000_000,
            funded_until_epoch=MAX_UINT256,
            current_epoch=1000,
            buffer_epochs=10,
        )
        == 0
    )


def test_deposit_new_dataset_no_existing_rails_skips_buffer():
    base = dict(
        data_size=1000,
        current_data_set_size=0,
        **PRICING,
        lockup_epochs=86400,
        is_new_data_set=True,
        with_cdn=False,
        current_lockup_rate=0,
        extra_runway_epochs=0,
        debt=0,
        available_funds=0,
        funded_until_epoch=0,
        current_epoch=1000,
    )
    with_buffer = calculate_deposit_needed(**base, buffer_epochs=100)
    without_buffer = calculate_deposit_needed(**base, buffer_epochs=0)
    assert with_buffer == without_buffer
    assert with_buffer > 0


def test_deposit_new_dataset_with_existing_rails_applies_buffer():
    base = dict(
        data_size=1000,
        current_data_set_size=0,
        **PRICING,
        lockup_epochs=86400,
        is_new_data_set=True,
        with_cdn=False,
        current_lockup_rate=100_000_000_000_000,
        extra_runway_epochs=0,
        debt=0,
        available_funds=0,
        funded_until_epoch=0,
        current_epoch=1000,
    )
    with_buffer = calculate_deposit_needed(**base, buffer_epochs=100)
    without_buffer = calculate_deposit_needed(**base, buffer_epochs=0)
    assert with_buffer > without_buffer


def test_deposit_underfunded_includes_debt():
    debt = 5_000_000_000_000_000_000
    result = calculate_deposit_needed(
        data_size=1000,
        current_data_set_size=0,
        **PRICING,
        lockup_epochs=86400,
        is_new_data_set=True,
        with_cdn=False,
        current_lockup_rate=10,
        extra_runway_epochs=0,
        debt=debt,
        available_funds=0,
        funded_until_epoch=50,
        current_epoch=1000,
        buffer_epochs=10,
    )
    assert result >= debt
