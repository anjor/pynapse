"""Tests mirroring upstream account-debt.test.ts."""

from __future__ import annotations

from pynapse.payments.calculations import (
    MAX_UINT256,
    calculate_account_debt,
    resolve_account_state,
)


def test_resolve_healthy_account():
    result = resolve_account_state(
        {
            "funds": 1000,
            "lockup_current": 100,
            "lockup_rate": 1,
            "lockup_last_settled_at": 0,
            "current_epoch": 100,
        }
    )
    assert result.funded_until_epoch == 900
    assert result.available_funds == 800


def test_resolve_underfunded_account():
    result = resolve_account_state(
        {
            "funds": 100,
            "lockup_current": 200,
            "lockup_rate": 2,
            "lockup_last_settled_at": 1000,
            "current_epoch": 1200,
        }
    )
    assert result.funded_until_epoch == 950
    assert result.available_funds == 0


def test_resolve_partially_funded_runs_out_before_current_epoch():
    result = resolve_account_state(
        {
            "funds": 100,
            "lockup_current": 50,
            "lockup_rate": 1,
            "lockup_last_settled_at": 0,
            "current_epoch": 200,
        }
    )
    assert result.funded_until_epoch == 50
    assert result.available_funds == 0


def test_resolve_zero_rate_returns_max_uint256():
    result = resolve_account_state(
        {
            "funds": 1000,
            "lockup_current": 100,
            "lockup_rate": 0,
            "lockup_last_settled_at": 0,
            "current_epoch": 100,
        }
    )
    assert result.funded_until_epoch == MAX_UINT256
    assert result.available_funds == 900


def test_debt_healthy_account_is_zero():
    assert (
        calculate_account_debt(
            {
                "funds": 1000,
                "lockup_current": 100,
                "lockup_rate": 1,
                "lockup_last_settled_at": 0,
                "current_epoch": 100,
            }
        )
        == 0
    )


def test_debt_underfunded_account_is_positive():
    assert (
        calculate_account_debt(
            {
                "funds": 100,
                "lockup_current": 50,
                "lockup_rate": 1,
                "lockup_last_settled_at": 0,
                "current_epoch": 200,
            }
        )
        == 150
    )


def test_debt_zero_rate_is_zero():
    assert (
        calculate_account_debt(
            {
                "funds": 1000,
                "lockup_current": 100,
                "lockup_rate": 0,
                "lockup_last_settled_at": 0,
                "current_epoch": 100,
            }
        )
        == 0
    )
