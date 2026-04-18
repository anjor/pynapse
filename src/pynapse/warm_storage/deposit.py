"""Deposit/runway/buffer calculations mirroring warm-storage costs API."""

from __future__ import annotations

from ..core.constants import (
    DEFAULT_BUFFER_EPOCHS,
    DEFAULT_RUNWAY_EPOCHS,
    LOCKUP_PERIOD,
    TIME_CONSTANTS,
)
from .calculations import calculate_additional_lockup_required


def calculate_runway_amount(*, net_rate_after_upload: int, extra_runway_epochs: int) -> int:
    """Extra funds to keep the account funded beyond the lockup period.

    Uses the net rate (current + delta) so the runway covers the full drain
    rate after the new rail is created.
    """
    return net_rate_after_upload * extra_runway_epochs


def calculate_buffer_amount(
    *,
    raw_deposit_needed: int,
    net_rate_after_upload: int,
    funded_until_epoch: int,
    current_epoch: int,
    available_funds: int,
    buffer_epochs: int,
) -> int:
    """Safety margin for epoch drift between balance check and tx execution."""
    if raw_deposit_needed > 0:
        return net_rate_after_upload * buffer_epochs

    if funded_until_epoch <= current_epoch + buffer_epochs:
        buffer_cost = net_rate_after_upload * buffer_epochs
        return max(0, buffer_cost - available_funds)

    return 0


def calculate_deposit_needed(
    *,
    data_size: int,
    current_data_set_size: int,
    price_per_tib_per_month: int,
    minimum_price_per_month: int,
    is_new_data_set: bool,
    with_cdn: bool,
    current_lockup_rate: int,
    debt: int,
    available_funds: int,
    funded_until_epoch: int,
    current_epoch: int,
    epochs_per_month: int = TIME_CONSTANTS["EPOCHS_PER_MONTH"],
    lockup_epochs: int = LOCKUP_PERIOD,
    extra_runway_epochs: int = DEFAULT_RUNWAY_EPOCHS,
    buffer_epochs: int = DEFAULT_BUFFER_EPOCHS,
) -> int:
    """Orchestrate lockup + runway + debt + buffer for total deposit needed."""
    lockup = calculate_additional_lockup_required(
        data_size=data_size,
        current_data_set_size=current_data_set_size,
        price_per_tib_per_month=price_per_tib_per_month,
        minimum_price_per_month=minimum_price_per_month,
        epochs_per_month=epochs_per_month,
        lockup_epochs=lockup_epochs,
        is_new_data_set=is_new_data_set,
        with_cdn=with_cdn,
    )

    net_rate_after_upload = current_lockup_rate + lockup.rate_delta_per_epoch
    runway = calculate_runway_amount(
        net_rate_after_upload=net_rate_after_upload,
        extra_runway_epochs=extra_runway_epochs,
    )
    raw_deposit_needed = lockup.total + runway + debt - available_funds

    # Skip buffer when no existing rails are draining and this is a new
    # dataset — the deposit lands before any rail is created.
    skip_buffer = current_lockup_rate == 0 and is_new_data_set

    buffer = 0
    if not skip_buffer:
        buffer = calculate_buffer_amount(
            raw_deposit_needed=raw_deposit_needed,
            net_rate_after_upload=net_rate_after_upload,
            funded_until_epoch=funded_until_epoch,
            current_epoch=current_epoch,
            available_funds=available_funds,
            buffer_epochs=buffer_epochs,
        )

    clamped = max(0, raw_deposit_needed)
    return clamped + buffer
