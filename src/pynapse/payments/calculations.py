"""Pure account-state and debt calculations mirroring the Payments contract."""

from __future__ import annotations

from dataclasses import dataclass

MAX_UINT256 = (1 << 256) - 1


@dataclass(frozen=True)
class AccountStateInputs:
    funds: int
    lockup_current: int
    lockup_rate: int
    lockup_last_settled_at: int
    current_epoch: int


@dataclass(frozen=True)
class ResolvedAccountState:
    funded_until_epoch: int
    available_funds: int


def _as_inputs(params: AccountStateInputs | dict) -> AccountStateInputs:
    if isinstance(params, AccountStateInputs):
        return params
    return AccountStateInputs(**params)


def calculate_account_debt(params: AccountStateInputs | dict) -> int:
    """Compute account debt — the unsettled lockup amount exceeding funds."""
    p = _as_inputs(params)
    elapsed = p.current_epoch - p.lockup_last_settled_at
    total_owed = p.lockup_current + p.lockup_rate * elapsed
    return max(0, total_owed - p.funds)


def resolve_account_state(params: AccountStateInputs | dict) -> ResolvedAccountState:
    """Project account state forward to ``current_epoch`` via simulated settlement.

    Pure function — no RPC call. Mirrors the contract-side computation of
    ``fundedUntilEpoch`` and ``availableFunds``.
    """
    p = _as_inputs(params)

    if p.lockup_rate == 0:
        funded_until_epoch = MAX_UINT256
    else:
        # Integer division — JS bigint uses truncation toward zero, matching
        # Python's behavior only for non-negative operands. The JS source
        # allows negative numerators (funds < lockup_current), which Python
        # would round toward -inf. Emulate JS truncation explicitly.
        numerator = p.funds - p.lockup_current
        quotient = _trunc_div(numerator, p.lockup_rate)
        funded_until_epoch = p.lockup_last_settled_at + quotient

    simulated_settled_at = min(funded_until_epoch, p.current_epoch)
    simulated_lockup_current = p.lockup_current + p.lockup_rate * (
        simulated_settled_at - p.lockup_last_settled_at
    )
    raw_available = p.funds - simulated_lockup_current
    available_funds = max(0, raw_available)

    return ResolvedAccountState(
        funded_until_epoch=funded_until_epoch,
        available_funds=available_funds,
    )


def _trunc_div(a: int, b: int) -> int:
    """Truncated division (toward zero) to match JS bigint semantics."""
    q, r = divmod(a, b)
    if r != 0 and (a < 0) != (b < 0):
        q += 1
    return q
