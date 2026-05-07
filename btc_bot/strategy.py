"""Pure trading logic functions — no I/O, no side effects."""
from __future__ import annotations


SL_PRICE_USDC: float = 0.72


def check_entry_window(seconds_left: int | float) -> bool:
    """Rule 1: enter only when 15–70 seconds remain in the round."""
    return 15 <= seconds_left <= 70


def check_ask_filter(up_ask_cents: float) -> bool:
    """Rule 2: UP ask must be in [80, 90] cents inclusive."""
    return 80 <= up_ask_cents <= 90


def check_gap_filter(binance_px: float, okx_px: float) -> bool:
    """Rule 3: |Binance_price − OKX_price| ≤ $3."""
    return abs(binance_px - okx_px) <= 3.0


def should_enter(
    seconds_left: int | float,
    up_ask_cents: float,
    binance_px: float,
    okx_px: float,
    already_in: bool,
) -> bool:
    """
    Rules 1+2+3+4: all filters must pass and no open position this round.
    """
    if already_in:
        return False
    return (
        check_entry_window(seconds_left)
        and check_ask_filter(up_ask_cents)
        and check_gap_filter(binance_px, okx_px)
    )


def compute_sl_price() -> float:
    """Rule 5: stop-loss limit sell price in USDC (fractional)."""
    return SL_PRICE_USDC


def choose_side(
    seconds_left: int | float,
    up_ask: float | None,
    dn_ask: float | None,
    binance_px: float,
    okx_px: float,
    already_in: bool,
) -> str | None:
    """
    Returns 'UP', 'DN', or None.
    Checks UP first; if UP ask not in range, checks DN ask.
    """
    if already_in:
        return None
    if not check_entry_window(seconds_left):
        return None
    if not check_gap_filter(binance_px, okx_px):
        return None
    if up_ask is not None and check_ask_filter(up_ask):
        return "UP"
    if dn_ask is not None and check_ask_filter(dn_ask):
        return "DN"
    return None
