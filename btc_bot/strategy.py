"""Pure trading logic functions — no I/O, no side effects."""
from __future__ import annotations


SL_PRICE_USDC: float = 0.72


def check_entry_window(
    seconds_left: int | float,
    min_sec: int = 15,
    max_sec: int = 70,
) -> bool:
    """Rule 1: enter only when min_sec–max_sec seconds remain in the round."""
    return min_sec <= seconds_left <= max_sec


def check_ask_filter(
    ask_cents: float,
    min_cents: int = 80,
    max_cents: int = 90,
) -> bool:
    """Rule 2: ask must be in [min_cents, max_cents] inclusive."""
    return min_cents <= ask_cents <= max_cents


def check_gap_filter(
    binance_px: float,
    okx_px: float,
    gap_max: float = 3.0,
) -> bool:
    """Rule 3: |Binance_price − OKX_price| ≤ gap_max."""
    return abs(binance_px - okx_px) <= gap_max


def should_enter(
    seconds_left: int | float,
    up_ask_cents: float,
    binance_px: float,
    okx_px: float,
    already_in: bool,
    entry_min: int = 15,
    entry_max: int = 70,
    ask_min: int = 80,
    ask_max: int = 90,
    gap_max: float = 3.0,
) -> bool:
    """Rules 1+2+3+4: all filters must pass and no open position this round."""
    if already_in:
        return False
    return (
        check_entry_window(seconds_left, entry_min, entry_max)
        and check_ask_filter(up_ask_cents, ask_min, ask_max)
        and check_gap_filter(binance_px, okx_px, gap_max)
    )


def compute_sl_price(sl_cents: int = 72) -> float:
    """Rule 5: stop-loss limit sell price in USDC (fractional)."""
    return sl_cents / 100


def choose_side(
    seconds_left: int | float,
    up_ask: float | None,
    dn_ask: float | None,
    binance_px: float,
    okx_px: float,
    already_in: bool,
    entry_min: int = 15,
    entry_max: int = 70,
    ask_min: int = 80,
    ask_max: int = 90,
    gap_max: float = 3.0,
) -> str | None:
    """
    Returns 'UP', 'DN', or None.
    Checks UP first; if UP ask not in range, checks DN ask.
    """
    if already_in:
        return None
    if not check_entry_window(seconds_left, entry_min, entry_max):
        return None
    if not check_gap_filter(binance_px, okx_px, gap_max):
        return None
    if up_ask is not None and check_ask_filter(up_ask, ask_min, ask_max):
        return "UP"
    if dn_ask is not None and check_ask_filter(dn_ask, ask_min, ask_max):
        return "DN"
    return None
