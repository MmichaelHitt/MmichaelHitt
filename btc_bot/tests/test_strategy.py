"""Tests for strategy.py — Block 2."""
from __future__ import annotations

import pytest
from btc_bot.strategy import (
    check_ask_filter,
    check_entry_window,
    check_gap_filter,
    choose_side,
    compute_sl_price,
    should_enter,
)


class TestCheckEntryWindow:
    def test_below_min_is_false(self):
        assert check_entry_window(14) is False

    def test_at_min_is_true(self):
        assert check_entry_window(15) is True

    def test_inside_window_is_true(self):
        assert check_entry_window(42) is True

    def test_at_max_is_true(self):
        assert check_entry_window(70) is True

    def test_above_max_is_false(self):
        assert check_entry_window(71) is False

    def test_zero_is_false(self):
        assert check_entry_window(0) is False

    def test_float_boundary_low(self):
        assert check_entry_window(14.9) is False

    def test_float_boundary_high(self):
        assert check_entry_window(70.1) is False


class TestCheckAskFilter:
    def test_below_min_is_false(self):
        assert check_ask_filter(79.9) is False

    def test_at_min_is_true(self):
        assert check_ask_filter(80) is True

    def test_inside_range_is_true(self):
        assert check_ask_filter(85) is True

    def test_at_max_is_true(self):
        assert check_ask_filter(90) is True

    def test_above_max_is_false(self):
        assert check_ask_filter(90.1) is False

    def test_zero_is_false(self):
        assert check_ask_filter(0) is False


class TestCheckGapFilter:
    def test_equal_prices_ok(self):
        assert check_gap_filter(65000.0, 65000.0) is True

    def test_gap_exactly_3_ok(self):
        assert check_gap_filter(65000.0, 65003.0) is True

    def test_gap_slightly_over_fails(self):
        assert check_gap_filter(65000.0, 65003.01) is False

    def test_negative_gap_within_limit_ok(self):
        assert check_gap_filter(65003.0, 65000.0) is True

    def test_large_gap_fails(self):
        assert check_gap_filter(65000.0, 65100.0) is False

    def test_gap_2_99_ok(self):
        assert check_gap_filter(65000.0, 65002.99) is True


class TestShouldEnter:
    BASE = dict(seconds_left=30, up_ask_cents=85, binance_px=65000.0, okx_px=65001.0)

    def test_all_conditions_met(self):
        assert should_enter(**self.BASE, already_in=False) is True

    def test_already_in_blocks_entry(self):
        assert should_enter(**self.BASE, already_in=True) is False

    def test_window_too_late(self):
        assert should_enter(seconds_left=14, up_ask_cents=85,
                            binance_px=65000, okx_px=65001, already_in=False) is False

    def test_window_too_early(self):
        assert should_enter(seconds_left=71, up_ask_cents=85,
                            binance_px=65000, okx_px=65001, already_in=False) is False

    def test_ask_too_low(self):
        assert should_enter(seconds_left=30, up_ask_cents=79,
                            binance_px=65000, okx_px=65001, already_in=False) is False

    def test_ask_too_high(self):
        assert should_enter(seconds_left=30, up_ask_cents=91,
                            binance_px=65000, okx_px=65001, already_in=False) is False

    def test_gap_too_large(self):
        assert should_enter(seconds_left=30, up_ask_cents=85,
                            binance_px=65000, okx_px=65010, already_in=False) is False

    def test_boundary_entry_window_min(self):
        assert should_enter(seconds_left=15, up_ask_cents=85,
                            binance_px=65000, okx_px=65001, already_in=False) is True

    def test_boundary_entry_window_max(self):
        assert should_enter(seconds_left=70, up_ask_cents=85,
                            binance_px=65000, okx_px=65001, already_in=False) is True

    def test_boundary_ask_min(self):
        assert should_enter(seconds_left=30, up_ask_cents=80,
                            binance_px=65000, okx_px=65001, already_in=False) is True

    def test_boundary_ask_max(self):
        assert should_enter(seconds_left=30, up_ask_cents=90,
                            binance_px=65000, okx_px=65001, already_in=False) is True

    def test_boundary_gap_exactly_3(self):
        assert should_enter(seconds_left=30, up_ask_cents=85,
                            binance_px=65000, okx_px=65003, already_in=False) is True

    def test_boundary_gap_just_over_3(self):
        assert should_enter(seconds_left=30, up_ask_cents=85,
                            binance_px=65000, okx_px=65003.01, already_in=False) is False


class TestChooseSide:
    B = dict(seconds_left=30, binance_px=65000.0, okx_px=65001.0, already_in=False)

    def test_up_ask_in_range_returns_up(self):
        assert choose_side(up_ask=85.0, dn_ask=None, **self.B) == "UP"

    def test_dn_ask_in_range_returns_dn(self):
        assert choose_side(up_ask=None, dn_ask=85.0, **self.B) == "DN"

    def test_up_takes_priority_over_dn(self):
        assert choose_side(up_ask=85.0, dn_ask=85.0, **self.B) == "UP"

    def test_neither_in_range_returns_none(self):
        assert choose_side(up_ask=95.0, dn_ask=95.0, **self.B) is None

    def test_both_none_returns_none(self):
        assert choose_side(up_ask=None, dn_ask=None, **self.B) is None

    def test_already_in_returns_none(self):
        assert choose_side(up_ask=85.0, dn_ask=85.0,
                           seconds_left=30, binance_px=65000, okx_px=65001,
                           already_in=True) is None

    def test_outside_window_returns_none(self):
        assert choose_side(up_ask=85.0, dn_ask=None,
                           seconds_left=200, binance_px=65000, okx_px=65001,
                           already_in=False) is None

    def test_gap_too_large_returns_none(self):
        assert choose_side(up_ask=85.0, dn_ask=None,
                           seconds_left=30, binance_px=65000, okx_px=65010,
                           already_in=False) is None

    def test_dn_ask_out_of_range_returns_none(self):
        assert choose_side(up_ask=None, dn_ask=95.0, **self.B) is None


class TestComputeSlPrice:
    def test_sl_price_is_0_72(self):
        assert compute_sl_price() == pytest.approx(0.72)

    def test_sl_price_type_is_float(self):
        assert isinstance(compute_sl_price(), float)
