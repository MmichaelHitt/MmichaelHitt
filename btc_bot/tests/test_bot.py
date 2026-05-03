"""Integration tests for bot.py — Block 7."""
from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from btc_bot.bot import BotState, log_trade, market_watcher_thread, seconds_to_close, trading_loop
from btc_bot.config import Config


def make_config(dry_run: bool = True) -> Config:
    return Config(dry_run=dry_run, proxy_url="")


def make_state(market: dict | None = None) -> BotState:
    state = BotState()
    if market:
        state.current_market.update(market)
    return state


def future_market(seconds: int = 120) -> dict:
    end = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    return {
        "slug": "btc-updown-5m-test",
        "up_token": "up_token_abc",
        "dn_token": "dn_token_xyz",
        "end_date": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


class TestSecondsToClose:
    def test_future_date_returns_positive(self):
        end = (datetime.now(timezone.utc) + timedelta(seconds=30)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        result = seconds_to_close(end)
        assert result is not None
        assert 28 <= result <= 31

    def test_past_date_returns_zero(self):
        end = (datetime.now(timezone.utc) - timedelta(seconds=10)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        assert seconds_to_close(end) == 0

    def test_invalid_iso_returns_none(self):
        assert seconds_to_close("not-a-date") is None


class TestLogTrade:
    def test_writes_csv_row(self, tmp_path):
        import btc_bot.bot as bot_module
        orig = bot_module.TRADES_CSV
        bot_module.TRADES_CSV = tmp_path / "trades.csv"
        try:
            log_trade("btc-updown-5m-123", "BUY", 85.0)
            content = (tmp_path / "trades.csv").read_text()
            assert "BUY" in content
            assert "85.0" in content
            assert "btc-updown-5m-123" in content
        finally:
            bot_module.TRADES_CSV = orig

    def test_writes_header_on_first_write(self, tmp_path):
        import btc_bot.bot as bot_module
        orig = bot_module.TRADES_CSV
        bot_module.TRADES_CSV = tmp_path / "trades.csv"
        try:
            log_trade("slug", "BUY", 85.0)
            content = (tmp_path / "trades.csv").read_text()
            assert "date" in content
            assert "slug" in content
        finally:
            bot_module.TRADES_CSV = orig


class TestTradingLoopSingleEntry:
    """Verify only one trade per round even with multiple ticks."""

    @pytest.mark.asyncio
    async def test_only_one_entry_per_round(self, tmp_path):
        import btc_bot.bot as bot_module
        orig_csv = bot_module.TRADES_CSV
        bot_module.TRADES_CSV = tmp_path / "trades.csv"

        cfg = make_config(dry_run=True)
        market = future_market(seconds=40)
        state = make_state(market)

        mock_clob = MagicMock()
        mock_clob.up_ask = 85.0

        mock_feed = MagicMock()
        mock_feed.binance_price = 65000.0
        mock_feed.okx_price = 65001.0

        mock_order_mgr = MagicMock()
        mock_order_mgr.place_buy_market.return_value = "dry-buy-001"
        mock_order_mgr.place_sl_limit.return_value = "dry-sl-001"

        import logging
        logger = logging.getLogger("test_bot")

        tick_count = 0

        async def fake_sleep(seconds):
            nonlocal tick_count
            tick_count += 1
            if tick_count >= 4:
                raise asyncio.CancelledError()

        with patch("btc_bot.bot.asyncio.sleep", new=fake_sleep), \
             patch("btc_bot.bot.log_trade"):
            try:
                await trading_loop(state, mock_feed, mock_clob, mock_order_mgr, cfg, logger)
            except asyncio.CancelledError:
                pass

        assert mock_order_mgr.place_buy_market.call_count == 1
        assert state.entered_this_round is True

        bot_module.TRADES_CSV = orig_csv

    @pytest.mark.asyncio
    async def test_no_entry_when_already_in(self):
        cfg = make_config(dry_run=True)
        market = future_market(seconds=40)
        state = make_state(market)
        state.entered_this_round = True

        mock_clob = MagicMock()
        mock_clob.up_ask = 85.0

        mock_feed = MagicMock()
        mock_feed.binance_price = 65000.0
        mock_feed.okx_price = 65001.0

        mock_order_mgr = MagicMock()

        import logging
        logger = logging.getLogger("test_bot_no_entry")

        tick_count = 0

        async def fake_sleep(seconds):
            nonlocal tick_count
            tick_count += 1
            if tick_count >= 3:
                raise asyncio.CancelledError()

        with patch("btc_bot.bot.asyncio.sleep", new=fake_sleep):
            try:
                await trading_loop(state, mock_feed, mock_clob, mock_order_mgr, cfg, logger)
            except asyncio.CancelledError:
                pass

        mock_order_mgr.place_buy_market.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_entry_when_ask_out_of_range(self):
        cfg = make_config(dry_run=True)
        market = future_market(seconds=40)
        state = make_state(market)

        mock_clob = MagicMock()
        mock_clob.up_ask = 95.0  # out of 80-90 range

        mock_feed = MagicMock()
        mock_feed.binance_price = 65000.0
        mock_feed.okx_price = 65001.0

        mock_order_mgr = MagicMock()

        import logging
        logger = logging.getLogger("test_bot_no_ask")

        tick_count = 0

        async def fake_sleep(seconds):
            nonlocal tick_count
            tick_count += 1
            if tick_count >= 3:
                raise asyncio.CancelledError()

        with patch("btc_bot.bot.asyncio.sleep", new=fake_sleep):
            try:
                await trading_loop(state, mock_feed, mock_clob, mock_order_mgr, cfg, logger)
            except asyncio.CancelledError:
                pass

        mock_order_mgr.place_buy_market.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_entry_outside_window(self):
        cfg = make_config(dry_run=True)
        # 200 seconds left — outside 15-70 window
        market = future_market(seconds=200)
        state = make_state(market)

        mock_clob = MagicMock()
        mock_clob.up_ask = 85.0

        mock_feed = MagicMock()
        mock_feed.binance_price = 65000.0
        mock_feed.okx_price = 65001.0

        mock_order_mgr = MagicMock()

        import logging
        logger = logging.getLogger("test_bot_no_window")

        tick_count = 0

        async def fake_sleep(seconds):
            nonlocal tick_count
            tick_count += 1
            if tick_count >= 3:
                raise asyncio.CancelledError()

        with patch("btc_bot.bot.asyncio.sleep", new=fake_sleep):
            try:
                await trading_loop(state, mock_feed, mock_clob, mock_order_mgr, cfg, logger)
            except asyncio.CancelledError:
                pass

        mock_order_mgr.place_buy_market.assert_not_called()

    @pytest.mark.asyncio
    async def test_sl_placed_immediately_after_buy(self, tmp_path):
        import btc_bot.bot as bot_module
        orig_csv = bot_module.TRADES_CSV
        bot_module.TRADES_CSV = tmp_path / "trades.csv"

        cfg = make_config(dry_run=True)
        market = future_market(seconds=40)
        state = make_state(market)

        mock_clob = MagicMock()
        mock_clob.up_ask = 85.0

        mock_feed = MagicMock()
        mock_feed.binance_price = 65000.0
        mock_feed.okx_price = 65001.0

        mock_order_mgr = MagicMock()
        mock_order_mgr.place_buy_market.return_value = "buy-111"
        mock_order_mgr.place_sl_limit.return_value = "sl-222"

        import logging
        logger = logging.getLogger("test_bot_sl")

        tick_count = 0

        async def fake_sleep(seconds):
            nonlocal tick_count
            tick_count += 1
            if tick_count >= 3:
                raise asyncio.CancelledError()

        with patch("btc_bot.bot.asyncio.sleep", new=fake_sleep), \
             patch("btc_bot.bot.log_trade"):
            try:
                await trading_loop(state, mock_feed, mock_clob, mock_order_mgr, cfg, logger)
            except asyncio.CancelledError:
                pass

        mock_order_mgr.place_sl_limit.assert_called_once()
        call_args = mock_order_mgr.place_sl_limit.call_args
        assert call_args[0][1] == pytest.approx(0.72)

        bot_module.TRADES_CSV = orig_csv


class TestMarketWatcherThread:
    def test_resets_entered_flag_on_switch(self):
        cfg = make_config()
        # Market ends in 5 seconds — within switch threshold (8s)
        end = (datetime.now(timezone.utc) + timedelta(seconds=5)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        market = {
            "slug": "btc-updown-5m-old",
            "up_token": "up_old",
            "dn_token": "dn_old",
            "end_date": end,
        }
        state = make_state(market)
        state.entered_this_round = True

        next_market = {
            "slug": "btc-updown-5m-new",
            "up_token": "up_new",
            "dn_token": "dn_new",
            "end_date": (datetime.now(timezone.utc) + timedelta(seconds=310)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        }

        mock_clob = MagicMock()
        import logging
        logger = logging.getLogger("test_watcher")

        switched_event = threading.Event()
        original_restart = mock_clob.restart.side_effect

        def on_restart(*args, **kwargs):
            switched_event.set()

        mock_clob.restart.side_effect = on_restart

        sleep_calls = [0]

        def fake_sleep(seconds):
            sleep_calls[0] += 1
            if sleep_calls[0] > 5:
                raise SystemExit()

        with patch("btc_bot.bot.find_next_market", return_value=next_market), \
             patch("btc_bot.bot.time.sleep", side_effect=fake_sleep):
            try:
                market_watcher_thread(state, mock_clob, cfg, logger)
            except SystemExit:
                pass

        assert switched_event.is_set(), "clob.restart was never called"
        with state.lock:
            entered = state.entered_this_round
            slug = state.current_market.get("slug")

        assert entered is False
        assert slug == "btc-updown-5m-new"
        mock_clob.restart.assert_called_once_with("up_new", "dn_new")
