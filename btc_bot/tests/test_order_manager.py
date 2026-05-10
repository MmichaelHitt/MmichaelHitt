"""Tests for order_manager.py — Block 6."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from btc_bot.order_manager import OrderManager

TOKEN_ID = "token_up_abc123"


def make_dry_run_manager() -> OrderManager:
    return OrderManager(
        api_key="key",
        api_secret="secret",
        passphrase="pass",
        private_key="0xdeadbeef",
        dry_run=True,
    )


class TestDryRunPlaceBuyMarket:
    def test_returns_fake_order_id(self):
        mgr = make_dry_run_manager()
        result = mgr.place_buy_market(TOKEN_ID, 2.0)
        assert result is not None
        assert result.startswith("dry-buy-")

    def test_does_not_call_clob_client(self):
        mgr = make_dry_run_manager()
        assert mgr._client is None
        mgr.place_buy_market(TOKEN_ID, 2.0)

    def test_returns_unique_ids(self):
        mgr = make_dry_run_manager()
        ids = {mgr.place_buy_market(TOKEN_ID, 2.0) for _ in range(10)}
        assert len(ids) == 10


class TestDryRunPlaceSlLimit:
    def test_returns_fake_order_id(self):
        mgr = make_dry_run_manager()
        result = mgr.place_sl_limit(TOKEN_ID, 0.72, 2.777)
        assert result is not None
        assert result.startswith("dry-sl-")

    def test_does_not_call_clob_client(self):
        mgr = make_dry_run_manager()
        assert mgr._client is None
        mgr.place_sl_limit(TOKEN_ID, 0.72, 2.777)

    def test_returns_unique_ids(self):
        mgr = make_dry_run_manager()
        ids = {mgr.place_sl_limit(TOKEN_ID, 0.72, 2.777) for _ in range(10)}
        assert len(ids) == 10


class TestDryRunCancelOrder:
    def test_returns_true(self):
        mgr = make_dry_run_manager()
        assert mgr.cancel_order("some-order-id") is True

    def test_works_with_any_id(self):
        mgr = make_dry_run_manager()
        for oid in ["abc", "123", "dry-buy-xyz"]:
            assert mgr.cancel_order(oid) is True


class TestDryRunGetPosition:
    def test_returns_zero(self):
        mgr = make_dry_run_manager()
        assert mgr.get_position(TOKEN_ID) == 0.0

    def test_returns_float(self):
        mgr = make_dry_run_manager()
        assert isinstance(mgr.get_position(TOKEN_ID), float)


class TestLivePlaceBuyMarket:
    def _make_live_manager(self, mock_client) -> OrderManager:
        mgr = OrderManager.__new__(OrderManager)
        mgr._dry_run = False
        mgr._proxy_url = ""
        mgr._client = mock_client
        return mgr

    def test_calls_post_order_and_returns_id(self):
        mock_client = MagicMock()
        mock_client.create_market_order.return_value = MagicMock()
        mock_client.post_order.return_value = {"orderID": "live-order-123"}

        with patch.dict("sys.modules", {
            "py_clob_client.clob_types": MagicMock(MarketOrderArgs=MagicMock()),
            "py_clob_client.order_builder.constants": MagicMock(BUY="BUY"),
        }):
            mgr = self._make_live_manager(mock_client)
            result = mgr.place_buy_market(TOKEN_ID, 2.0)

        assert result == "live-order-123"
        mock_client.post_order.assert_called_once()

    def test_returns_none_on_exception(self):
        mock_client = MagicMock()
        mock_client.create_market_order.side_effect = Exception("network error")

        with patch.dict("sys.modules", {
            "py_clob_client.clob_types": MagicMock(MarketOrderArgs=MagicMock()),
            "py_clob_client.order_builder.constants": MagicMock(BUY="BUY"),
        }):
            mgr = self._make_live_manager(mock_client)
            result = mgr.place_buy_market(TOKEN_ID, 2.0)

        assert result is None


class TestLivePlaceSlLimit:
    def _make_live_manager(self, mock_client) -> OrderManager:
        mgr = OrderManager.__new__(OrderManager)
        mgr._dry_run = False
        mgr._proxy_url = ""
        mgr._client = mock_client
        return mgr

    def test_calls_post_order_and_returns_id(self):
        mock_client = MagicMock()
        mock_client.create_order.return_value = MagicMock()
        mock_client.post_order.return_value = {"orderID": "sl-order-456"}

        with patch.dict("sys.modules", {
            "py_clob_client.clob_types": MagicMock(OrderArgs=MagicMock()),
            "py_clob_client.order_builder.constants": MagicMock(SELL="SELL"),
        }):
            mgr = self._make_live_manager(mock_client)
            result = mgr.place_sl_limit(TOKEN_ID, 0.72, 2.777)

        assert result == "sl-order-456"

    def test_returns_none_on_exception(self):
        mock_client = MagicMock()
        mock_client.create_order.side_effect = Exception("API error")

        with patch.dict("sys.modules", {
            "py_clob_client.clob_types": MagicMock(OrderArgs=MagicMock()),
            "py_clob_client.order_builder.constants": MagicMock(SELL="SELL"),
        }):
            mgr = self._make_live_manager(mock_client)
            result = mgr.place_sl_limit(TOKEN_ID, 0.72, 2.777)

        assert result is None


class TestLiveCancelOrder:
    def _make_live_manager(self, mock_client) -> OrderManager:
        mgr = OrderManager.__new__(OrderManager)
        mgr._dry_run = False
        mgr._proxy_url = ""
        mgr._client = mock_client
        return mgr

    def test_returns_true_when_canceled(self):
        mock_client = MagicMock()
        mock_client.cancel.return_value = {"canceled": True}
        mgr = self._make_live_manager(mock_client)
        assert mgr.cancel_order("order-123") is True

    def test_returns_false_on_exception(self):
        mock_client = MagicMock()
        mock_client.cancel.side_effect = Exception("not found")
        mgr = self._make_live_manager(mock_client)
        assert mgr.cancel_order("order-123") is False


class TestLiveGetPosition:
    def _make_live_manager(self, mock_client) -> OrderManager:
        mgr = OrderManager.__new__(OrderManager)
        mgr._dry_run = False
        mgr._proxy_url = ""
        mgr._client = mock_client
        return mgr

    def test_returns_position_size(self):
        mock_client = MagicMock()
        mock_client.get_positions.return_value = [
            {"token_id": TOKEN_ID, "size": "2.777"},
            {"token_id": "other", "size": "10.0"},
        ]
        mgr = self._make_live_manager(mock_client)
        assert mgr.get_position(TOKEN_ID) == pytest.approx(2.777)

    def test_returns_zero_when_not_found(self):
        mock_client = MagicMock()
        mock_client.get_positions.return_value = []
        mgr = self._make_live_manager(mock_client)
        assert mgr.get_position(TOKEN_ID) == 0.0

    def test_returns_zero_on_exception(self):
        mock_client = MagicMock()
        mock_client.get_positions.side_effect = Exception("error")
        mgr = self._make_live_manager(mock_client)
        assert mgr.get_position(TOKEN_ID) == 0.0
