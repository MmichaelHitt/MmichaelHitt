"""Tests for clob_stream.py — Block 5."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from btc_bot.clob_stream import ClobStream

UP_TOKEN = "up_token_abc123"
DN_TOKEN = "dn_token_xyz789"


def make_stream() -> ClobStream:
    return ClobStream(UP_TOKEN, DN_TOKEN)


class TestInitialState:
    def test_all_none_initially(self):
        s = make_stream()
        assert s.up_ask is None
        assert s.up_bid is None
        assert s.dn_ask is None
        assert s.dn_bid is None


class TestSubscribeMsg:
    def test_subscribe_msg_contains_tokens(self):
        s = make_stream()
        msg = json.loads(s._subscribe_msg())
        assert UP_TOKEN in msg["assets_ids"]
        assert DN_TOKEN in msg["assets_ids"]
        assert msg["type"] == "market"


class TestOnOpen:
    def test_sends_subscribe_on_open(self):
        s = make_stream()
        mock_ws = MagicMock()
        s._on_open(mock_ws)
        mock_ws.send.assert_called_once()
        sent = json.loads(mock_ws.send.call_args[0][0])
        assert sent["type"] == "market"


class TestHandleBook:
    def _make_book_msg(self, token: str, bids: list[float], asks: list[float]) -> dict:
        return {
            "event_type": "book",
            "asset_id": token,
            "bids": [{"price": str(p)} for p in bids],
            "asks": [{"price": str(p)} for p in asks],
        }

    def test_parses_up_book(self):
        s = make_stream()
        msg = json.dumps(self._make_book_msg(UP_TOKEN, [0.82, 0.81], [0.85, 0.86]))
        s._on_message(None, msg)
        assert s.up_bid == pytest.approx(82.0)
        assert s.up_ask == pytest.approx(85.0)

    def test_parses_dn_book(self):
        s = make_stream()
        msg = json.dumps(self._make_book_msg(DN_TOKEN, [0.10], [0.15]))
        s._on_message(None, msg)
        assert s.dn_bid == pytest.approx(10.0)
        assert s.dn_ask == pytest.approx(15.0)

    def test_ignores_unknown_token(self):
        s = make_stream()
        msg = json.dumps(self._make_book_msg("unknown_token", [0.5], [0.55]))
        s._on_message(None, msg)
        assert s.up_ask is None
        assert s.dn_ask is None

    def test_filters_near_zero_prices(self):
        s = make_stream()
        msg = json.dumps(self._make_book_msg(
            UP_TOKEN,
            [0.82, 0.0001],
            [0.85, 0.9999],
        ))
        s._on_message(None, msg)
        assert s.up_bid == pytest.approx(82.0)
        assert s.up_ask == pytest.approx(85.0)

    def test_empty_bids_sets_none(self):
        s = make_stream()
        msg = json.dumps(self._make_book_msg(UP_TOKEN, [], [0.85]))
        s._on_message(None, msg)
        assert s.up_bid is None
        assert s.up_ask == pytest.approx(85.0)


class TestHandleBestBidAsk:
    def _make_bba_msg(self, token: str, best_bid: str | None, best_ask: str | None) -> dict:
        d: dict = {"event_type": "best_bid_ask", "asset_id": token}
        if best_bid is not None:
            d["best_bid"] = best_bid
        if best_ask is not None:
            d["best_ask"] = best_ask
        return d

    def test_updates_up_bid_ask(self):
        s = make_stream()
        msg = json.dumps(self._make_bba_msg(UP_TOKEN, "0.83", "0.86"))
        s._on_message(None, msg)
        assert s.up_bid == pytest.approx(83.0)
        assert s.up_ask == pytest.approx(86.0)

    def test_updates_dn_bid_ask(self):
        s = make_stream()
        msg = json.dumps(self._make_bba_msg(DN_TOKEN, "0.12", "0.14"))
        s._on_message(None, msg)
        assert s.dn_bid == pytest.approx(12.0)
        assert s.dn_ask == pytest.approx(14.0)

    def test_partial_update_preserves_other(self):
        s = make_stream()
        # Set initial bid
        s._on_message(None, json.dumps(self._make_bba_msg(UP_TOKEN, "0.80", "0.85")))
        # Update only ask
        s._on_message(None, json.dumps(self._make_bba_msg(UP_TOKEN, None, "0.87")))
        assert s.up_bid == pytest.approx(80.0)
        assert s.up_ask == pytest.approx(87.0)

    def test_ignores_unknown_token(self):
        s = make_stream()
        msg = json.dumps(self._make_bba_msg("rando", "0.5", "0.55"))
        s._on_message(None, msg)
        assert s.up_ask is None


class TestHandlePriceChange:
    def _make_pc_msg(self, changes: list[dict]) -> dict:
        return {"event_type": "price_change", "price_changes": changes}

    def test_updates_up_ask(self):
        s = make_stream()
        msg = json.dumps(self._make_pc_msg([
            {"asset_id": UP_TOKEN, "best_bid": "0.81", "best_ask": "0.84"},
        ]))
        s._on_message(None, msg)
        assert s.up_ask == pytest.approx(84.0)
        assert s.up_bid == pytest.approx(81.0)

    def test_multiple_changes_in_one_message(self):
        s = make_stream()
        msg = json.dumps(self._make_pc_msg([
            {"asset_id": UP_TOKEN, "best_ask": "0.84"},
            {"asset_id": DN_TOKEN, "best_ask": "0.16"},
        ]))
        s._on_message(None, msg)
        assert s.up_ask == pytest.approx(84.0)
        assert s.dn_ask == pytest.approx(16.0)

    def test_ignores_unknown_token_in_price_change(self):
        s = make_stream()
        msg = json.dumps(self._make_pc_msg([
            {"asset_id": "unknown", "best_ask": "0.50"},
        ]))
        s._on_message(None, msg)
        assert s.up_ask is None


class TestMalformedMessages:
    def test_ignores_non_json(self):
        s = make_stream()
        s._on_message(None, "not valid json")
        assert s.up_ask is None

    def test_ignores_unknown_event_type(self):
        s = make_stream()
        s._on_message(None, json.dumps({"event_type": "mystery"}))
        assert s.up_ask is None


class TestRestart:
    def test_clears_book_on_restart(self):
        s = make_stream()
        # Manually set some values
        s._up_ask = 85.0
        s._dn_ask = 15.0

        new_up = "new_up_token"
        new_dn = "new_dn_token"

        s._running = False

        import btc_bot.clob_stream as cs_module
        from unittest.mock import patch

        with patch.object(s, "_run_thread"):
            s.restart(new_up, new_dn)

        assert s._up_token == new_up
        assert s._dn_token == new_dn
        assert s.up_ask is None
        assert s.dn_ask is None
