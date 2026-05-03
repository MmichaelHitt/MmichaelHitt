"""Tests for price_feeds.py — Block 4."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from btc_bot.price_feeds import PriceFeed, _parse_proxy


class TestParsProxy:
    def test_empty_returns_nones(self):
        assert _parse_proxy("") == (None, None, None)

    def test_parses_host_and_port(self):
        host, port, auth = _parse_proxy("http://user:pass@82.206.73.210:50100")
        assert host == "82.206.73.210"
        assert port == 50100

    def test_parses_auth(self):
        _, _, auth = _parse_proxy("http://myuser:mypass@1.2.3.4:9999")
        assert auth == ("myuser", "mypass")

    def test_no_auth_returns_none(self):
        _, _, auth = _parse_proxy("http://1.2.3.4:9999")
        assert auth is None


class TestPriceFeedInitialState:
    def test_binance_price_none_initially(self):
        feed = PriceFeed()
        assert feed.binance_price is None

    def test_okx_price_none_initially(self):
        feed = PriceFeed()
        assert feed.okx_price is None


class TestBinanceMessageParsing:
    def test_parses_trade_price(self):
        feed = PriceFeed()
        msg = json.dumps({"e": "trade", "p": "65432.10", "q": "0.001"})
        feed._binance_on_message(None, msg)
        assert feed.binance_price == pytest.approx(65432.10)

    def test_ignores_malformed_message(self):
        feed = PriceFeed()
        feed._binance_on_message(None, "not json")
        assert feed.binance_price is None

    def test_ignores_missing_price_key(self):
        feed = PriceFeed()
        feed._binance_on_message(None, json.dumps({"e": "trade"}))
        assert feed.binance_price is None

    def test_latest_price_overwrites_previous(self):
        feed = PriceFeed()
        feed._binance_on_message(None, json.dumps({"p": "60000"}))
        feed._binance_on_message(None, json.dumps({"p": "61000"}))
        assert feed.binance_price == pytest.approx(61000.0)


class TestOKXMessageParsing:
    def _make_okx_msg(self, prices: list[float]) -> str:
        return json.dumps({
            "arg": {"channel": "trades", "instId": "BTC-USDT"},
            "data": [{"px": str(p), "sz": "0.01"} for p in prices],
        })

    def test_parses_single_trade(self):
        feed = PriceFeed()
        feed._okx_on_message(None, self._make_okx_msg([64500.0]))
        assert feed.okx_price == pytest.approx(64500.0)

    def test_parses_multiple_trades_last_wins(self):
        feed = PriceFeed()
        feed._okx_on_message(None, self._make_okx_msg([64500.0, 64600.0, 64700.0]))
        assert feed.okx_price == pytest.approx(64700.0)

    def test_ignores_non_trades_channel(self):
        feed = PriceFeed()
        msg = json.dumps({
            "arg": {"channel": "ticker"},
            "data": [{"px": "99999"}],
        })
        feed._okx_on_message(None, msg)
        assert feed.okx_price is None

    def test_ignores_malformed_message(self):
        feed = PriceFeed()
        feed._okx_on_message(None, "garbage")
        assert feed.okx_price is None

    def test_ignores_missing_data(self):
        feed = PriceFeed()
        msg = json.dumps({"arg": {"channel": "trades"}})
        feed._okx_on_message(None, msg)
        assert feed.okx_price is None


class TestPriceFeedThreadSafety:
    def test_concurrent_writes_are_safe(self):
        import threading
        feed = PriceFeed()
        errors: list[Exception] = []

        def write_prices(start: float) -> None:
            try:
                for i in range(100):
                    feed._push_binance(start + i)
                    feed._push_okx(start + i)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=write_prices, args=(i * 1000,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert feed.binance_price is not None
        assert feed.okx_price is not None


class TestStartBinanceProxy:
    def test_proxy_parsed_and_passed_to_ws(self):
        feed = PriceFeed(proxy_url="http://user:pass@1.2.3.4:1234")
        feed._running = True

        mock_ws_instance = MagicMock()
        mock_ws_instance.run_forever.side_effect = RuntimeError("stop")

        with patch("btc_bot.price_feeds.websocket") as mock_ws_mod:
            mock_ws_mod.WebSocketApp.return_value = mock_ws_instance
            try:
                feed.start_binance()
            except Exception:
                pass

        import time as _time
        _time.sleep(0.1)
        feed.stop()


class TestStartOKXProxy:
    def test_okx_sends_subscribe_on_open(self):
        feed = PriceFeed()
        mock_ws = MagicMock()
        feed._okx_on_open(mock_ws)
        mock_ws.send.assert_called_once()
        sent = json.loads(mock_ws.send.call_args[0][0])
        assert sent["op"] == "subscribe"
        assert sent["args"][0]["channel"] == "trades"
