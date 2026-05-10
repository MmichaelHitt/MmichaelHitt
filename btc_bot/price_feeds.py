"""Binance + OKX WebSocket price feeds with proxy support."""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque

import websocket  # type: ignore[import-untyped]

logger = logging.getLogger("btc_bot")

BINANCE_WSS = "wss://stream.binance.com:9443/ws/btcusdt@trade"
OKX_WSS = "wss://ws.okx.com:8443/ws/v5/public"
OKX_SUBSCRIBE = json.dumps(
    {"op": "subscribe", "args": [{"channel": "trades", "instId": "BTC-USDT"}]}
)


class PriceFeed:
    """Thread-safe BTC price feed from Binance and OKX."""

    def __init__(self, proxy_url: str = "") -> None:
        self._proxy_url = proxy_url
        self._proxy_ok = True
        self._lock = threading.Lock()
        self._binance_prices: deque[tuple[float, float]] = deque(maxlen=500)
        self._okx_prices: deque[tuple[float, float]] = deque(maxlen=500)
        self._running = False

    # ── Public accessors ────────────────────────────────────────────────────

    @property
    def binance_price(self) -> float | None:
        with self._lock:
            return self._binance_prices[-1][1] if self._binance_prices else None

    @property
    def okx_price(self) -> float | None:
        with self._lock:
            return self._okx_prices[-1][1] if self._okx_prices else None

    # ── Internal updaters (called from WS threads) ──────────────────────────

    def _push_binance(self, price: float) -> None:
        with self._lock:
            self._binance_prices.append((time.time(), price))

    def _push_okx(self, price: float) -> None:
        with self._lock:
            self._okx_prices.append((time.time(), price))

    # ── Proxy helpers ────────────────────────────────────────────────────────

    def _proxy_kwargs(self) -> dict:
        """Return run_forever proxy kwargs; empty dict if proxy disabled/failed."""
        if not self._proxy_ok or not self._proxy_url:
            return {}
        host, port, auth = _parse_proxy(self._proxy_url)
        if host is None:
            return {}
        return {"http_proxy_host": host, "http_proxy_port": port, "http_proxy_auth": auth}

    def _check_proxy_error(self, error) -> None:
        msg = str(error).lower()
        if "proxy" in msg or "only http" in msg or "socks" in msg:
            self._proxy_ok = False

    # ── Binance WS ──────────────────────────────────────────────────────────

    def _binance_on_message(self, ws, message: str) -> None:
        try:
            data = json.loads(message)
            self._push_binance(float(data["p"]))
        except Exception:
            pass

    def _binance_on_error(self, ws, error) -> None:
        self._check_proxy_error(error)
        logger.error("[Binance] Error: %s", error)

    def _binance_on_close(self, ws, *args) -> None:
        logger.info("[Binance] Connection closed, reconnecting...")

    def start_binance(self) -> None:
        def run() -> None:
            while self._running:
                try:
                    ws = websocket.WebSocketApp(
                        BINANCE_WSS,
                        on_message=self._binance_on_message,
                        on_error=self._binance_on_error,
                        on_close=self._binance_on_close,
                    )
                    ws.run_forever(**self._proxy_kwargs())
                except Exception as e:
                    logger.error("[Binance] Connection error: %s", e)
                if self._running:
                    time.sleep(2)

        threading.Thread(target=run, daemon=True, name="BinanceFeed").start()

    # ── OKX WS ──────────────────────────────────────────────────────────────

    def _okx_on_open(self, ws) -> None:
        logger.info("[OKX WSS] Connected")
        ws.send(OKX_SUBSCRIBE)

    def _okx_on_message(self, ws, message: str) -> None:
        try:
            data = json.loads(message)
            if data.get("arg", {}).get("channel") == "trades":
                for trade in data.get("data", []):
                    self._push_okx(float(trade["px"]))
        except Exception:
            pass

    def _okx_on_error(self, ws, error) -> None:
        self._check_proxy_error(error)
        logger.error("[OKX] Error: %s", error)

    def _okx_on_close(self, ws, *args) -> None:
        logger.info("[OKX] Connection closed, reconnecting...")

    def start_okx(self) -> None:
        def run() -> None:
            while self._running:
                try:
                    ws = websocket.WebSocketApp(
                        OKX_WSS,
                        on_open=self._okx_on_open,
                        on_message=self._okx_on_message,
                        on_error=self._okx_on_error,
                        on_close=self._okx_on_close,
                    )
                    ws.run_forever(ping_interval=20, **self._proxy_kwargs())
                except Exception as e:
                    logger.error("[OKX] Connection error: %s", e)
                if self._running:
                    time.sleep(2)

        threading.Thread(target=run, daemon=True, name="OKXFeed").start()

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        self.start_binance()
        self.start_okx()

    def stop(self) -> None:
        self._running = False


# ── Helper ────────────────────────────────────────────────────────────────────

def _parse_proxy(proxy_url: str) -> tuple[str | None, int | None, tuple[str, str] | None]:
    """
    Parse http://user:pass@host:port → (host, port, (user, pass)).
    Returns (None, None, None) if proxy_url is empty.
    """
    if not proxy_url:
        return None, None, None
    try:
        from urllib.parse import urlparse

        p = urlparse(proxy_url)
        host = p.hostname
        port = p.port
        auth = (p.username, p.password) if p.username else None
        return host, port, auth
    except Exception:
        return None, None, None
