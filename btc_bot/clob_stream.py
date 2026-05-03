"""Polymarket CLOB WebSocket stream — order book bid/ask."""
from __future__ import annotations

import json
import logging
import threading
import time

import websocket  # type: ignore[import-untyped]

from btc_bot.price_feeds import _parse_proxy

CLOB_WSS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

logger = logging.getLogger("btc_bot")


class ClobStream:
    """Thread-safe CLOB order book stream for one UP/DN market."""

    def __init__(self, up_token: str, dn_token: str, proxy_url: str = "") -> None:
        self._up_token = up_token
        self._dn_token = dn_token
        self._proxy_url = proxy_url
        self._lock = threading.Lock()

        self._up_bid: float | None = None
        self._up_ask: float | None = None
        self._dn_bid: float | None = None
        self._dn_ask: float | None = None

        self._ws: websocket.WebSocketApp | None = None
        self._running = False

    # ── Public accessors ─────────────────────────────────────────────────────

    @property
    def up_ask(self) -> float | None:
        with self._lock:
            return self._up_ask

    @property
    def up_bid(self) -> float | None:
        with self._lock:
            return self._up_bid

    @property
    def dn_ask(self) -> float | None:
        with self._lock:
            return self._dn_ask

    @property
    def dn_bid(self) -> float | None:
        with self._lock:
            return self._dn_bid

    # ── WS callbacks ─────────────────────────────────────────────────────────

    def _subscribe_msg(self) -> str:
        return json.dumps({
            "assets_ids": [self._up_token, self._dn_token],
            "type": "market",
        })

    def _on_open(self, ws) -> None:
        logger.info("[CLOB WSS] Connected")
        ws.send(self._subscribe_msg())

    def _on_message(self, ws, message: str) -> None:
        try:
            data = json.loads(message)
            etype = data.get("event_type")
            token_map = {self._up_token: "up", self._dn_token: "dn"}

            if etype == "book":
                self._handle_book(data, token_map)
            elif etype == "best_bid_ask":
                self._handle_best_bid_ask(data, token_map)
            elif etype == "price_change":
                self._handle_price_change(data, token_map)
        except Exception:
            pass

    def _handle_book(self, data: dict, token_map: dict[str, str]) -> None:
        side = token_map.get(data.get("asset_id", ""))
        if not side:
            return
        bids = [
            float(b["price"])
            for b in data.get("bids", [])
            if 0.001 < float(b["price"]) < 0.999
        ]
        asks = [
            float(a["price"])
            for a in data.get("asks", [])
            if 0.001 < float(a["price"]) < 0.999
        ]
        with self._lock:
            if side == "up":
                self._up_bid = round(max(bids) * 100, 1) if bids else None
                self._up_ask = round(min(asks) * 100, 1) if asks else None
            else:
                self._dn_bid = round(max(bids) * 100, 1) if bids else None
                self._dn_ask = round(min(asks) * 100, 1) if asks else None

    def _handle_best_bid_ask(self, data: dict, token_map: dict[str, str]) -> None:
        side = token_map.get(data.get("asset_id", ""))
        if not side:
            return
        bb = data.get("best_bid")
        ba = data.get("best_ask")
        with self._lock:
            if side == "up":
                if bb:
                    self._up_bid = round(float(bb) * 100, 1)
                if ba:
                    self._up_ask = round(float(ba) * 100, 1)
            else:
                if bb:
                    self._dn_bid = round(float(bb) * 100, 1)
                if ba:
                    self._dn_ask = round(float(ba) * 100, 1)

    def _handle_price_change(self, data: dict, token_map: dict[str, str]) -> None:
        for pc in data.get("price_changes", []):
            side = token_map.get(pc.get("asset_id", ""))
            if not side:
                continue
            bb = pc.get("best_bid")
            ba = pc.get("best_ask")
            with self._lock:
                if side == "up":
                    if bb:
                        self._up_bid = round(float(bb) * 100, 1)
                    if ba:
                        self._up_ask = round(float(ba) * 100, 1)
                else:
                    if bb:
                        self._dn_bid = round(float(bb) * 100, 1)
                    if ba:
                        self._dn_ask = round(float(ba) * 100, 1)

    def _on_error(self, ws, error) -> None:
        logger.error("[CLOB WSS] Error: %s", error)

    def _on_close(self, ws, *args) -> None:
        logger.info("[CLOB WSS] Connection closed")

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        self._run_thread()

    def close(self) -> None:
        self._running = False
        with self._lock:
            ws = self._ws
        if ws:
            try:
                ws.close()
            except Exception:
                pass

    def restart(self, new_up: str, new_dn: str) -> None:
        """Close current connection and start fresh with new tokens."""
        self.close()
        time.sleep(0.3)
        with self._lock:
            self._up_token = new_up
            self._dn_token = new_dn
            self._up_bid = self._up_ask = None
            self._dn_bid = self._dn_ask = None
        self._running = True
        self._run_thread()

    def _run_thread(self) -> None:
        def run() -> None:
            while self._running:
                try:
                    proxy_host, proxy_port, proxy_auth = _parse_proxy(self._proxy_url)
                    ws = websocket.WebSocketApp(
                        CLOB_WSS,
                        on_open=self._on_open,
                        on_message=self._on_message,
                        on_error=self._on_error,
                        on_close=self._on_close,
                    )
                    with self._lock:
                        self._ws = ws
                    ws.run_forever(
                        http_proxy_host=proxy_host,
                        http_proxy_port=proxy_port,
                        http_proxy_auth=proxy_auth,
                    )
                except Exception as e:
                    logger.error("[CLOB WSS] Connection error: %s", e)
                if self._running:
                    time.sleep(2)

        threading.Thread(target=run, daemon=True, name="ClobStream").start()
