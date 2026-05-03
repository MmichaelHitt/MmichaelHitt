"""Order placement/cancellation via py_clob_client with proxy and dry-run support."""
from __future__ import annotations

import logging
import uuid
from typing import Any

logger = logging.getLogger("btc_bot")

_DRY_RUN_PREFIX = "[DRY-RUN]"


class OrderManager:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str,
        private_key: str,
        proxy_url: str = "",
        dry_run: bool = False,
    ) -> None:
        self._dry_run = dry_run
        self._proxy_url = proxy_url
        self._client: Any | None = None

        if not dry_run:
            self._client = self._build_client(api_key, api_secret, passphrase, private_key)

    # ── Client factory ───────────────────────────────────────────────────────

    def _build_client(
        self, api_key: str, api_secret: str, passphrase: str, private_key: str
    ) -> Any:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        host = "https://clob.polymarket.com"
        chain_id = 137  # Polygon mainnet

        creds = ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=passphrase,
        )
        client = ClobClient(host, key=private_key, chain_id=chain_id, creds=creds)
        return client

    # ── Public API ───────────────────────────────────────────────────────────

    def place_buy_market(self, token_id: str, usdc_amount: float) -> str | None:
        """
        Place a market buy order for token_id spending up to usdc_amount USDC.
        Returns order_id or None on failure.
        """
        if self._dry_run:
            fake_id = f"dry-buy-{uuid.uuid4().hex[:8]}"
            logger.info(
                "%s place_buy_market token=%s amount=%.2f → fake_id=%s",
                _DRY_RUN_PREFIX, token_id, usdc_amount, fake_id,
            )
            return fake_id

        try:
            from py_clob_client.clob_types import MarketOrderArgs
            from py_clob_client.order_builder.constants import BUY

            assert self._client is not None
            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=usdc_amount,
            )
            signed_order = self._client.create_market_order(order_args)
            resp = self._client.post_order(signed_order)
            order_id: str = resp.get("orderID") or resp.get("order_id", "")
            logger.info("[ORDER] Buy market placed: token=%s amount=%.2f id=%s",
                        token_id, usdc_amount, order_id)
            return order_id or None
        except Exception as e:
            logger.error("[ORDER] place_buy_market failed: %s", e)
            return None

    def place_sl_limit(self, token_id: str, price: float, size: float) -> str | None:
        """
        Place a limit sell order (stop-loss) at price for size units.
        Returns order_id or None on failure.
        """
        if self._dry_run:
            fake_id = f"dry-sl-{uuid.uuid4().hex[:8]}"
            logger.info(
                "%s place_sl_limit token=%s price=%.4f size=%.4f → fake_id=%s",
                _DRY_RUN_PREFIX, token_id, price, size, fake_id,
            )
            return fake_id

        try:
            from py_clob_client.clob_types import OrderArgs
            from py_clob_client.order_builder.constants import SELL

            assert self._client is not None
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=SELL,
            )
            signed_order = self._client.create_order(order_args)
            resp = self._client.post_order(signed_order)
            order_id: str = resp.get("orderID") or resp.get("order_id", "")
            logger.info("[ORDER] SL limit placed: token=%s price=%.4f size=%.4f id=%s",
                        token_id, price, size, order_id)
            return order_id or None
        except Exception as e:
            logger.error("[ORDER] place_sl_limit failed: %s", e)
            return None

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order by order_id. Returns True on success."""
        if self._dry_run:
            logger.info("%s cancel_order id=%s", _DRY_RUN_PREFIX, order_id)
            return True

        try:
            assert self._client is not None
            resp = self._client.cancel(order_id)
            success: bool = bool(resp.get("canceled")) or resp.get("status") == "canceled"
            logger.info("[ORDER] cancel_order id=%s success=%s", order_id, success)
            return success
        except Exception as e:
            logger.error("[ORDER] cancel_order failed: %s", e)
            return False

    def get_position(self, token_id: str) -> float:
        """Return current position size for token_id (0.0 if unknown)."""
        if self._dry_run:
            logger.info("%s get_position token=%s → 0.0 (simulated)", _DRY_RUN_PREFIX, token_id)
            return 0.0

        try:
            assert self._client is not None
            resp = self._client.get_positions()
            for pos in resp:
                if pos.get("asset") == token_id or pos.get("token_id") == token_id:
                    return float(pos.get("size", 0.0))
            return 0.0
        except Exception as e:
            logger.error("[ORDER] get_position failed: %s", e)
            return 0.0
