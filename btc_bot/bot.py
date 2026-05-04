"""Main asyncio entry point for the Polymarket BTC Up/Down trading bot."""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure the project root (parent of btc_bot/) is on sys.path when running
# bot.py directly (e.g. from inside btc_bot/ in PyCharm or CLI).
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import asyncio
import csv
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from btc_bot.clob_stream import ClobStream
from btc_bot.config import Config, load_config
from btc_bot.logger_setup import setup_logger
from btc_bot.market_finder import find_latest_active_market, find_next_market
from btc_bot.order_manager import OrderManager
from btc_bot.price_feeds import PriceFeed
from btc_bot.strategy import compute_sl_price, should_enter

TRADES_CSV = Path(__file__).parent / "trades.csv"
POLL_INTERVAL = 0.5


def seconds_to_close(end_date_iso: str) -> int | None:
    try:
        end = datetime.fromisoformat(end_date_iso.replace("Z", "+00:00"))
        return max(0, int((end - datetime.now(timezone.utc)).total_seconds()))
    except Exception:
        return None


def log_trade(slug: str, side: str, price: float, pnl: float | None = None) -> None:
    first_write = not TRADES_CSV.exists()
    with TRADES_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if first_write:
            writer.writerow(["date", "slug", "side", "price", "pnl"])
        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            slug,
            side,
            price,
            pnl if pnl is not None else "",
        ])


class BotState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.current_market: dict = {}
        self.entered_this_round: bool = False
        self.sl_order_id: str | None = None
        self.buy_order_id: str | None = None
        self.position_size: float = 0.0


def market_watcher_thread(
    state: BotState,
    clob: ClobStream,
    cfg: Config,
    logger,
) -> None:
    switched = False

    while True:
        time.sleep(1)
        with state.lock:
            end_date = state.current_market.get("end_date", "")

        if not end_date:
            continue

        secs = seconds_to_close(end_date)
        if secs is None:
            continue

        if secs <= cfg.switch_before_end_sec and not switched:
            switched = True
            logger.info("[AutoSwitch] %ds to end — searching next round...", secs)

            next_market = find_next_market(end_date, proxy_url=cfg.proxy_url)
            if next_market is None:
                logger.warning("[AutoSwitch] Next round not found, will retry...")
                def _reset() -> None:
                    nonlocal switched
                    time.sleep(30)
                    switched = False
                threading.Thread(target=_reset, daemon=True).start()
                continue

            logger.info("[AutoSwitch] Switching to: %s", next_market["slug"])
            with state.lock:
                state.current_market.update(next_market)
                state.entered_this_round = False
                state.sl_order_id = None
                state.buy_order_id = None
                state.position_size = 0.0

            clob.restart(next_market["up_token"], next_market["dn_token"])

        elif secs > cfg.switch_before_end_sec:
            switched = False


async def trading_loop(
    state: BotState,
    price_feed: PriceFeed,
    clob: ClobStream,
    order_mgr: OrderManager,
    cfg: Config,
    logger,
) -> None:
    while True:
        await asyncio.sleep(POLL_INTERVAL)

        with state.lock:
            market = dict(state.current_market)
            entered = state.entered_this_round

        if not market:
            continue

        secs = seconds_to_close(market.get("end_date", ""))
        if secs is None:
            continue

        up_ask = clob.up_ask
        b_px = price_feed.binance_price
        o_px = price_feed.okx_price

        # ── Status line ────────────────────────────────────────────────────
        slug_short = market.get("slug", "")[-12:]
        now_str = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        b_str = f"${b_px:,.2f}" if b_px else "—"
        o_str = f"${o_px:,.2f}" if o_px else "—"
        ask_str = f"{up_ask:.1f}¢" if up_ask is not None else "—"
        logger.info(
            "[%s] [%s] Binance=%s OKX=%s | UP ask=%s | %ss left | entered=%s",
            now_str, slug_short, b_str, o_str, ask_str, secs, entered,
        )

        # ── Entry logic ────────────────────────────────────────────────────
        if up_ask is None or b_px is None or o_px is None:
            continue

        if not should_enter(secs, up_ask, b_px, o_px, entered):
            continue

        # Place buy
        up_token = market.get("up_token", "")
        buy_id = order_mgr.place_buy_market(up_token, cfg.position_usdc)
        if buy_id is None:
            logger.warning("[BOT] Buy order failed, skipping round")
            continue

        # Compute SL size: USDC / SL_price
        sl_price = compute_sl_price()
        sl_size = cfg.position_usdc / sl_price

        # Place SL
        sl_id = order_mgr.place_sl_limit(up_token, sl_price, sl_size)

        with state.lock:
            state.entered_this_round = True
            state.buy_order_id = buy_id
            state.sl_order_id = sl_id
            state.position_size = sl_size

        logger.info(
            "[BOT] Entered: buy_id=%s sl_id=%s price=%.2f size=%.4f",
            buy_id, sl_id, up_ask, sl_size,
        )
        log_trade(market.get("slug", ""), "BUY", up_ask)


async def main_async(cfg: Config, logger) -> None:
    logger.info("=" * 60)
    logger.info("  Polymarket BTC Up/Down Trading Bot")
    logger.info("  dry_run=%s", cfg.dry_run)
    logger.info("=" * 60)

    logger.info("Searching for active round...")
    market = find_latest_active_market(proxy_url=cfg.proxy_url, slug_override=cfg.market_slug_override)
    logger.info("Active round: %s  end=%s", market["slug"], market["end_date"])

    state = BotState()
    with state.lock:
        state.current_market.update(market)

    price_feed = PriceFeed(proxy_url=cfg.proxy_url)
    clob = ClobStream(market["up_token"], market["dn_token"], proxy_url=cfg.proxy_url)
    order_mgr = OrderManager(
        api_key=cfg.poly_api_key,
        api_secret=cfg.poly_api_secret,
        passphrase=cfg.poly_api_passphrase,
        private_key=cfg.poly_wallet_private_key,
        proxy_url=cfg.proxy_url,
        dry_run=cfg.dry_run,
    )

    price_feed.start()
    clob.start()

    threading.Thread(
        target=market_watcher_thread,
        args=(state, clob, cfg, logger),
        daemon=True,
        name="MarketWatcher",
    ).start()

    logger.info("Waiting 3s for initial data...")
    await asyncio.sleep(3)
    logger.info("Starting trading loop!")

    await trading_loop(state, price_feed, clob, order_mgr, cfg, logger)


def main() -> None:
    cfg = load_config()
    logger = setup_logger()
    asyncio.run(main_async(cfg, logger))


if __name__ == "__main__":
    main()
