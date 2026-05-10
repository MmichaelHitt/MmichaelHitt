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
from btc_bot.strategy import choose_side, compute_sl_price

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
        self.entry_price_cents: float = 0.0
        self.sl_triggered: bool = False
        self.position_side: str = ""  # "UP" or "DN"
        self.sl_count: int = 0  # SL triggers this round; re-entry allowed while <= 2


def market_watcher_thread(
    state: BotState,
    clob: ClobStream,
    cfg: Config,
    order_mgr: OrderManager,
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
                old_sl_id = state.sl_order_id
            if old_sl_id:
                order_mgr.cancel_order(old_sl_id)
            with state.lock:
                state.current_market.clear()
                state.current_market.update(next_market)
                state.entered_this_round = False
                state.sl_order_id = None
                state.buy_order_id = None
                state.position_size = 0.0
                state.entry_price_cents = 0.0
                state.sl_triggered = False
                state.position_side = ""
                state.sl_count = 0

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
            sl_triggered = state.sl_triggered
            entry_px = state.entry_price_cents
            pos_size = state.position_size
            pos_side = state.position_side
            sl_count = state.sl_count

        if not market:
            continue

        secs = seconds_to_close(market.get("end_date", ""))
        if secs is None:
            continue

        up_ask = clob.up_ask
        dn_ask = clob.dn_ask
        b_px = price_feed.binance_price
        o_px = price_feed.okx_price

        # ── Status line ────────────────────────────────────────────────────
        slug_short = market.get("slug", "")[-12:]
        now_str = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        b_str = f"${b_px:,.2f}" if b_px else "—"
        o_str = f"${o_px:,.2f}" if o_px else "—"
        up_str = f"{up_ask:.1f}¢" if up_ask is not None else "—"
        dn_str = f"{dn_ask:.1f}¢" if dn_ask is not None else "—"
        logger.info(
            "[%s] [%s] Binance=%s OKX=%s | UP=%s DN=%s | %ss left | entered=%s%s",
            now_str, slug_short, b_str, o_str, up_str, dn_str, secs, entered,
            f"({pos_side})" if entered and pos_side else "",
        )

        # ── SL monitoring ────────────────────────────────────────────────────
        if entered and not sl_triggered:
            position_ask = up_ask if pos_side == "UP" else dn_ask
            if position_ask is not None and position_ask <= cfg.sl_price_cents:
                allow_reentry = False
                with state.lock:
                    if not state.sl_triggered:
                        state.sl_triggered = True
                        state.sl_count += 1
                        if state.sl_count <= 2:
                            allow_reentry = True
                            state.entered_this_round = False
                            state.position_side = ""

                gapped = position_ask < cfg.sl_price_cents
                if gapped:
                    # Price gapped below SL limit: cancel the limit order and
                    # close the position immediately with a market sell.
                    with state.lock:
                        old_sl_id = state.sl_order_id
                        state.sl_order_id = None
                    if old_sl_id:
                        order_mgr.cancel_order(old_sl_id)
                    position_token = market.get(
                        "up_token" if pos_side == "UP" else "dn_token", ""
                    )
                    if position_token:
                        sell_id = order_mgr.place_sell_market(position_token, pos_size)
                        if sell_id is None:
                            logger.error(
                                "[BOT] Market sell FAILED after gap — position may be open!"
                            )
                        else:
                            logger.info("[BOT] Gap-close market sell placed: id=%s", sell_id)

                pnl = (position_ask - entry_px) * pos_size / 100
                logger.warning(
                    "[BOT] SL triggered! %s ask=%.1f¢ | entry=%.1f¢ | pnl≈%.4f USDC%s%s",
                    pos_side, position_ask, entry_px, pnl,
                    " | GAPPED → market sell" if gapped else f" | limit @ {cfg.sl_price_cents:.0f}¢",
                    " | re-entry allowed" if allow_reentry else " | no more re-entries",
                )
                log_trade(market.get("slug", ""), f"SL_{pos_side}", position_ask, pnl=pnl)

        # ── Entry logic ────────────────────────────────────────────────────
        if b_px is None or o_px is None:
            continue

        side = choose_side(
            secs, up_ask, dn_ask, b_px, o_px, entered,
            entry_min=cfg.entry_window_min,
            entry_max=cfg.entry_window_max,
            ask_min=cfg.ask_min,
            ask_max=cfg.ask_max,
            gap_max=cfg.gap_max,
        )
        if side is None:
            continue

        entry_ask = up_ask if side == "UP" else dn_ask
        entry_token = market.get("up_token" if side == "UP" else "dn_token", "")

        # Cancel previous SL order if re-entering after stop-loss
        with state.lock:
            old_sl_id = state.sl_order_id
        if old_sl_id:
            order_mgr.cancel_order(old_sl_id)
            with state.lock:
                state.sl_order_id = None

        # Place buy
        buy_id = order_mgr.place_buy_market(entry_token, cfg.position_usdc)
        if buy_id is None:
            logger.warning("[BOT] Buy order failed, skipping round")
            continue

        # sl_size = tokens actually purchased (position_usdc / entry_price_usdc)
        sl_price = compute_sl_price(cfg.sl_price_cents)
        sl_size = cfg.position_usdc / (entry_ask / 100)

        # Place SL
        sl_id = order_mgr.place_sl_limit(entry_token, sl_price, sl_size)
        if sl_id is None:
            logger.error(
                "[BOT] SL placement FAILED — %s position is UNPROTECTED! size=%.4f entry=%.2f¢",
                side, sl_size, entry_ask,
            )

        with state.lock:
            state.entered_this_round = True
            state.buy_order_id = buy_id
            state.sl_order_id = sl_id
            state.position_size = sl_size
            state.entry_price_cents = entry_ask
            state.sl_triggered = False
            state.position_side = side
            # sl_count intentionally NOT reset here — limits re-entries per round

        logger.info(
            "[BOT] Entered %s: buy_id=%s sl_id=%s price=%.2f size=%.4f",
            side, buy_id, sl_id, entry_ask, sl_size,
        )
        log_trade(market.get("slug", ""), f"BUY_{side}", entry_ask)


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
        args=(state, clob, cfg, order_mgr, logger),
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
