"""Configuration loading: .env, constants, argparse."""
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    # Trading constants
    entry_window_min: int = 15
    entry_window_max: int = 70
    ask_min: int = 80
    ask_max: int = 90
    sl_price_cents: int = 72
    gap_max: float = 3.0
    position_usdc: float = 2.0
    switch_before_end_sec: int = 8

    # Credentials
    poly_api_key: str = ""
    poly_api_secret: str = ""
    poly_api_passphrase: str = ""
    poly_wallet_private_key: str = ""

    # Infrastructure
    proxy_url: str = ""

    # Mode
    dry_run: bool = False

    # Manual market override (optional)
    market_slug_override: str = ""


def load_config(argv: list[str] | None = None) -> Config:
    parser = argparse.ArgumentParser(description="Polymarket BTC Up/Down trading bot")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Simulate orders without sending to CLOB API",
    )
    args, _ = parser.parse_known_args(argv)

    env_dry_run = os.getenv("DRY_RUN", "false").strip().lower() in ("1", "true", "yes")
    dry_run = args.dry_run or env_dry_run

    return Config(
        poly_api_key=os.getenv("POLY_API_KEY", ""),
        poly_api_secret=os.getenv("POLY_API_SECRET", ""),
        poly_api_passphrase=os.getenv("POLY_API_PASSPHRASE", ""),
        poly_wallet_private_key=os.getenv("POLY_WALLET_PRIVATE_KEY", ""),
        proxy_url=os.getenv("PROXY_URL", "http://mh2457652:r8hzakNM2T@82.206.73.210:50100"),
        dry_run=dry_run,
        market_slug_override=os.getenv("MARKET_SLUG_OVERRIDE", ""),
    )
