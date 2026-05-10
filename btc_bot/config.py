"""Configuration loading: .env, constants, argparse."""
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Always load .env from the same directory as this file,
# regardless of the working directory the user launches from.
load_dotenv(Path(__file__).parent / ".env")


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
    poly_funder_address: str = ""

    # Infrastructure
    proxy_url: str = ""

    # Mode
    dry_run: bool = False

    # Manual market override (optional)
    market_slug_override: str = ""


def _int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, ""))
    except (ValueError, TypeError):
        return default


def _float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, ""))
    except (ValueError, TypeError):
        return default


def load_config(argv: list[str] | None = None) -> Config:
    parser = argparse.ArgumentParser(description="Polymarket BTC Up/Down trading bot")
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--position-usdc", type=float, default=None)
    parser.add_argument("--sl-price-cents", type=int, default=None)
    parser.add_argument("--ask-min", type=int, default=None)
    parser.add_argument("--ask-max", type=int, default=None)
    parser.add_argument("--entry-min", type=int, default=None)
    parser.add_argument("--entry-max", type=int, default=None)
    parser.add_argument("--gap-max", type=float, default=None)
    args, _ = parser.parse_known_args(argv)

    env_dry_run = os.getenv("DRY_RUN", "false").strip().lower() in ("1", "true", "yes")
    dry_run = args.dry_run or env_dry_run

    return Config(
        # Trading params — CLI arg > env var > default
        entry_window_min=args.entry_min or _int("ENTRY_MIN_SEC", 15),
        entry_window_max=args.entry_max or _int("ENTRY_MAX_SEC", 70),
        ask_min=args.ask_min or _int("ASK_MIN_CENTS", 80),
        ask_max=args.ask_max or _int("ASK_MAX_CENTS", 90),
        sl_price_cents=args.sl_price_cents or _int("SL_PRICE_CENTS", 72),
        gap_max=args.gap_max or _float("GAP_MAX_USD", 3.0),
        position_usdc=args.position_usdc or _float("POSITION_USDC", 2.0),
        switch_before_end_sec=_int("SWITCH_BEFORE_END_SEC", 8),
        # Credentials — support both naming conventions; strip whitespace/CRLF
        poly_api_key=(os.getenv("POLY_API_KEY", "")).strip(),
        poly_api_secret=(os.getenv("POLY_API_SECRET") or os.getenv("POLY_SECRET", "")).strip(),
        poly_api_passphrase=(os.getenv("POLY_API_PASSPHRASE") or os.getenv("POLY_PASSPHRASE", "")).strip(),
        poly_wallet_private_key=(os.getenv("POLY_WALLET_PRIVATE_KEY") or os.getenv("PRIVATE_KEY", "")).strip(),
        poly_funder_address=(os.getenv("POLY_FUNDER_ADDRESS", "")).strip(),
        # Infrastructure
        proxy_url=os.getenv("PROXY_URL", "http://mh2457652:r8hzakNM2T@82.206.73.210:50100"),
        dry_run=dry_run,
        market_slug_override=os.getenv("MARKET_SLUG_OVERRIDE", ""),
    )
