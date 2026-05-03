"""Market discovery: find active rounds and next rounds via Gamma API."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import requests

GAMMA_API = "https://gamma-api.polymarket.com"
MARKET_SLUG_PREFIX = "btc-updown-5m"
HEADERS = {"User-Agent": "Mozilla/5.0"}

logger = logging.getLogger("btc_bot")


def _build_proxies(proxy_url: str) -> dict[str, str] | None:
    if not proxy_url:
        return None
    return {"http": proxy_url, "https": proxy_url}


def _try_slug(slug: str, proxy_url: str = "") -> dict | None:
    """Request one slug, return market dict or None."""
    proxies = _build_proxies(proxy_url)
    try:
        resp = requests.get(
            f"{GAMMA_API}/events",
            params={"slug": slug},
            headers=HEADERS,
            timeout=8,
            proxies=proxies,
        )
        resp.raise_for_status()
        events = resp.json()
        if not events:
            return None
        ev = events[0]
        mkt = ev["markets"][0]
        end_raw = mkt.get("endDate", "")
        token_ids = mkt.get("clobTokenIds", "[]")
        if isinstance(token_ids, str):
            token_ids = json.loads(token_ids)
        if len(token_ids) < 2:
            return None
        end_dt = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
        return {
            "up_token": token_ids[0],
            "dn_token": token_ids[1],
            "end_date": end_raw,
            "end_dt": end_dt,
            "slug": slug,
            "question": mkt.get("question", ""),
        }
    except Exception:
        return None


def _scan_slugs(base_ts: int, count: int = 10, proxy_url: str = "") -> list[dict]:
    """
    Scan slugs of form {PREFIX}-{ts} starting at base_ts,
    stepping 300 s, for count iterations.
    Returns list of found market dicts.
    """
    found: list[dict] = []
    for delta in range(count):
        ts = base_ts + delta * 300
        slug = f"{MARKET_SLUG_PREFIX}-{ts}"
        item = _try_slug(slug, proxy_url=proxy_url)
        if item:
            found.append(item)
            logger.info("[scan] ✅ %s", slug)
    return found


def find_latest_active_market(proxy_url: str = "") -> dict:
    """Find the current active round at startup."""
    now = datetime.now(timezone.utc)
    now_ts = int(now.timestamp())
    base_ts = (now_ts // 300) * 300 - 3 * 300

    logger.info("[scan] Scanning slugs %s-{ts}...", MARKET_SLUG_PREFIX)
    candidates = _scan_slugs(base_ts, count=10, proxy_url=proxy_url)
    candidates = [c for c in candidates if c["end_dt"] > now]

    if not candidates:
        raise RuntimeError(
            f"Active round '{MARKET_SLUG_PREFIX}' not found. "
            "Check MARKET_SLUG_PREFIX or set MARKET_SLUG_OVERRIDE manually."
        )

    candidates.sort(key=lambda x: x["end_dt"])
    best = candidates[0]
    logger.info("[scan] Question: %s", best.get("question", ""))
    best.pop("end_dt", None)
    best.pop("question", None)
    return best


def find_next_market(current_end_date_iso: str, proxy_url: str = "") -> dict | None:
    """Find the next round after the current one ends."""
    try:
        current_end = datetime.fromisoformat(current_end_date_iso.replace("Z", "+00:00"))
    except Exception:
        current_end = datetime.now(timezone.utc)

    current_ts = int(current_end.timestamp())
    candidates = _scan_slugs(current_ts, count=8, proxy_url=proxy_url)
    candidates = [c for c in candidates if c["end_dt"] > current_end]

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["end_dt"])
    best = candidates[0]
    best.pop("end_dt", None)
    best.pop("question", None)
    return best
