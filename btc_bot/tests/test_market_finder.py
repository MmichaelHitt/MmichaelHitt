"""Tests for market_finder.py — Block 3."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from btc_bot.market_finder import (
    _scan_slugs,
    _try_slug,
    find_latest_active_market,
    find_next_market,
)

FUTURE_DT = datetime.now(timezone.utc) + timedelta(hours=1)
FUTURE_ISO = FUTURE_DT.strftime("%Y-%m-%dT%H:%M:%SZ")

PAST_DT = datetime.now(timezone.utc) - timedelta(hours=1)
PAST_ISO = PAST_DT.strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_event_response(slug: str, end_iso: str, token_ids=None):
    if token_ids is None:
        token_ids = ["token_up_123", "token_dn_456"]
    return [
        {
            "markets": [
                {
                    "endDate": end_iso,
                    "clobTokenIds": json.dumps(token_ids),
                    "question": f"Will BTC go up? ({slug})",
                }
            ]
        }
    ]


class TestTrySlug:
    def test_returns_none_on_empty_response(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status.return_value = None
        with patch("requests.get", return_value=mock_resp):
            result = _try_slug("btc-updown-5m-999")
        assert result is None

    def test_returns_none_on_request_exception(self):
        with patch("requests.get", side_effect=Exception("timeout")):
            result = _try_slug("btc-updown-5m-999")
        assert result is None

    def test_returns_dict_with_correct_keys(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _make_event_response("btc-updown-5m-100", FUTURE_ISO)
        mock_resp.raise_for_status.return_value = None
        with patch("requests.get", return_value=mock_resp):
            result = _try_slug("btc-updown-5m-100")
        assert result is not None
        assert result["up_token"] == "token_up_123"
        assert result["dn_token"] == "token_dn_456"
        assert result["slug"] == "btc-updown-5m-100"
        assert "end_dt" in result

    def test_returns_none_when_fewer_than_2_tokens(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {
                "markets": [
                    {
                        "endDate": FUTURE_ISO,
                        "clobTokenIds": json.dumps(["only_one"]),
                        "question": "Q",
                    }
                ]
            }
        ]
        mock_resp.raise_for_status.return_value = None
        with patch("requests.get", return_value=mock_resp):
            result = _try_slug("btc-updown-5m-100")
        assert result is None

    def test_passes_proxy_to_requests(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _make_event_response("btc-updown-5m-100", FUTURE_ISO)
        mock_resp.raise_for_status.return_value = None
        with patch("requests.get", return_value=mock_resp) as mock_get:
            _try_slug("btc-updown-5m-100", proxy_url="http://user:pass@1.2.3.4:1234")
        call_kwargs = mock_get.call_args.kwargs
        assert call_kwargs["proxies"] == {
            "http": "http://user:pass@1.2.3.4:1234",
            "https": "http://user:pass@1.2.3.4:1234",
        }

    def test_no_proxy_when_empty(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _make_event_response("btc-updown-5m-100", FUTURE_ISO)
        mock_resp.raise_for_status.return_value = None
        with patch("requests.get", return_value=mock_resp) as mock_get:
            _try_slug("btc-updown-5m-100", proxy_url="")
        call_kwargs = mock_get.call_args.kwargs
        assert call_kwargs["proxies"] is None


class TestScanSlugs:
    def test_returns_only_found_slugs(self):
        responses = {}
        base_ts = 1_700_000_000
        for i in range(3):
            slug = f"btc-updown-5m-{base_ts + i * 300}"
            responses[slug] = _make_event_response(slug, FUTURE_ISO)

        def mock_get(url, params=None, **kwargs):
            slug = params.get("slug", "")
            m = MagicMock()
            m.raise_for_status.return_value = None
            m.json.return_value = responses.get(slug, [])
            return m

        with patch("requests.get", side_effect=mock_get):
            found = _scan_slugs(base_ts, count=5)
        assert len(found) == 3

    def test_returns_empty_list_when_none_found(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status.return_value = None
        with patch("requests.get", return_value=mock_resp):
            found = _scan_slugs(1_700_000_000, count=3)
        assert found == []


class TestFindLatestActiveMarket:
    def _make_mock_get(self, active_slugs: list[tuple[str, str]]):
        slug_map = {slug: resp for slug, resp in active_slugs}

        def mock_get(url, params=None, **kwargs):
            slug = (params or {}).get("slug", "")
            m = MagicMock()
            m.raise_for_status.return_value = None
            m.json.return_value = slug_map.get(slug, [])
            return m

        return mock_get

    def test_returns_earliest_active_market(self):
        now = datetime.now(timezone.utc)
        base_ts = (int(now.timestamp()) // 300) * 300 - 3 * 300
        slug1 = f"btc-updown-5m-{base_ts}"
        slug2 = f"btc-updown-5m-{base_ts + 300}"
        end1 = (now + timedelta(minutes=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end2 = (now + timedelta(minutes=8)).strftime("%Y-%m-%dT%H:%M:%SZ")
        active = [
            (slug1, _make_event_response(slug1, end1)),
            (slug2, _make_event_response(slug2, end2)),
        ]
        with patch("requests.get", side_effect=self._make_mock_get(active)):
            result = find_latest_active_market()
        assert result["slug"] == slug1

    def test_raises_when_no_active_round(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status.return_value = None
        with patch("requests.get", return_value=mock_resp):
            with pytest.raises(RuntimeError, match="Active round"):
                find_latest_active_market()

    def test_excludes_expired_markets(self):
        now = datetime.now(timezone.utc)
        base_ts = (int(now.timestamp()) // 300) * 300 - 3 * 300
        slug_past = f"btc-updown-5m-{base_ts}"
        slug_future = f"btc-updown-5m-{base_ts + 900}"
        end_past = (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_future = (now + timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
        active = [
            (slug_past, _make_event_response(slug_past, end_past)),
            (slug_future, _make_event_response(slug_future, end_future)),
        ]
        with patch("requests.get", side_effect=self._make_mock_get(active)):
            result = find_latest_active_market()
        assert result["slug"] == slug_future

    def test_result_has_no_end_dt_or_question_keys(self):
        now = datetime.now(timezone.utc)
        base_ts = (int(now.timestamp()) // 300) * 300 - 3 * 300
        slug = f"btc-updown-5m-{base_ts}"
        end = (now + timedelta(minutes=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
        active = [(slug, _make_event_response(slug, end))]
        with patch("requests.get", side_effect=self._make_mock_get(active)):
            result = find_latest_active_market()
        assert "end_dt" not in result
        assert "question" not in result


class TestFindNextMarket:
    def test_returns_none_when_no_candidates(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status.return_value = None
        with patch("requests.get", return_value=mock_resp):
            result = find_next_market(FUTURE_ISO)
        assert result is None

    def test_returns_market_after_current_end(self):
        current_end = datetime.now(timezone.utc) + timedelta(seconds=30)
        current_end_iso = current_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        current_ts = int(current_end.timestamp())

        next_slug = f"btc-updown-5m-{current_ts}"
        next_end = (current_end + timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")

        def mock_get(url, params=None, **kwargs):
            slug = (params or {}).get("slug", "")
            m = MagicMock()
            m.raise_for_status.return_value = None
            if slug == next_slug:
                m.json.return_value = _make_event_response(next_slug, next_end)
            else:
                m.json.return_value = []
            return m

        with patch("requests.get", side_effect=mock_get):
            result = find_next_market(current_end_iso)
        assert result is not None
        assert result["slug"] == next_slug

    def test_invalid_iso_does_not_raise(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status.return_value = None
        with patch("requests.get", return_value=mock_resp):
            result = find_next_market("not-a-date")
        assert result is None
