from __future__ import annotations

import base64
import json
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend  # type: ignore

from .config import (
    get_kalshi_api_base_url,
    get_kalshi_api_key_id,
    get_kalshi_private_key_pem,
)


class KalshiHistoryMixin:
    def list_mention_markets_historical(
        self,
        *,
        text_term: Optional[str] = None,
        months: int = 12,
        include_closed: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        List historical mention-like markets for the last N months.
        By default returns final-result markets (settled/determined). If include_closed=True,
        also includes markets with status=closed (no final result yet).
        optionally filtered by text term.
        """
        import pandas as _pd

        earliest_ts = int((_pd.Timestamp.utcnow() - _pd.Timedelta(days=30 * max(months, 1))).timestamp())
        # Fetch final-result statuses; optionally include 'closed'
        statuses = ["settled", "determined"]
        if include_closed:
            statuses = ["closed", "settled", "determined"]
        combined: List[Dict[str, Any]] = []
        for s in statuses:
            try:
                items = self.list_markets_paginated(
                    status_filter=s,
                    per_page=500,
                    max_pages=20,
                    earliest_close_ts=earliest_ts,
                    min_close_ts=earliest_ts,
                    max_close_ts=int(_pd.Timestamp.utcnow().timestamp()),
                )
                if items:
                    combined.extend(items)
            except Exception:
                # Continue with other statuses even if one fails
                continue
        # Fallback: if nothing came back (API quirk), try without a status and filter locally
        if not combined:
            combined = self.list_markets_paginated(
                status_filter=None, per_page=500, max_pages=20, earliest_close_ts=earliest_ts
            )
            combined = [m for m in combined if str(m.get("status", "")).lower() in set(statuses)]
        mention_like = _filter_mention_like(combined)
        if text_term:
            mention_like = [m for m in mention_like if _contains_term(m, text_term)]
        by_ticker: Dict[str, Dict[str, Any]] = {}
        for m in mention_like:
            t = m.get("ticker")
            if t and t not in by_ticker:
                by_ticker[t] = m
        return list(by_ticker.values())

    def list_mention_markets_closed_recent(self, *, limit: int = 12) -> List[Dict[str, Any]]:
        """
        Returns the most recent 'closed' mention-like markets (not necessarily final results).
        """
        items = self.list_markets_paginated(status_filter="closed", per_page=500, max_pages=5, earliest_close_ts=None)
        mention_like = _filter_mention_like(items)
        # Sort by close time desc
        import pandas as _pd
        def to_ts(m: Dict[str, Any]) -> int:
            t = m.get("close_time") or m.get("end_date") or m.get("expiry_time") or m.get("latest_expiration_time")
            ts = _pd.to_datetime(t, utc=True, errors="coerce")
            if ts is None or _pd.isna(ts):
                return 0
            return int(ts.timestamp())
        mention_like.sort(key=to_ts, reverse=True)
        # Dedup by ticker and take first N
        by_ticker: Dict[str, Dict[str, Any]] = {}
        for m in mention_like:
            t = m.get("ticker")
            if t and t not in by_ticker:
                by_ticker[t] = m
        return list(by_ticker.values())[: max(0, int(limit))]

    def list_mention_markets_window(
        self,
        *,
        months: int = 12,
        statuses: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Markets-based bootstrap for a time window using Kalshi's min/max_close_ts.
        """
        import pandas as _pd
        earliest_ts = int((_pd.Timestamp.utcnow() - _pd.Timedelta(days=30 * max(months, 1))).timestamp())
        latest_ts = int(_pd.Timestamp.utcnow().timestamp())
        if not statuses:
            statuses = ["closed", "settled", "determined"]
        combined: List[Dict[str, Any]] = []
        for s in statuses:
            try:
                items = self.list_markets_paginated(
                    status_filter=s,
                    per_page=500,
                    max_pages=50,
                    min_close_ts=earliest_ts,
                    max_close_ts=latest_ts,
                )
                if items:
                    combined.extend(items)
            except Exception:
                continue
        # Dedup by ticker
        by_ticker: Dict[str, Dict[str, Any]] = {}
        for m in combined:
            t = m.get("ticker")
            if t and t not in by_ticker:
                by_ticker[t] = m
        return list(by_ticker.values())


class KalshiClient(KalshiHistoryMixin):
    """
    Minimal Kalshi HTTP client with RSA-PSS request signing.
    """

    def __init__(self, *, base_url: Optional[str] = None) -> None:
        self.base_url = (base_url or get_kalshi_api_base_url()).rstrip("/")
        self.api_key_id = get_kalshi_api_key_id()
        self._private_key = self._load_private_key(get_kalshi_private_key_pem())
        self._session = requests.Session()

    @staticmethod
    def _load_private_key(pem_text: str):
        return serialization.load_pem_private_key(
            pem_text.encode("utf-8"),
            password=None,
            backend=default_backend(),
        )

    def _sign_headers(self, method: str, path: str, body_bytes: Optional[bytes] = None) -> Dict[str, str]:
        """
        Creates Kalshi signing headers:
          - KALSHI-ACCESS-KEY
          - KALSHI-ACCESS-TIMESTAMP (ms)
          - KALSHI-ACCESS-SIGNATURE (base64 of RSA-PSS SHA256 signature)
        Message format: timestamp + method + path + (body or empty)
        """
        timestamp_ms = str(int(time.time() * 1000))
        normalized_method = (method or "GET").upper()
        msg_parts = [timestamp_ms, normalized_method, path]
        if body_bytes:
            msg_parts.append(body_bytes.decode("utf-8"))
        message = "".join(msg_parts).encode("utf-8")

        signature = self._private_key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
            hashes.SHA256(),
        )
        sig_b64 = base64.b64encode(signature).decode("utf-8")

        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "KALSHI-ACCESS-SIGNATURE": sig_b64,
        }

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        timeout: int = 20,
    ) -> Tuple[int, Dict[str, Any]]:
        url = f"{self.base_url}{path}"
        body_bytes = json.dumps(json_body).encode("utf-8") if json_body is not None else None
        headers = self._sign_headers(method, path, body_bytes)
        if json_body is not None:
            headers["Content-Type"] = "application/json"
        resp = self._session.request(method=method, url=url, headers=headers, params=params, json=json_body, timeout=timeout)
        try:
            data: Dict[str, Any] = resp.json()
        except Exception:
            data = {"raw": resp.text}
        return resp.status_code, data

    # Debug/raw access
    def request_debug(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        timeout: int = 20,
    ) -> Dict[str, Any]:
        status, data = self._request(method, path, params=params, json_body=json_body, timeout=timeout)
        return {
            "status": status,
            "data": data,
        }

    # Public methods

    def list_series(self, *, limit: int = 100, cursor: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        status, data = self._request("GET", "/trade-api/v2/series", params=params)
        if status != 200:
            raise RuntimeError(f"Kalshi series request failed: {status} {data}")
        return data

    def list_markets(
        self,
        *,
        series_ticker: Optional[str] = None,
        status_filter: Optional[str] = None,
        limit: int = 200,
        cursor: Optional[str] = None,
        min_close_ts: Optional[int] = None,
        max_close_ts: Optional[int] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit}
        if series_ticker:
            params["series_ticker"] = series_ticker
        if status_filter:
            # Valid values per docs: 'initialized', 'active', 'closed', 'settled', 'determined'
            params["status"] = status_filter
        if cursor:
            params["cursor"] = cursor
        if min_close_ts is not None:
            params["min_close_ts"] = int(min_close_ts)
        if max_close_ts is not None:
            params["max_close_ts"] = int(max_close_ts)
        status, data = self._request("GET", "/trade-api/v2/markets", params=params)
        if status != 200:
            raise RuntimeError(f"Kalshi markets request failed: {status} {data}")
        return data

    def list_events(
        self,
        *,
        series_ticker: Optional[str] = None,
        limit: int = 200,
        cursor: Optional[str] = None,
        with_nested_markets: bool = False,
        status_filter: Optional[str] = None,
        min_close_ts: Optional[int] = None,
        max_close_ts: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Fetch events. When with_nested_markets is True, response includes 'markets' array per event.
        """
        params: Dict[str, Any] = {"limit": limit}
        if series_ticker:
            params["series_ticker"] = series_ticker
        if cursor:
            params["cursor"] = cursor
        if with_nested_markets:
            params["with_nested_markets"] = "true"
        if status_filter:
            # Expected values per docs: 'open', 'closed', 'settled', 'determined'
            params["status"] = status_filter
        if min_close_ts is not None:
            params["min_close_ts"] = int(min_close_ts)
        if max_close_ts is not None:
            params["max_close_ts"] = int(max_close_ts)
        status, data = self._request("GET", "/trade-api/v2/events", params=params)
        if status != 200:
            raise RuntimeError(f"Kalshi events request failed: {status} {data}")
        return data

    def list_events_paginated(
        self,
        *,
        series_ticker: Optional[str] = None,
        per_page: int = 100,
        max_pages: int = 50,
        with_nested_markets: bool = True,
        status_filter: Optional[str] = None,
        min_close_ts: Optional[int] = None,
        max_close_ts: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Paginate through events; optionally include nested markets.
        """
        all_events: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        for _ in range(max_pages):
            data = self.list_events(
                series_ticker=series_ticker,
                limit=per_page,
                cursor=cursor,
                with_nested_markets=with_nested_markets,
                status_filter=status_filter,
                min_close_ts=min_close_ts,
                max_close_ts=max_close_ts,
            )
            evs = data.get("events", []) or data.get("data", []) or []
            if not evs:
                break
            all_events.extend(evs)
            cursor = data.get("cursor")
            if not cursor:
                break
        return all_events

    def list_markets_paginated(
        self,
        *,
        series_ticker: Optional[str] = None,
        status_filter: Optional[str] = None,
        per_page: int = 500,
        max_pages: int = 20,
        earliest_close_ts: Optional[int] = None,
        min_close_ts: Optional[int] = None,
        max_close_ts: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch multiple pages of markets to cover historical queries.
        If earliest_close_ts (epoch seconds) is provided, paginate until we reach
        markets older than that threshold or pages are exhausted.
        """
        all_items: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        for _ in range(max_pages):
            data = self.list_markets(
                series_ticker=series_ticker,
                status_filter=status_filter,
                limit=per_page,
                cursor=cursor,
                min_close_ts=min_close_ts,
                max_close_ts=max_close_ts,
            )
            items = data.get("markets", []) or data.get("data", []) or []
            if not items:
                break
            all_items.extend(items)
            cursor = data.get("cursor")
            if earliest_close_ts is not None:
                import pandas as _pd
                # find oldest close timestamp in this page
                ts_list = []
                for m in items:
                    t = m.get("close_time") or m.get("end_date") or m.get("expiry_time") or m.get("latest_expiration_time")
                    ts = _pd.to_datetime(t, utc=True, errors="coerce")
                    if ts is not None and not _pd.isna(ts):
                        ts_list.append(int(ts.timestamp()))
                if ts_list and min(ts_list) < int(earliest_close_ts):
                    break
            if not cursor:
                break
        # Deduplicate by ticker
        by_ticker: Dict[str, Dict[str, Any]] = {}
        for m in all_items:
            t = m.get("ticker")
            if t and t not in by_ticker:
                by_ticker[t] = m
        return list(by_ticker.values())

    def list_markets_debug(
        self,
        *,
        series_ticker: Optional[str] = None,
        status_filter: Optional[str] = None,
        limit: int = 50,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit}
        if series_ticker:
            params["series_ticker"] = series_ticker
        if status_filter:
            params["status"] = status_filter
        return self.request_debug("GET", "/trade-api/v2/markets", params=params)

    def find_mention_series_tickers(self) -> List[str]:
        """
        Heuristic to find 'Mention-like' series. Looks for 'mention', 'say', or 'speech'
        in series title or ticker.
        """
        tickers: List[str] = []
        data = self.list_series(limit=200)
        items = data.get("series", []) or data.get("data", []) or []
        for s in items:
            title = str(s.get("title", "")).lower()
            ticker = str(s.get("ticker", "")).lower()
            if (
                "mention" in title
                or "mention" in ticker
                or " say " in f" {title} "
                or "say" in ticker
                or "speech" in title
                or "speech" in ticker
            ):
                t = s.get("ticker")
                if t:
                    tickers.append(t)
        # Deduplicate preserving order
        seen = set()
        unique = []
        for t in tickers:
            if t not in seen:
                seen.add(t)
                unique.append(t)
        return unique

    def list_mention_markets(self) -> List[Dict[str, Any]]:
        """
        Attempts to list markets for 'Mention-like' series. Falls back to filtering all markets
        by category=='mentions' or text/ticker heuristics that catch "mention" and "say" markets.
        """
        mention_markets: List[Dict[str, Any]] = []
        try:
            series_tickers = self.find_mention_series_tickers()
        except Exception:
            series_tickers = []

        if series_tickers:
            for stkr in series_tickers:
                try:
                    data = self.list_markets(series_ticker=stkr, status_filter="active", limit=500)
                    markets = data.get("markets", []) or data.get("data", []) or []
                    mention_markets.extend(markets)
                except Exception:
                    # Skip problematic series
                    continue
            # Deduplicate and filter category first
            by_ticker: Dict[str, Dict[str, Any]] = {}
            for m in mention_markets:
                t = m.get("ticker")
                if t and t not in by_ticker:
                    by_ticker[t] = m
            values = list(by_ticker.values())
            cat_filtered = [m for m in values if str(m.get("category", "")).lower() == "mentions"]
            # Fallback if empty after series query: global fetch + category/heuristic filter
            if not cat_filtered and not values:
                try:
                    data = self.list_markets(status_filter="active", limit=500)
                    all_markets = data.get("markets", []) or data.get("data", []) or []
                    filtered_cat = [m for m in all_markets if str(m.get("category", "")).lower() == "mentions"]
                    return filtered_cat or _filter_mention_like(all_markets)
                except Exception:
                    return []
            if cat_filtered:
                return cat_filtered
            return _filter_mention_like(values)

        # No series found: global fetch
        try:
            data = self.list_markets(status_filter="active", limit=500)
            all_markets = data.get("markets", []) or data.get("data", []) or []
            filtered_cat = [m for m in all_markets if str(m.get("category", "")).lower() == "mentions"]
            return filtered_cat or _filter_mention_like(all_markets)
        except Exception:
            return []

    def list_mention_events_active(self) -> List[Dict[str, Any]]:
        """
        Returns events (with nested markets) that are mention-like and currently have > 1 active markets.
        """
        try:
            series_tickers = self.find_mention_series_tickers()
        except Exception:
            series_tickers = []
        events: List[Dict[str, Any]] = []
        # Fetch by candidate series to reduce scope
        if series_tickers:
            for stkr in series_tickers:
                try:
                    data = self.list_events(series_ticker=stkr, limit=200, with_nested_markets=True)
                    evs = data.get("events", []) or data.get("data", []) or []
                    if isinstance(evs, list):
                        events.extend(evs)
                except Exception:
                    continue
        # Fallback global fetch if necessary
        if not events:
            try:
                data = self.list_events(limit=200, with_nested_markets=True)
                evs = data.get("events", []) or data.get("data", []) or []
                if isinstance(evs, list):
                    events.extend(evs)
            except Exception:
                pass
        # Filter to mention-like at event or nested market level
        filtered: List[Dict[str, Any]] = []
        for e in events:
            if not isinstance(e, dict):
                continue
            title = str(e.get("title", "")).lower()
            series_ticker = str(e.get("series_ticker", "")).lower()
            ev_ticker = str(e.get("event_ticker", "")).lower()
            markets = e.get("markets") or []
            is_mention_event = (
                "mention" in title
                or " say " in f" {title} "
                or "mention" in series_ticker
                or "say" in series_ticker
                or "mention" in ev_ticker
                or "say" in ev_ticker
            )
            # Or any nested market qualifies
            if not is_mention_event:
                for m in markets or []:
                    if not isinstance(m, dict):
                        continue
                    cat = str(m.get("category", "")).lower()
                    mtitle = str(m.get("title", "")).lower()
                    mtick = str(m.get("ticker", "")).lower()
                    if (
                        cat == "mentions"
                        or "mention" in mtitle
                        or " say " in f" {mtitle} "
                        or "mention" in mtick
                        or "say" in mtick
                    ):
                        is_mention_event = True
                        break
            if not is_mention_event:
                continue
            # Keep only active markets and dedupe by ticker
            by_ticker: Dict[str, Dict[str, Any]] = {}
            for m in (markets or []):
                if not isinstance(m, dict):
                    continue
                if str(m.get("status", "")).lower() != "active":
                    continue
                t = m.get("ticker")
                if t and t not in by_ticker:
                    by_ticker[t] = m
            active_markets = list(by_ticker.values())
            if len(active_markets) <= 1:
                continue
            # Return event with filtered markets
            filtered.append({**e, "markets": active_markets})
        # Deduplicate by event_ticker
        by_evt: Dict[str, Dict[str, Any]] = {}
        for e in filtered:
            t = e.get("event_ticker")
            if t and t not in by_evt:
                by_evt[t] = e
        return list(by_evt.values())

    def list_mention_events_not_active(
        self,
        *,
        text_term: Optional[str] = None,
        months: int = 12,
        include_closed: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Returns mention-like events with nested markets filtered to NOT ACTIVE.
        When include_closed is False, returns only settled/determined markets.
        """
        import pandas as _pd
        earliest_ts = int((_pd.Timestamp.utcnow() - _pd.Timedelta(days=30 * max(months, 1))).timestamp())
        # Page through events broadly; we'll filter by time below
        events = self.list_events_paginated(per_page=100, max_pages=50, with_nested_markets=True, earliest_close_ts=None)
        results: List[Dict[str, Any]] = []
        for e in events:
            if not isinstance(e, dict):
                continue
            title = str(e.get("title", "")).lower()
            series_ticker_l = str(e.get("series_ticker", "")).lower()
            ev_ticker_l = str(e.get("event_ticker", "")).lower()
            is_mention_event = (
                "mention" in title
                or " say " in f" {title} "
                or "mention" in series_ticker_l
                or "say" in series_ticker_l
                or "mention" in ev_ticker_l
                or "say" in ev_ticker_l
            )
            mkts = [m for m in (e.get("markets") or []) if isinstance(m, dict)]
            if not is_mention_event:
                for m in mkts:
                    cat = str(m.get("category", "")).lower()
                    mtitle = str(m.get("title", "")).lower()
                    mtick = str(m.get("ticker", "")).lower()
                    if cat == "mentions" or "mention" in mtitle or " say " in f" {mtitle} " or "mention" in mtick or "say" in mtick:
                        is_mention_event = True
                        break
            if not is_mention_event:
                continue
            # Filter to not-active statuses
            allowed = {"settled", "determined"} if not include_closed else {"closed", "settled", "determined"}
            filt_mkts: List[Dict[str, Any]] = []
            for m in mkts:
                st = str(m.get("status", "")).lower()
                if st not in allowed:
                    continue
                # Months lookback per market end
                t = m.get("close_time") or m.get("end_date") or m.get("expiry_time") or m.get("latest_expiration_time")
                ts = _pd.to_datetime(t, utc=True, errors="coerce")
                if ts is None or _pd.isna(ts) or int(ts.timestamp()) < earliest_ts:
                    continue
                filt_mkts.append(m)
            if not filt_mkts:
                continue
            # Optional text filter across market and event fields
            if text_term:
                needle = text_term.lower().strip()
                def m_has_term(m: Dict[str, Any]) -> bool:
                    fields = [
                        str(e.get("title", "")),
                        str(m.get("title", "")),
                        str(m.get("subtitle", "")),
                        str(m.get("yes_sub_title", "")),
                        str(m.get("no_sub_title", "")),
                        str(m.get("ticker", "")),
                        str(m.get("event_ticker", "")),
                        str(m.get("series_ticker", "")),
                    ]
                    return needle in " ".join(fields).lower()
                filt_mkts = [m for m in filt_mkts if m_has_term(m)]
                if not filt_mkts:
                    continue
            results.append({**e, "markets": filt_mkts})
        # Deduplicate events
        by_evt: Dict[str, Dict[str, Any]] = {}
        for e in results:
            t = e.get("event_ticker")
            if t and t not in by_evt:
                by_evt[t] = e
        return list(by_evt.values())

    def list_mention_events_window(self, *, months: int = 12) -> List[Dict[str, Any]]:
        """
        BROAD bootstrap fetch: mention-like events within a months window, with nested markets preserved.
        Queries historical statuses (closed, settled, determined) scoped to mention-like series tickers
        to ensure complete coverage without needing to page through all Kalshi events.
        Filters markets by end-time locally for accuracy.
        """
        import pandas as _pd
        earliest_ts = int((_pd.Timestamp.utcnow() - _pd.Timedelta(days=30 * max(months, 1))).timestamp())

        # Filter-first: discover mention-like series tickers, then fetch events only within those series.
        # This avoids paging through the entire events corpus (which can truncate older mention events).
        try:
            series_tickers = self.find_mention_series_tickers()
        except Exception:
            series_tickers = []

        # Query for EACH historical status separately to ensure complete coverage.
        # The Kalshi API only returns events matching the requested status.
        all_events: List[Dict[str, Any]] = []
        for status in ["closed", "settled", "determined"]:
            # If we failed to discover series tickers, fall back to a global fetch (slower).
            targets = series_tickers if series_tickers else [None]
            for stkr in targets:
                try:
                    evs = self.list_events_paginated(
                        series_ticker=stkr,
                        per_page=200,
                        max_pages=500,
                        with_nested_markets=True,
                        status_filter=status,
                        # Avoid event-level time filters here; we filter market end-times locally below.
                    )
                    if evs:
                        all_events.extend(evs)
                except Exception:
                    continue

        # Deduplicate by event_ticker first (same event may appear in multiple status queries)
        by_evt_raw: Dict[str, Dict[str, Any]] = {}
        for e in all_events:
            if not isinstance(e, dict):
                continue
            t = e.get("event_ticker")
            if t and t not in by_evt_raw:
                by_evt_raw[t] = e
        events = list(by_evt_raw.values())

        # Filter to mention-like events
        results: List[Dict[str, Any]] = []
        for e in events:
            title = str(e.get("title", "")).lower()
            series_ticker_l = str(e.get("series_ticker", "")).lower()
            ev_ticker_l = str(e.get("event_ticker", "")).lower()
            is_mention_event = (
                "mention" in title
                or " say " in f" {title} "
                or "mention" in series_ticker_l
                or "say" in series_ticker_l
                or "mention" in ev_ticker_l
                or "say" in ev_ticker_l
            )
            mkts = [m for m in (e.get("markets") or []) if isinstance(m, dict)]
            if not is_mention_event:
                for m in mkts:
                    cat = str(m.get("category", "")).lower()
                    mtitle = str(m.get("title", "")).lower()
                    mtick = str(m.get("ticker", "")).lower()
                    if cat == "mentions" or "mention" in mtitle or " say " in f" {mtitle} " or "mention" in mtick or "say" in mtick:
                        is_mention_event = True
                        break
            if not is_mention_event:
                continue
            # Keep only markets whose end is within the window (filter locally for accuracy)
            filt: List[Dict[str, Any]] = []
            for m in mkts:
                t = m.get("close_time") or m.get("end_date") or m.get("expiry_time") or m.get("latest_expiration_time")
                ts = _pd.to_datetime(t, utc=True, errors="coerce")
                if ts is None or _pd.isna(ts) or int(ts.timestamp()) < earliest_ts:
                    continue
                filt.append(m)
            if not filt:
                continue
            results.append({**e, "markets": filt})
        # Final dedup
        by_evt: Dict[str, Dict[str, Any]] = {}
        for e in results:
            t = e.get("event_ticker")
            if t and t not in by_evt:
                by_evt[t] = e
        return list(by_evt.values())

    def list_mention_events_closed_recent(self, *, limit: int = 12) -> List[Dict[str, Any]]:
        """
        Most recent mention-like events that have at least one market in statuses
        {'closed','settled','determined'}. Returns events with nested markets preserved.
        """
        import pandas as _pd
        events = self.list_events_paginated(per_page=100, max_pages=50, with_nested_markets=True, earliest_close_ts=None)
        allowed = {"closed", "settled", "determined"}
        shortlisted: List[Dict[str, Any]] = []
        for e in events:
            if not isinstance(e, dict):
                continue
            title = str(e.get("title", "")).lower()
            series_ticker = str(e.get("series_ticker", "")).lower()
            ev_ticker = str(e.get("event_ticker", "")).lower()
            is_mention = (
                "mention" in title
                or " say " in f" {title} "
                or "mention" in series_ticker
                or "say" in series_ticker
                or "mention" in ev_ticker
                or "say" in ev_ticker
            )
            mkts = [m for m in (e.get("markets") or []) if isinstance(m, dict)]
            if not is_mention:
                for m in mkts:
                    cat = str(m.get("category", "")).lower()
                    mtitle = str(m.get("title", "")).lower()
                    mtick = str(m.get("ticker", "")).lower()
                    if cat == "mentions" or "mention" in mtitle or " say " in f" {mtitle} " or "mention" in mtick or "say" in mtick:
                        is_mention = True
                        break
            if not is_mention:
                continue
            # Keep only markets with allowed statuses
            filtered = [m for m in mkts if str(m.get("status", "")).lower() in allowed]
            if not filtered:
                continue
            # Compute most recent end among filtered markets
            ts_list: List[int] = []
            for m in filtered:
                t = m.get("close_time") or m.get("end_date") or m.get("expiry_time") or m.get("latest_expiration_time")
                ts = _pd.to_datetime(t, utc=True, errors="coerce")
                if ts is not None and not _pd.isna(ts):
                    ts_list.append(int(ts.timestamp()))
            if not ts_list:
                continue
            shortlisted.append({**e, "markets": filtered, "_latest_ts": max(ts_list)})
        shortlisted.sort(key=lambda e: int(e.get("_latest_ts") or 0), reverse=True)
        # Drop helper field and cap
        out: List[Dict[str, Any]] = []
        for e in shortlisted[: max(0, int(limit))]:
            e2 = dict(e)
            e2.pop("_latest_ts", None)
            out.append(e2)
        return out

    def list_mention_events_window_events_api(
        self,
        *,
        months: int = 12,
        statuses: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Use Events API to fetch mention-like events within a time window, filtering events
        by status at the event level. Markets are kept as-is (no status filtering).
        """
        import pandas as _pd
        earliest_ts = int((_pd.Timestamp.utcnow() - _pd.Timedelta(days=30 * max(months, 1))).timestamp())
        latest_ts = int(_pd.Timestamp.utcnow().timestamp())
        if not statuses:
            statuses = ["closed", "settled", "determined"]
        collected: List[Dict[str, Any]] = []
        for s in statuses:
            try:
                evs = self.list_events_paginated(
                    per_page=100,
                    max_pages=100,
                    with_nested_markets=True,
                    status_filter=s,
                    min_close_ts=earliest_ts,
                    max_close_ts=latest_ts,
                )
                if evs:
                    collected.extend(evs)
            except Exception:
                continue
        # Filter to mention-like using event-level fields OR nested markets
        filtered: List[Dict[str, Any]] = []
        for e in collected:
            if not isinstance(e, dict):
                continue
            title = str(e.get("title", "")).lower()
            series_ticker = str(e.get("series_ticker", "")).lower()
            ev_ticker = str(e.get("event_ticker", "")).lower()
            mkts = [m for m in (e.get("markets") or []) if isinstance(m, dict)]
            is_mention = (
                "mention" in title
                or " say " in f" {title} "
                or "mention" in series_ticker
                or "say" in series_ticker
                or "mention" in ev_ticker
                or "say" in ev_ticker
            )
            if not is_mention:
                for m in mkts:
                    cat = str(m.get("category", "")).lower()
                    mtitle = str(m.get("title", "")).lower()
                    mtick = str(m.get("ticker", "")).lower()
                    if cat == "mentions" or "mention" in mtitle or " say " in f" {mtitle} " or "mention" in mtick or "say" in mtick:
                        is_mention = True
                        break
            if is_mention:
                filtered.append(e)
        # Deduplicate by event_ticker
        by_evt: Dict[str, Dict[str, Any]] = {}
        for e in filtered:
            t = e.get("event_ticker")
            if t and t not in by_evt:
                by_evt[t] = e
        return list(by_evt.values())


def _filter_mention_like(markets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Flexible filter to capture 'mention' and 'say' style markets.
    Includes any market where:
      - category == mentions OR
      - title contains 'mention' or 'say' OR
      - ticker/event_ticker/series_ticker contains 'MENTION' or 'SAY'
    """
    results: List[Dict[str, Any]] = []
    for m in markets:
        category = str(m.get("category", "")).lower()
        title = str(m.get("title", "")).lower()
        ticker = str(m.get("ticker", "")).lower()
        event_ticker = str(m.get("event_ticker", "")).lower()
        series_ticker = str(m.get("series_ticker", "")).lower()
        if (
            category == "mentions"
            or "mention" in title
            or " say " in f" {title} "
            or "mention" in ticker
            or "say" in ticker
            or "mention" in event_ticker
            or "say" in event_ticker
            or "mention" in series_ticker
            or "say" in series_ticker
        ):
            results.append(m)
    return results


def _contains_term(m: Dict[str, Any], term: str) -> bool:
    if not term:
        return True
    needle = term.lower().strip()
    if not needle:
        return True
    fields = [
        str(m.get("title", "")),
        str(m.get("subtitle", "")),
        str(m.get("yes_sub_title", "")),
        str(m.get("no_sub_title", "")),
        str(m.get("ticker", "")),
        str(m.get("event_ticker", "")),
        str(m.get("series_ticker", "")),
    ]
    hay = " ".join(fields).lower()
    return needle in hay






