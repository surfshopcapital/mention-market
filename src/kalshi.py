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
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit}
        if series_ticker:
            params["series_ticker"] = series_ticker
        if status_filter:
            # Supported values: 'unopened', 'open', 'closed', 'settled'
            params["status"] = status_filter
        if cursor:
            params["cursor"] = cursor
        status, data = self._request("GET", "/trade-api/v2/markets", params=params)
        if status != 200:
            raise RuntimeError(f"Kalshi markets request failed: {status} {data}")
        return data

    def list_markets_paginated(
        self,
        *,
        series_ticker: Optional[str] = None,
        status_filter: Optional[str] = None,
        per_page: int = 500,
        max_pages: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Fetch multiple pages of markets to cover historical queries.
        """
        all_items: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        for _ in range(max_pages):
            data = self.list_markets(series_ticker=series_ticker, status_filter=status_filter, limit=per_page, cursor=cursor)
            items = data.get("markets", []) or data.get("data", []) or []
            if not items:
                break
            all_items.extend(items)
            cursor = data.get("cursor")
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
                    data = self.list_markets(series_ticker=stkr, status_filter="open", limit=200)
                    markets = data.get("markets", []) or data.get("data", []) or []
                    mention_markets.extend(markets)
                except Exception:
                    # Skip problematic series
                    continue
            # Deduplicate by ticker if present
            by_ticker: Dict[str, Dict[str, Any]] = {}
            for m in mention_markets:
                t = m.get("ticker")
                if t and t not in by_ticker:
                    by_ticker[t] = m
            values = list(by_ticker.values())
            # Prefer category 'mentions' if present, else include heuristics
            cat_filtered = [m for m in values if str(m.get("category", "")).lower() == "mentions"]
            if cat_filtered:
                return cat_filtered
            return _filter_mention_like(values)

        # Fallback: fetch a broad markets page and filter by title text
        try:
            data = self.list_markets(status_filter="open", limit=500)
            all_markets = data.get("markets", []) or data.get("data", []) or []
            # Prefer category filter first; otherwise apply heuristics
            filtered_cat = [m for m in all_markets if str(m.get("category", "")).lower() == "mentions"]
            return filtered_cat or _filter_mention_like(all_markets)
        except Exception:
            return []


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


class KalshiHistoryMixin:
    def list_mention_markets_historical(self, *, text_term: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List historical (closed/settled) mention-like markets, optionally filtered by text term.
        """
        # Fetch closed and settled pages
        closed = self.list_markets_paginated(status_filter="closed", per_page=500, max_pages=5)
        settled = self.list_markets_paginated(status_filter="settled", per_page=500, max_pages=5)
        all_hist = closed + settled
        # Filter mention-like
        mention_like = _filter_mention_like(all_hist)
        # Optional text filter
        if text_term:
            mention_like = [m for m in mention_like if _contains_term(m, text_term)]
        # Deduplicate by ticker
        by_ticker: Dict[str, Dict[str, Any]] = {}
        for m in mention_like:
            t = m.get("ticker")
            if t and t not in by_ticker:
                by_ticker[t] = m
        return list(by_ticker.values())



