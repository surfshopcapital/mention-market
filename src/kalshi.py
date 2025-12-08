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


class KalshiClient:
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
            # Common values: 'active', 'open', 'closed'; fall back if unsupported
            params["status"] = status_filter
        if cursor:
            params["cursor"] = cursor
        status, data = self._request("GET", "/trade-api/v2/markets", params=params)
        if status != 200:
            raise RuntimeError(f"Kalshi markets request failed: {status} {data}")
        return data

    def find_mention_series_tickers(self) -> List[str]:
        """
        Heuristic to find 'Mention' series. Looks for 'mention' in series title or ticker.
        """
        tickers: List[str] = []
        data = self.list_series(limit=200)
        items = data.get("series", []) or data.get("data", []) or []
        for s in items:
            title = str(s.get("title", "")).lower()
            ticker = str(s.get("ticker", "")).lower()
            if "mention" in title or "mention" in ticker:
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
        Attempts to list markets for 'Mention' series. Falls back to filtering all markets
        by title containing 'mention' if series lookup fails.
        """
        mention_markets: List[Dict[str, Any]] = []
        try:
            series_tickers = self.find_mention_series_tickers()
        except Exception:
            series_tickers = []

        if series_tickers:
            for stkr in series_tickers:
                try:
                    data = self.list_markets(series_ticker=stkr, status_filter="active", limit=200)
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
            if by_ticker:
                return list(by_ticker.values())

        # Fallback: fetch a broad markets page and filter by title text
        try:
            data = self.list_markets(status_filter="active", limit=200)
            all_markets = data.get("markets", []) or data.get("data", []) or []
            filtered = []
            for m in all_markets:
                title = str(m.get("title", "")).lower()
                if "mention" in title:
                    filtered.append(m)
            return filtered
        except Exception:
            return []



