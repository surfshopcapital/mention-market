from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

from .kalshi import KalshiClient


@st.cache_data(show_spinner=False, ttl=900)
def get_cached_mention_universe(cache_bust: int = 0) -> Dict[str, object]:
	"""
	Load and cache the entire mention universe for speed across pages.
	Returns:
	  {
	    "events_active": List[dict],
	    "events_hist": List[dict],
	    "all_markets": List[dict],
	    "generated_at": iso string
	  }
	TTL: 15 minutes. Pass a different cache_bust to force refresh.
	"""
	client = KalshiClient()

	# Active mention events (with nested markets filtered to active)
	try:
		events_active = client.list_mention_events_active()
	except Exception:
		events_active = []

	# Historical mention events across last 12 months (closed/settled/determined)
	try:
		events_hist = client.list_mention_events_window_events_api(months=12, statuses=["closed", "settled", "determined"])
	except Exception:
		# Fallback: build from markets window if events API route is unavailable at runtime
		try:
			mkts = client.list_mention_markets_window(months=12, statuses=["closed", "settled", "determined"])
			by_event: Dict[str, List[dict]] = {}
			for m in mkts:
				ev = str(m.get("event_ticker") or "") or str(m.get("title") or "")
				by_event.setdefault(ev, []).append(m)
			events_hist = []
			for ev_ticker, items in by_event.items():
				disp = str((items[0] or {}).get("title") or ev_ticker or "Event")
				events_hist.append({"event_ticker": ev_ticker, "title": disp, "markets": items})
		except Exception:
			events_hist = []

	# Flatten all markets (active + hist) for convenience
	all_markets: List[dict] = []
	for e in events_active:
		for m in (e.get("markets") or []):
			if isinstance(m, dict):
				all_markets.append(m)
	for e in events_hist:
		for m in (e.get("markets") or []):
			if isinstance(m, dict):
				all_markets.append(m)

	return {
		"events_active": events_active,
		"events_hist": events_hist,
		"all_markets": all_markets,
		"generated_at": pd.Timestamp.utcnow().isoformat(),
	}


