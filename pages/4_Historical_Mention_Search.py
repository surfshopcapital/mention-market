from __future__ import annotations

import hashlib
import json
from typing import List, Dict

import pandas as pd
import streamlit as st

from src.kalshi import KalshiClient
from src.db import get_session, init_db
from src.storage import get_market_tags_bulk


def _safe_parse_dt(value: object) -> pd.Timestamp | None:
    try:
        ts = pd.to_datetime(value, utc=True, errors="coerce")
        if pd.isna(ts):
            return None
        return ts
    except Exception:
        return None


def _derive_description(m: dict) -> str:
    for k in ("subtitle", "yes_sub_title", "no_sub_title"):
        v = m.get(k)
        if v:
            return str(v)
    t = str(m.get("ticker", ""))
    if "-" in t:
        return t.split("-")[-1]
    return ""


def _group_by_title(markets: List[dict]) -> List[dict]:
    by_title: Dict[str, List[dict]] = {}
    for m in markets:
        title = str(m.get("title", "")).strip() or str(m.get("event_ticker") or m.get("ticker") or "Unknown")
        by_title.setdefault(title, []).append(m)
    groups: List[dict] = []
    for title, items in by_title.items():
        vol = sum(int(m.get("volume") or 0) for m in items)
        all_times = [
            m.get("close_time") or m.get("end_date") or m.get("expiry_time") or m.get("latest_expiration_time")
            for m in items
        ]
        parsed = [t for t in (_safe_parse_dt(x) for x in all_times) if t is not None]
        last_ts = max(parsed) if parsed else None
        groups.append(
            {
                "title": title,
                "items": items,
                "total_volume": vol,
                "last_ts": last_ts,
            }
        )
    # Sort by last market time desc
    groups.sort(key=lambda g: (g["last_ts"] or pd.Timestamp(0, tz="UTC")), reverse=True)
    return groups


@st.cache_data(show_spinner=False, ttl=900)
def _fetch_history(term: str) -> List[dict]:
    client = KalshiClient()
    return client.list_mention_markets_historical(text_term=term)


def main() -> None:
    st.set_page_config(page_title="Historical Mention Search", page_icon="🕰️", layout="wide")
    try:
        init_db()
    except Exception:
        pass

    st.title("Historical Mention Search")
    st.caption("Search past mention-style markets by text or tag.")

    left, right = st.columns([2, 1])
    with left:
        q = st.text_input("Search text (title/series/ticker contains)", value="", placeholder="e.g., bessent, trump")
    with right:
        tag_q = st.text_input("Filter by tag (optional)", value="", placeholder="e.g., earnings")
    top_controls = st.columns([1, 1, 2])
    with top_controls[0]:
        manual = st.button("Search / Refresh", type="primary", use_container_width=True)
    with top_controls[1]:
        ttl_min = st.selectbox("Cache TTL", options=[5, 15, 60], index=0, help="Minutes to cache results")

    # Compute cache key
    ttl_seconds = int(ttl_min) * 60
    query_key = f"v1_hist_{q.strip().lower()}_{ttl_seconds}"
    if manual:
        query_key = f"{query_key}_{st.session_state.get('hist_force', 0) + 1}"
        st.session_state["hist_force"] = int(query_key.split("_")[-1])

    with st.spinner("Searching historical markets..."):
        data = _fetch_history(q.strip().lower())
    groups = _group_by_title(data)

    # Bulk tag fetch for first tickers of each group
    tickers = [g["items"][0].get("ticker") for g in groups if g.get("items")]
    tags_map: dict[str, List[str]] = {}
    try:
        with get_session() as sess:
            tags_map = get_market_tags_bulk(sess, tickers)
    except Exception:
        tags_map = {}

    # Tag filter if provided
    if tag_q.strip():
        needle = tag_q.strip().lower()
        def group_has_tag(g: dict) -> bool:
            first_t = str(g["items"][0].get("ticker"))
            tags = [t.lower() for t in tags_map.get(first_t, [])]
            return any(needle in t for t in tags)
        groups = [g for g in groups if group_has_tag(g)]

    # Summary bar
    num_markets = len(groups)
    total_volume = sum(int(g["total_volume"]) for g in groups)
    last_dates = [g["last_ts"] for g in groups if g["last_ts"] is not None]
    last_date_str = max(last_dates).strftime("%b %d, %Y %H:%M UTC") if last_dates else "—"
    s1, s2, s3 = st.columns(3)
    with s1:
        st.metric("Markets", num_markets)
    with s2:
        st.metric("Total volume", f"{total_volume:,}")
    with s3:
        st.metric("Last market", last_date_str)

    st.divider()
    st.subheader("Results")

    # Render small cards 8 per row (title + final volume)
    cols_per_row = 8
    for i in range(0, len(groups), cols_per_row):
        row = groups[i : i + cols_per_row]
        cols = st.columns(len(row))
        selected_group_in_row = None
        for col, g in zip(cols, row):
            with col:
                st.markdown(
                    f"""
                    <div style="background:#f8f9fa;border:1px solid #e0e0e0;border-radius:10px;padding:10px;margin-bottom:6px;">
                      <div style="font-weight:600;margin-bottom:6px;line-height:1.2">{g['title']}</div>
                      <div style="font-size:12px;color:#555;">Final volume: <b>{int(g['total_volume']):,}</b></div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                first_ticker = g["items"][0].get("ticker") if g["items"] else ""
                group_key = (first_ticker or f"{i}_{abs(hash(g['title']))}").replace(" ", "_")
                if st.button("View", key=f"hist_view_{group_key}"):
                    st.session_state["hist_selected_title"] = g["title"]
                    st.rerun()
                # Show tags
                existing_tags = tags_map.get(str(first_ticker), [])
                if existing_tags:
                    st.caption("Tags: " + ", ".join(sorted(existing_tags)))
                if st.session_state.get("hist_selected_title") == g["title"]:
                    selected_group_in_row = g

        if selected_group_in_row:
            rows = []
            for m in selected_group_in_row["items"]:
                rows.append(
                    {
                        "Subtitle": _derive_description(m),
                        "Final volume": m.get("volume"),
                        "Result": m.get("result"),
                        "End": pd.to_datetime(
                            m.get("close_time") or m.get("end_date") or m.get("expiry_time") or m.get("latest_expiration_time"),
                            utc=True,
                            errors="coerce",
                        ),
                        "Ticker": m.get("ticker"),
                    }
                )
            df = pd.DataFrame(rows)
            if "End" in df.columns:
                df["End"] = df["End"].dt.strftime("%b %d, %Y %H:%M UTC")
            st.dataframe(df[["Subtitle", "Final volume", "Result", "End", "Ticker"]], use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()


