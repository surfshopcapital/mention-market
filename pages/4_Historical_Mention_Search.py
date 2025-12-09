from __future__ import annotations

from typing import List, Dict

import pandas as pd
import streamlit as st
from collections import Counter, defaultdict

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


def _group_by_event(markets: List[dict]) -> List[dict]:
    # Group closed/settled/determined by event
    by_event: Dict[str, List[dict]] = {}
    for m in markets:
        event_ticker = str(m.get("event_ticker") or "").strip()
        if not event_ticker:
            event_ticker = str(m.get("title") or m.get("ticker") or "Unknown")
        by_event.setdefault(event_ticker, []).append(m)
    groups: List[dict] = []
    for event_ticker, items in by_event.items():
        vol = sum(int(m.get("volume") or 0) for m in items)
        all_times = [
            m.get("close_time") or m.get("end_date") or m.get("expiry_time") or m.get("latest_expiration_time")
            for m in items
        ]
        parsed = [t for t in (_safe_parse_dt(x) for x in all_times) if t is not None]
        last_ts = max(parsed) if parsed else None
        disp_title = str((items[0] or {}).get("title") or event_ticker or "Event").strip()
        groups.append(
            {
                "event_ticker": event_ticker,
                "display_title": disp_title,
                "items": items,
                "total_volume": vol,
                "last_ts": last_ts,
            }
        )
    # Sort by last market time desc
    groups.sort(key=lambda g: (g["last_ts"] or pd.Timestamp(0, tz="UTC")), reverse=True)
    return groups


@st.cache_data(show_spinner=False, ttl=300)
def _fetch_history(term: str, months: int, include_closed: bool) -> List[dict]:
    client = KalshiClient()
    # Use events-based not-active fetch for broader recall, then flatten to markets
    evs = client.list_mention_events_not_active(text_term=term, months=months, include_closed=include_closed)
    markets: List[dict] = []
    for e in evs:
        for m in e.get("markets") or []:
            if isinstance(m, dict):
                markets.append(m)
    return markets

@st.cache_data(show_spinner=False, ttl=180)
def _fetch_recent_closed(limit: int = 12) -> List[dict]:
    client = KalshiClient()
    return client.list_mention_markets_closed_recent(limit=limit)


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
    top_controls = st.columns([1, 2])
    with top_controls[0]:
        manual = st.button("Search / Refresh", type="primary", use_container_width=True)
    with top_controls[1]:
        months = st.selectbox("Lookback (months)", options=[3, 6, 12], index=2)
    debug_mode = st.checkbox("Show debug", value=False)
    include_closed = st.checkbox("Include closed (no final result yet)", value=False)

    # Default: If no query or tag, show recent closed mention markets (cards; 6 per row)
    if not (q.strip() or tag_q.strip()):
        st.subheader("Recent closed mention markets")
        try:
            recent = _fetch_recent_closed(limit=12)
        except Exception as e:
            st.error(f"Failed to fetch recent closed: {e}")
            return
        if debug_mode:
            with st.expander("Debug: Recent-closed mention markets"):
                st.write({"count": len(recent)})
                if recent:
                    sample_cols = ["ticker", "event_ticker", "series_ticker", "title", "status", "close_time", "result"]
                    st.dataframe(pd.DataFrame([{k: m.get(k) for k in sample_cols} for m in recent[:12]]), hide_index=True, use_container_width=True)
        if not recent:
            st.info("No recent closed mention markets found.")
            return
        # Cards 6 per row
        cols_per_row = 6
        for i in range(0, len(recent), cols_per_row):
            row = recent[i : i + cols_per_row]
            cols = st.columns(len(row))
            for c, m in zip(cols, row):
                with c:
                    end_ts = _safe_parse_dt(m.get("close_time") or m.get("end_date") or m.get("expiry_time") or m.get("latest_expiration_time"))
                    end_disp = end_ts.strftime("%b %d, %Y %H:%M UTC") if end_ts is not None else "—"
                    said = ""
                    res = (m.get("result") or "").strip()
                    res_disp = res.upper() if res else ""
                    if res_disp == "YES":
                        said = "Said"
                    elif res_disp == "NO":
                        said = "Not said"
                    st.markdown(
                        f"""
                        <div style="background:#fff;border:1px solid #e0e0e0;border-radius:10px;padding:10px;margin-bottom:6px;">
                          <div style="font-weight:600;margin-bottom:6px;line-height:1.2">{str(m.get("title") or m.get("ticker") or "")}</div>
                          <div style="font-size:12px;color:#555;line-height:1.4">
                            <div>Word: <b>{_derive_description(m)}</b></div>
                            <div>Status: <b>{str(m.get("status") or "").lower()}</b></div>
                            <div>Result: <b>{res_disp or "—"}</b> {("("+said+")") if said else ""}</div>
                            <div>End: <b>{end_disp}</b></div>
                          </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
        return

    with st.spinner("Searching historical markets..."):
        try:
            data = _fetch_history(q.strip().lower(), int(months), include_closed)
        except Exception as e:
            st.error(f"Failed to fetch history: {e}")
            return
    hist_dicts = [m for m in data if isinstance(m, dict)]
    groups = _group_by_event(hist_dicts)

    if debug_mode:
        with st.expander("Debug: Historical mention markets"):
            total_fetched = len(hist_dicts)
            non_dict_entries = len(data) - len(hist_dicts)
            uniq_series = len({str(m.get("series_ticker") or "") for m in hist_dicts})
            uniq_events = len({str(m.get("event_ticker") or "") for m in hist_dicts})
            status_counts = Counter([str(m.get("status") or "").lower() for m in hist_dicts])
            res_counts = Counter([str((m.get("result") or "")).lower() for m in hist_dicts])
            ev_to_tickers = defaultdict(set)
            for m in hist_dicts:
                ev = str(m.get("event_ticker") or "")
                if not ev:
                    ev = str(m.get("title") or m.get("ticker") or "Unknown")
                t = m.get("ticker")
                if t:
                    ev_to_tickers[ev].add(t)
            ev_sizes = sorted([(ev, len(tks)) for ev, tks in ev_to_tickers.items()], key=lambda x: x[1], reverse=True)
            num_events_gt1 = sum(1 for _, n in ev_sizes if n > 1)
            st.write(
                {
                    "fetched_markets_dicts": total_fetched,
                    "non_dict_entries": non_dict_entries,
                    "unique_series": uniq_series,
                    "unique_events": uniq_events,
                    "status_counts": dict(status_counts),
                    "result_counts": dict(res_counts),
                    "events_with_>1_strikes": num_events_gt1,
                    "events_total": len(ev_sizes),
                    "groups_count": len(groups),
                }
            )
            if hist_dicts:
                sample_cols = ["ticker", "event_ticker", "series_ticker", "title", "status", "result", "close_time"]
                sample_rows = []
                for m in hist_dicts[:10]:
                    sample_rows.append({k: m.get(k) for k in sample_cols})
                st.caption("Sample historical markets (first 10)")
                st.dataframe(pd.DataFrame(sample_rows), hide_index=True, use_container_width=True)

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
    num_markets = len(groups)  # events count
    total_volume = sum(int(g["total_volume"]) for g in groups)
    last_dates = [g["last_ts"] for g in groups if g["last_ts"] is not None]
    last_date_str = max(last_dates).strftime("%b %d, %Y %H:%M UTC") if last_dates else "—"
    s1, s2, s3 = st.columns(3)
    with s1:
        st.metric("Events", num_markets)
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
                      <div style="font-weight:600;margin-bottom:6px;line-height:1.2">{g.get('display_title') or g.get('event_ticker')}</div>
                      <div style="font-size:12px;color:#555;">Final volume: <b>{int(g['total_volume']):,}</b></div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                first_ticker = g["items"][0].get("ticker") if g["items"] else ""
                group_key = (first_ticker or g.get("event_ticker") or f"{i}_{abs(hash(g.get('display_title', '')))}").replace(" ", "_")
                if st.button("View", key=f"hist_view_{group_key}"):
                    st.session_state["hist_selected_event"] = g.get("event_ticker") or g.get("display_title")
                    st.rerun()
                # Show tags
                existing_tags = tags_map.get(str(first_ticker), [])
                if existing_tags:
                    st.caption("Tags: " + ", ".join(sorted(existing_tags)))
                sel = st.session_state.get("hist_selected_event")
                if sel and sel == (g.get("event_ticker") or g.get("display_title")):
                    selected_group_in_row = g

        if selected_group_in_row:
            rows = []
            for m in selected_group_in_row["items"]:
                # Map result to upper-case YES/NO when present
                res = (m.get("result") or "").strip()
                res_disp = res.upper() if res else ""
                said = ""
                if res_disp == "YES":
                    said = "Said"
                elif res_disp == "NO":
                    said = "Not said"
                rows.append(
                    {
                        "Word": _derive_description(m),
                        "Final volume": m.get("volume"),
                        "Result": res_disp,
                        "Said?": said,
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
            cols = [c for c in ["Word", "Final volume", "Result", "Said?", "End", "Ticker"] if c in df.columns]
            st.dataframe(df[cols], width="stretch", hide_index=True)
            # Raw fields for this group (pull all data for each ticker)
            with st.expander("Raw fields (selected event tickers)"):
                try:
                    raw_df = pd.json_normalize(selected_group_in_row["items"])
                    st.dataframe(raw_df, hide_index=True, use_container_width=True)
                except Exception:
                    st.write(selected_group_in_row["items"])


if __name__ == "__main__":
    main()


