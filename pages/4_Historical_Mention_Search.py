from __future__ import annotations

from typing import List, Dict

import pandas as pd
import streamlit as st
from collections import Counter, defaultdict

from src.kalshi import KalshiClient
from src.db import get_session, init_db
from src.storage import get_market_tags_bulk
from src.ui_components import inject_dark_theme


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


@st.cache_data(show_spinner=False, ttl=1200)
def _bootstrap_events(months: int, cache_key: str) -> List[dict]:
    """
    Preload mention-like events within window using Events API with event-level status filters.
    cache_key lets us force refresh when the user clicks Search/Refresh.
    """
    client = KalshiClient()
    try:
        return client.list_mention_events_window_events_api(months=months, statuses=["closed", "settled", "determined"])
    except AttributeError:
        # Fallback: build from Events API directly in-page (identical logic)
        import pandas as _pd
        earliest_ts = int((_pd.Timestamp.utcnow() - _pd.Timedelta(days=30 * max(months, 1))).timestamp())
        latest_ts = int(_pd.Timestamp.utcnow().timestamp())
        statuses = ["closed", "settled", "determined"]
        collected: List[dict] = []
        for s in statuses:
            try:
                evs = client.list_events_paginated(
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
        # mention-like filter
        filtered: List[dict] = []
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
        # dedupe
        by_evt: Dict[str, dict] = {}
        for e in filtered:
            t = e.get("event_ticker")
            if t and t not in by_evt:
                by_evt[t] = e
        return list(by_evt.values())

@st.cache_data(show_spinner=False, ttl=300)
def _fetch_history(term: str, months: int, include_closed: bool, cache_key: str) -> List[dict]:
    # Filter within preloaded events; then flatten to markets (no additional market-status filtering)
    all_events = _bootstrap_events(months, cache_key)
    needle = (term or "").strip().lower()
    markets: List[dict] = []
    for e in all_events:
        mkts = [m for m in (e.get("markets") or []) if isinstance(m, dict)]
        # Require: event title contains the search term (if provided)
        if needle:
            title_text = str(e.get("title", "")).lower()
            if needle not in title_text:
                continue
        # Exclude small events: only include if event has more than 2 markets (strikes)
        if len(mkts) <= 2:
            continue
        markets.extend(mkts)
    return markets

@st.cache_data(show_spinner=False, ttl=180)
def _fetch_recent_closed_events(limit: int = 12) -> List[dict]:
    client = KalshiClient()
    return client.list_mention_events_closed_recent(limit=limit)


def main() -> None:
    st.set_page_config(page_title="Historical Mention Search", page_icon="🕰️", layout="wide")
    inject_dark_theme()
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
        manual = st.button("Search / Refresh", type="primary")
    with top_controls[1]:
        months = st.selectbox("Lookback (months)", options=[3, 6, 12], index=0)
    include_closed = st.checkbox("Include closed (no final result yet)", value=False)

    # Default: If no query or tag, show recent closed mention markets (cards; 6 per row)
    # Build refresh-aware cache key
    cache_key = "v1"
    if manual:
        cache_key = f"v1_force_{int(pd.Timestamp.utcnow().timestamp())}"

    if not (q.strip() or tag_q.strip()):
        st.subheader("Recent closed mention events (last month)")
        try:
            # Always use a 1-month window for recent events snapshot
            evs = _bootstrap_events(1, cache_key)
            # Filter to allowed statuses and sort by latest end
            allowed = {"closed", "settled", "determined"}
            def to_latest_ts(e: dict) -> int:
                ts_list = []
                for m in (e.get("markets") or []):
                    if str(m.get("status","")).lower() not in allowed:
                        continue
                    t = m.get("close_time") or m.get("end_date") or m.get("expiry_time") or m.get("latest_expiration_time")
                    ts = pd.to_datetime(t, utc=True, errors="coerce")
                    if ts is not None and not pd.isna(ts):
                        ts_list.append(int(ts.timestamp()))
                return max(ts_list) if ts_list else 0
            evs_sorted = sorted(evs, key=to_latest_ts, reverse=True)
            # Keep only events that have at least one allowed-status market
            recent_events = []
            for e in evs_sorted:
                mkts = [m for m in (e.get("markets") or []) if isinstance(m, dict) and str(m.get("status","")).lower() in allowed]
                if mkts:
                    recent_events.append({**e, "markets": mkts})
                if len(recent_events) >= 12:
                    break
        except Exception as e:
            st.error(f"Failed to fetch recent closed: {e}")
            return
        if not recent_events:
            st.info("No recent closed mention events found.")
            return
        # Cards 6 per row
        cols_per_row = 6
        for i in range(0, len(recent_events), cols_per_row):
            row = recent_events[i : i + cols_per_row]
            cols = st.columns(len(row))
            for c, e in zip(cols, row):
                with c:
                    mkts = [m for m in (e.get("markets") or []) if isinstance(m, dict)]
                    # derive latest end across markets
                    end_disp = "—"
                    end_epoch = None
                    if mkts:
                        parsed = [ _safe_parse_dt(m.get("close_time") or m.get("end_date") or m.get("expiry_time") or m.get("latest_expiration_time")) for m in mkts ]
                        parsed = [p for p in parsed if p is not None]
                        end_ts = max(parsed) if parsed else None
                        if end_ts is not None:
                            end_disp = end_ts.strftime("%b %d, %Y %H:%M UTC")
                            end_epoch = int(end_ts.timestamp())
                    statuses = dict(pd.Series([str(m.get("status","")).lower() for m in mkts]).value_counts()) if mkts else {}

                    # palette: green (<1w), blue (<1m), red (older)
                    import time as _t
                    now = int(_t.time())
                    day = 86400
                    if end_epoch is not None and (now - end_epoch) < 7 * day:
                        bg = "#e8f5e9"; border = "#43a047"  # green
                    elif end_epoch is not None and (now - end_epoch) < 30 * day:
                        bg = "#e3f2fd"; border = "#1e88e5"  # blue
                    else:
                        bg = "#ffebee"; border = "#e53935"  # red

                    st.markdown(
                        f"""
                        <div style="background:{bg};border:1px solid {border};border-radius:10px;padding:10px;margin-bottom:6px;">
                          <div style="font-weight:600;margin-bottom:6px;line-height:1.2;color:#000">{str(e.get("title") or e.get("event_ticker") or "")}</div>
                          <div style="font-size:12px;color:#000;line-height:1.4">
                            <div>Event: <b>{e.get("event_ticker")}</b></div>
                            <div>Markets: <b>{len(mkts)}</b></div>
                            <div>Statuses: <b>{statuses}</b></div>
                            <div>End: <b>{end_disp}</b></div>
                          </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                    # View button for nested markets
                    if st.button("View", key=f"recent_view_{e.get('event_ticker') or i}"):
                        st.session_state["hist_selected_recent_event"] = e.get("event_ticker") or str(i)
        # If a recent event is selected, render nested markets table
        sel_evt = st.session_state.get("hist_selected_recent_event")
        if sel_evt:
            match = None
            for e in recent_events:
                if (e.get("event_ticker") or "") == sel_evt or (str(sel_evt) == str(recent_events.index(e))):
                    match = e
                    break
            if match:
                rows = []
                for m in [mm for mm in (match.get("markets") or []) if isinstance(mm, dict)]:
                    res = (m.get("result") or "").strip()
                    res_disp = res.upper() if res else ""
                    said = "Said" if res_disp == "YES" else ("Not said" if res_disp == "NO" else "")
                    rows.append(
                        {
                            "Word": (m.get("subtitle") or m.get("yes_sub_title") or m.get("no_sub_title") or ""),
                            "Final volume": m.get("volume"),
                            "Result": res_disp,
                            "Said?": said,
                            "End": pd.to_datetime(m.get("close_time") or m.get("end_date") or m.get("expiry_time") or m.get("latest_expiration_time"), utc=True, errors="coerce"),
                            "Ticker": m.get("ticker"),
                        }
                    )
                df = pd.DataFrame(rows)
                if "End" in df.columns:
                    df["End"] = df["End"].dt.strftime("%b %d, %Y %H:%M UTC")
                cols = [c for c in ["Word", "Final volume", "Result", "Said?", "End", "Ticker"] if c in df.columns]
                st.dataframe(df[cols], width="stretch", hide_index=True)
        return

    with st.spinner("Searching historical markets..."):
        try:
            data = _fetch_history(q.strip().lower(), int(months), include_closed, cache_key)
        except Exception as e:
            st.error(f"Failed to fetch history: {e}")
            return
    hist_dicts = [m for m in data if isinstance(m, dict)]
    # Fallback to markets-based final/hybrid fetch if events route returned nothing
    if not hist_dicts:
        try:
            client = KalshiClient()
            fallback = client.list_mention_markets_historical(text_term=q.strip().lower(), months=int(months), include_closed=include_closed)
            hist_dicts = [m for m in fallback if isinstance(m, dict)]
            # Informational note removed (debug disabled by default)
        except Exception:
            pass
    groups = _group_by_event(hist_dicts)


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

    # Render small cards 8 per row (title + final volume + end date) with palette
    cols_per_row = 8
    for i in range(0, len(groups), cols_per_row):
        row = groups[i : i + cols_per_row]
        cols = st.columns(len(row))
        selected_group_in_row = None
        for col, g in zip(cols, row):
            with col:
                end_disp = g["last_ts"].strftime("%b %d, %Y %H:%M UTC") if g.get("last_ts") is not None else "—"
                # palette: green (<1w), blue (<1m), red (older)
                import time as _t
                now = int(_t.time())
                day = 86400
                end_epoch = int(g["last_ts"].timestamp()) if g.get("last_ts") is not None else None
                if end_epoch is not None and (now - end_epoch) < 7 * day:
                    bg = "#e8f5e9"; border = "#43a047"
                elif end_epoch is not None and (now - end_epoch) < 30 * day:
                    bg = "#e3f2fd"; border = "#1e88e5"
                else:
                    bg = "#ffebee"; border = "#e53935"
                st.markdown(
                    f"""
                    <div style="background:{bg};border:1px solid {border};border-radius:10px;padding:10px;margin-bottom:6px;">
                      <div style="font-weight:600;margin-bottom:6px;line-height:1.2;color:#000">{g.get('display_title') or g.get('event_ticker')}</div>
                      <div style="font-size:12px;color:#000;">Final volume: <b>{int(g['total_volume']):,}</b></div>
                      <div style="font-size:12px;color:#000;">End: <b>{end_disp}</b></div>
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
                    st.dataframe(raw_df, hide_index=True, width="stretch")
                except Exception:
                    st.write(selected_group_in_row["items"])

    # Strike summary across the filtered set
    st.divider()
    st.subheader("Strike summary")
    try:
        # Limit to events currently included (after optional tag filtering)
        included_events = {g.get("event_ticker") for g in groups if g.get("event_ticker")}
        summary_markets = [m for m in hist_dicts if str(m.get("event_ticker") or "") in included_events] if included_events else list(hist_dicts)

        by_word: Dict[str, List[dict]] = {}
        for m in summary_markets:
            word = _derive_description(m)
            if not word:
                continue
            by_word.setdefault(word, []).append(m)

        # Filter controls for recency buckets
        bucket_choice = st.radio("Filter by recency", options=["All", "Green (<1w)", "Blue (<1m)", "Red (older)"], horizontal=True, index=0)
        import time as _t
        now = int(_t.time())
        day = 86400
        def in_bucket(ts: pd.Timestamp | None) -> bool:
            if bucket_choice == "All" or ts is None:
                return bucket_choice == "All"
            epoch = int(ts.timestamp())
            delta = now - epoch
            if bucket_choice.startswith("Green"):
                return delta < 7 * day
            if bucket_choice.startswith("Blue"):
                return delta < 30 * day and delta >= 7 * day
            if bucket_choice.startswith("Red"):
                return delta >= 30 * day
            return True

        rows = []
        for word, items in by_word.items():
            total = len(items)
            said_count = 0
            vol_sum = 0
            most_recent_ts = None
            for m in items:
                res = (m.get("result") or "").strip().upper()
                if res == "YES":
                    said_count += 1
                vol_sum += int(m.get("volume") or 0)
                ts = _safe_parse_dt(m.get("close_time") or m.get("end_date") or m.get("expiry_time") or m.get("latest_expiration_time"))
                if ts is not None and (most_recent_ts is None or ts > most_recent_ts):
                    most_recent_ts = ts
            if bucket_choice != "All" and not in_bucket(most_recent_ts):
                continue
            pct = (said_count / total * 100.0) if total else 0.0
            avg_vol = (vol_sum / total) if total else 0.0
            rows.append(
                {
                    "Strike (word)": word,
                    "Times said": said_count,
                    "Events possible": total,
                    "% said": round(pct, 2),
                    "Average volume": int(avg_vol),
                    "Most recent end": most_recent_ts.strftime("%b %d, %Y %H:%M UTC") if most_recent_ts is not None else "—",
                }
            )
        if rows:
            # Sort with Events Possible desc first as requested
            df_sum = pd.DataFrame(rows).sort_values(by=["Events possible", "Times said"], ascending=[False, False])
            st.dataframe(df_sum, width="stretch", hide_index=True)
        else:
            st.info("No strikes found for the current filters.")
    except Exception:
        st.info("Summary unavailable for current selection.")


if __name__ == "__main__":
    main()


