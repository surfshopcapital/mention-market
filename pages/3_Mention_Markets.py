from __future__ import annotations

import hashlib
import json
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from collections import Counter, defaultdict

from src.kalshi import KalshiClient
from src.config import get_kalshi_api_base_url
from src.db import get_session, init_db
from src.storage import add_market_tags, get_market_tags, get_market_tags_bulk
from src.ui_components import inject_dark_theme
from src.data_cache import get_cached_mention_universe


def _to_display_df(markets: list[dict]) -> pd.DataFrame:
    rows = []
    for m in markets:
        if not isinstance(m, dict):
            continue
        rows.append(
            {
                "Title": m.get("title"),
                "Ticker": m.get("ticker"),
                "Series": m.get("series_ticker") or (m.get("series") or {}).get("ticker"),
                "Status": m.get("status"),
                "Yes Bid (¢)": m.get("yes_bid") or m.get("yes_price") or (m.get("orderbook") or {}).get("yes_bid"),
                "Yes Ask (¢)": m.get("yes_ask") or (m.get("orderbook") or {}).get("yes_ask"),
                "No Bid (¢)": m.get("no_bid") or (m.get("orderbook") or {}).get("no_bid"),
                "No Ask (¢)": m.get("no_ask") or (m.get("orderbook") or {}).get("no_ask"),
                "Volume": m.get("volume"),
                "Open Interest": m.get("open_interest"),
                "End Date": m.get("close_time") or m.get("end_date") or m.get("expiry_time"),
            }
        )
    df = pd.DataFrame(rows)
    # Stable column order
    cols = [
        "Title",
        "Ticker",
        "Series",
        "Status",
        "Yes Bid (¢)",
        "Yes Ask (¢)",
        "No Bid (¢)",
        "No Ask (¢)",
        "Volume",
        "Open Interest",
        "End Date",
    ]
    existing_cols = [c for c in cols if c in df.columns]
    return df[existing_cols]


def _load_events_from_cache(cache_bust: int) -> list[dict]:
    uni = get_cached_mention_universe(cache_bust)
    return list(uni.get("events_active") or [])


def _safe_parse_dt(value: object) -> str:
    try:
        ts = pd.to_datetime(value, utc=True, errors="coerce")
        if pd.isna(ts):
            return ""
        # Render friendly, UTC label
        return ts.strftime("%b %d, %Y %H:%M UTC")
    except Exception:
        return str(value or "")


def _derive_description(m: dict) -> str:
    # Prefer explicit fields; fallback to last token of ticker
    for k in ("subtitle", "yes_sub_title", "no_sub_title"):
        v = m.get(k)
        if v:
            return str(v)
    t = str(m.get("ticker", ""))
    if "-" in t:
        return t.split("-")[-1]
    return ""


def _events_to_groups(events: list[dict]) -> list[dict]:
    # Events already grouped; compute display aggregations from nested active markets
    groups = []
    for e in events:
        if not isinstance(e, dict):
            continue
        items = [m for m in (e.get("markets") or []) if isinstance(m, dict)]
        if not items:
            continue
        total_vol = sum(int(m.get("volume") or 0) for m in items)
        end_times = [
            m.get("close_time")
            or m.get("end_date")
            or m.get("expiry_time")
            or m.get("latest_expiration_time")
            for m in items
        ]
        end_iso = None
        end_ts_epoch = None
        try:
            parsed = pd.to_datetime([e for e in end_times if e], utc=True, errors="coerce")
            parsed = [p for p in parsed if not pd.isna(p)]
            if parsed:
                end_latest = max(parsed)
                end_soonest = min(parsed)
                end_iso = end_latest.isoformat()
                end_ts_epoch = int(end_soonest.timestamp())
        except Exception:
            end_iso = None
        disp_title = str(e.get("title") or e.get("event_ticker") or "Event").strip()
        groups.append(
            {
                "event_ticker": e.get("event_ticker"),
                "display_title": disp_title,
                "num_strikes": len(items),
                "total_volume": total_vol,
                "end_date": _safe_parse_dt(end_iso or (end_times[0] if end_times else "")),
                "end_ts": end_ts_epoch if end_ts_epoch is not None else 2**31 - 1,
                "items": items,
            }
        )
    groups.sort(key=lambda g: int(g.get("end_ts") or (2**31 - 1)))
    return groups


@st.cache_data(show_spinner=False)
def _prepare_groups_cached(events_json_key: str, events_payload: list[dict]) -> list[dict]:
    """
    Returns grouped events with:
      - nested markets already active and deduped in client
      - keep only events with > 1 strike
      - totals recomputed
      - sorted by soonest end date
    Cached by the events_json_key to avoid recomputation on selection reruns.
    """
    groups = _events_to_groups(events_payload)
    filtered_groups = []
    for g in groups:
        # Items are already active & deduped by client method; enforce >1 strikes rule here
        if len(g.get("items") or []) <= 1:
            continue
        filtered_groups.append(g)
    # Resort by soonest end date in case values changed
    filtered_groups.sort(key=lambda g: int(g.get("end_ts") or (2**31 - 1)))
    return filtered_groups


def main() -> None:
    st.set_page_config(page_title="Mention Markets", page_icon="💬", layout="wide")
    inject_dark_theme()
    # Ensure DB schema (including market_tags) exists
    try:
        init_db()
    except Exception:
        # If DB is unreachable, continue without tags
        pass
    st.title("Mention Markets")
    st.caption("Live Kalshi mention markets and key stats.")

    with st.sidebar:
        st.subheader("Refresh")
        refresh_sec = st.slider("Auto-refresh interval (seconds)", min_value=0, max_value=600, value=300, step=30)
        _ = st.caption("Set to 0 to disable auto-refresh.")
        manual = st.button("Refresh now", type="primary")
        debug_mode = st.checkbox("Show debug", value=False)

    # Build a cache key that updates when user refreshes or interval elapses
    cache_key = "v1"
    if manual:
        # Force cache bust
        cache_key = f"v1_force_{st.session_state.get('mm_force_ct', 0) + 1}"
        st.session_state["mm_force_ct"] = int(cache_key.split("_")[-1])
    elif refresh_sec > 0:
        # Time-bucketed cache key
        import time as _t

        bucket = int(_t.time() // max(refresh_sec, 1))
        cache_key = f"v1_{bucket}"

    (tab_main,) = st.tabs(["Markets"])

    with tab_main:
        try:
            with st.spinner("Loading mention events..."):
                bust = st.session_state.get("global_cache_bust", 0)
                if manual:
                    bust = int(bust) + 1
                    st.session_state["global_cache_bust"] = bust
                events = _load_events_from_cache(bust)
        except Exception as e:
            st.error(f"Failed to load markets: {e}")
            return

        if not events:
            st.info("No mention events found. Showing a sample of active markets to verify connectivity.")
            try:
                client = KalshiClient()
                any_resp = client.request_debug("GET", "/trade-api/v2/markets", params={"limit": 50, "status": "active"})
                data_obj = any_resp.get("data", {})
                mk_items = []
                if isinstance(data_obj, dict):
                    mk_items = data_obj.get("markets") or data_obj.get("data") or data_obj.get("items") or []
                if not mk_items:
                    mk_items = any_resp.get("markets") or any_resp.get("data") or []
                if isinstance(mk_items, dict):
                    # Normalize only if nested 'markets' exists; otherwise discard
                    if "markets" in mk_items and isinstance(mk_items["markets"], list):
                        mk_items = mk_items["markets"]
                    else:
                        mk_items = []
                if not isinstance(mk_items, list):
                    mk_items = []
                if mk_items:
                    # Build synthetic single-event to help debug rendering
                    events = [
                        {
                            "event_ticker": "SAMPLE",
                            "title": "Sample Active Markets",
                            "markets": [m for m in mk_items if isinstance(m, dict)][:50],
                        }
                    ]
                else:
                    st.warning("Active markets sample request returned no items.")
            except Exception as e:
                st.error(f"Active markets sample error: {e}")

        if events:
            # Cache the grouped (by event) structure by a stable JSON key for speed on reruns
            events_key = hashlib.md5(json.dumps(events, sort_keys=True).encode("utf-8")).hexdigest()
            # Reuse groups from session if payload hasn't changed to make card clicks instantaneous
            if (
                st.session_state.get("mm_groups_key") == events_key
                and isinstance(st.session_state.get("mm_groups"), list)
            ):
                groups = st.session_state["mm_groups"]
            else:
                groups = _prepare_groups_cached(events_key, events)
                st.session_state["mm_groups_key"] = events_key
                st.session_state["mm_groups"] = groups
                st.session_state["mm_group_map"] = {g.get("event_ticker") or g.get("display_title"): g for g in groups}

            # Debug panel
            if debug_mode:
                with st.expander("Debug: Active mention markets"):
                    ev_dicts = [e for e in events if isinstance(e, dict)]
                    total_events = len(ev_dicts)
                    non_dict_entries = len(events) - len(ev_dicts)
                    # Count nested active markets and build distributions
                    active_counts = []
                    status_counts = Counter()
                    cat_counts = Counter()
                    for e in ev_dicts:
                        mkts = [m for m in (e.get("markets") or []) if isinstance(m, dict)]
                        active = [m for m in mkts if str(m.get("status", "")).lower() == "active"]
                        active_counts.append(len(active))
                        for m in active:
                            status_counts.update([str(m.get("status") or "").lower()])
                            cat_counts.update([str(m.get("category") or "").lower()])
                    num_events_gt1 = sum(1 for n in active_counts if n > 1)
                    st.write(
                        {
                            "fetched_events_dicts": total_events,
                            "non_dict_entries": non_dict_entries,
                            "events_with_>1_strikes": num_events_gt1,
                            "events_total": total_events,
                            "status_counts": dict(status_counts),
                            "category_counts": dict(cat_counts),
                        }
                    )
                    if ev_dicts:
                        # Show a small sample of rows
                        sample_rows = []
                        for e in ev_dicts[:5]:
                            sample_rows.append(
                                {
                                    "event_ticker": e.get("event_ticker"),
                                    "title": e.get("title"),
                                    "series_ticker": e.get("series_ticker"),
                                    "num_nested_markets": len(e.get("markets") or []),
                                }
                            )
                        st.caption("Sample events (first 5)")
                        st.dataframe(pd.DataFrame(sample_rows), hide_index=True, use_container_width=True)
                    if len(groups) == 0 and total_events > 0:
                        st.warning("No event cards after grouping. Likely cause: all events have <=1 strike and are filtered out by the UI rule (require >1).")

            # Preload tags for all first tickers (single DB roundtrip, cached in session for 5 minutes)
            tickers_for_tags: list[str] = [g["items"][0].get("ticker") for g in groups if g.get("items")]
            need_reload_tags = False
            now_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)
            if st.session_state.get("mm_tags_loaded_at_ms") is None:
                need_reload_tags = True
            else:
                # reload if older than 5 minutes or manual refresh
                need_reload_tags = (now_ms - int(st.session_state["mm_tags_loaded_at_ms"])) > 5 * 60 * 1000 or manual
            if need_reload_tags:
                try:
                    with get_session() as sess:
                        tags_map = get_market_tags_bulk(sess, tickers_for_tags)
                    st.session_state["mm_tags_map"] = tags_map
                    st.session_state["mm_tags_loaded_at_ms"] = now_ms
                except Exception:
                    st.session_state["mm_tags_map"] = {}
                    st.session_state["mm_tags_loaded_at_ms"] = now_ms

            # Top summary bar (events)
            total_markets = len(groups)  # number of events
            total_volume = sum(int(g["total_volume"]) for g in groups)
            s1, s2 = st.columns(2)
            with s1:
                st.metric("Total events", total_markets)
            with s2:
                st.metric("Total volume", f"{total_volume:,}")

            st.subheader("Markets")
            # Render cards in grid
            cols_per_row = 4
            for i in range(0, len(groups), cols_per_row):
                row = groups[i : i + cols_per_row]
                cols = st.columns(len(row))
                selected_group_in_row = None
                for col, g in zip(cols, row):
                    with col:
                        # Color palette based on time-to-end
                        import time as _t

                        now = int(_t.time())
                        end_ts = int(g.get("end_ts") or now)
                        delta = max(end_ts - now, 0)
                        day = 86400
                        if delta < day:
                            bg = "#e8f5e9"  # green-50
                            border = "#43a047"  # green-600
                        elif delta < 7 * day:
                            bg = "#e3f2fd"  # blue-50
                            border = "#1e88e5"  # blue-600
                        else:
                            bg = "#ffebee"  # red-50
                            border = "#e53935"  # red-600

                        # Styled card
                        st.markdown(
                            f"""
                            <div style="background:{bg};border:1px solid {border};border-radius:10px;padding:12px;margin-bottom:6px;">
                              <div style="font-weight:600;margin-bottom:6px;color:#000;">{g.get('display_title') or g.get('event_ticker')}</div>
                              <div style="display:flex;gap:16px;font-size:12px;color:#000;">
                                <div>Strikes: <b>{g['num_strikes']}</b></div>
                                <div>Volume: <b>{int(g['total_volume']):,}</b></div>
                                <div>End: <b>{g['end_date']}</b></div>
                              </div>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )

                        # Controls row: [View Strikes] [Add tag] [tag input]
                        # Stable group key based on first ticker (fallback to index + title)
                        first_ticker = g["items"][0].get("ticker") if g["items"] else ""
                        group_key = (first_ticker or g.get("event_ticker") or f"{i}_{abs(hash(g.get('display_title', '')))}").replace(" ", "_")
                        ctrl_cols = st.columns([1, 1, 3])
                        with ctrl_cols[0]:
                            if st.button("View strikes", key=f"view_{group_key}"):
                                st.session_state["mm_selected_event"] = g.get("event_ticker") or g.get("display_title")
                                st.session_state["mm_scrolled"] = False
                                st.rerun()
                        with ctrl_cols[1]:
                            apply = st.button("Add tag", key=f"add_tag_{group_key}", disabled=(not first_ticker))
                        with ctrl_cols[2]:
                            tag_val = st.text_input(
                                "Tag",
                                value="",
                                key=f"tag_{group_key}",
                                label_visibility="collapsed",
                                placeholder="Add tag",
                            )

                        # Tag persistence (after controls for layout)
                        existing_tags = list((st.session_state.get("mm_tags_map") or {}).get(str(first_ticker), []))
                        if apply and tag_val.strip() and first_ticker:
                            try:
                                with get_session() as sess:
                                    updated = add_market_tags(sess, str(first_ticker), [tag_val.strip()])
                                existing_tags = updated
                                st.session_state[f"tags_{group_key}"] = updated
                                st.session_state[f"tag_{group_key}"] = ""  # clear input
                                st.success("Tag saved")
                            except Exception:
                                st.warning("Failed to save tag.")
                            # Update in-memory tags map immediately so UI does not wait for refetch
                            current_map = st.session_state.get("mm_tags_map") or {}
                            cur_list = list(current_map.get(str(first_ticker), []))
                            if tag_val.strip() not in cur_list:
                                cur_list.append(tag_val.strip())
                            current_map[str(first_ticker)] = sorted(cur_list)
                            st.session_state["mm_tags_map"] = current_map
                            st.session_state["mm_tags_loaded_at_ms"] = now_ms
                        if st.session_state.get(f"tags_{group_key}"):
                            existing_tags = st.session_state[f"tags_{group_key}"]
                        if existing_tags:
                            st.caption("Tags: " + ", ".join(sorted(existing_tags)))

                        # Mark if this group is selected; table will render full width below the row
                        sel_key = st.session_state.get("mm_selected_event")
                        if sel_key and sel_key == (g.get("event_ticker") or g.get("display_title")):
                            selected_group_in_row = g

                # Full-width strikes table for the selected card in this row
                if selected_group_in_row:
                    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
                    rows = []
                    for m in selected_group_in_row["items"]:
                        rows.append(
                            {
                                "Ticker": m.get("ticker"),
                                "Description": _derive_description(m),
                                "Yes Bid (¢)": m.get("yes_bid"),
                                "Yes Ask (¢)": m.get("yes_ask"),
                                "No Bid (¢)": m.get("no_bid"),
                                "No Ask (¢)": m.get("no_ask"),
                                "Volume": m.get("volume"),
                                "Open Interest": m.get("open_interest"),
                                "End Date": m.get("close_time") or m.get("end_date") or m.get("expiry_time") or m.get("latest_expiration_time"),
                            }
                        )
                    df = pd.DataFrame(rows)
                    if "End Date" in df.columns:
                        df["End Date"] = df["End Date"].apply(_safe_parse_dt)
                    if "Yes Bid (¢)" in df.columns:
                        df = df.sort_values(by="Yes Bid (¢)", ascending=False, na_position="last")
                    st.dataframe(df, width="stretch", hide_index=True)

                    # Played controls per EVENT (persistent)
                    st.caption("Mark event as played and add notes")
                    evt_ticker = str(selected_group_in_row.get("event_ticker") or "")
                    evt_title = str(selected_group_in_row.get("display_title") or evt_ticker or "Event")
                    cols_evt = st.columns([3, 1, 3])
                    with cols_evt[0]:
                        st.write(f"{evt_ticker} – {evt_title}")
                    with cols_evt[1]:
                        if st.checkbox("Played", key=f"played_evt_{evt_ticker}"):
                            try:
                                # Lazy import to avoid hard failure on environments missing migrations
                                from src.storage import upsert_trade_entry  # type: ignore
                                with get_session() as sess:
                                    # Use event_ticker as unique key in trade journal
                                    upsert_trade_entry(
                                        sess,
                                        market_ticker=evt_ticker,
                                        event_ticker=evt_ticker,
                                        title=evt_title,
                                        word="",
                                        note="",
                                    )
                                st.success("Event saved")
                            except Exception:
                                st.warning("Failed to save played event")
                    with cols_evt[2]:
                        note_key_evt = f"note_evt_{evt_ticker}"
                        note_val_evt = st.text_input("Note", key=note_key_evt, label_visibility="collapsed", placeholder="Add event note")
                        if st.button("Save event note", key=f"save_note_evt_{evt_ticker}"):
                            try:
                                from src.storage import set_trade_note  # type: ignore
                                with get_session() as sess:
                                    set_trade_note(sess, evt_ticker, note_val_evt or "")
                                st.success("Note saved")
                            except Exception:
                                st.warning("Failed to save note")

                    strikes: list[str] = []
                    for term in df.get("Description", []).tolist() if "Description" in df.columns else []:
                        t = str(term or "").strip()
                        if t and t not in strikes:
                            strikes.append(t)
                    st.caption("Strikes list")
                    st.markdown(", ".join(strikes) if strikes else "—")

    # Raw data section removed per request


if __name__ == "__main__":
    main()


