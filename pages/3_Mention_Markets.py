from __future__ import annotations

import hashlib
import json
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from src.kalshi import KalshiClient
from src.config import get_kalshi_api_base_url


def _to_display_df(markets: list[dict]) -> pd.DataFrame:
    rows = []
    for m in markets:
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


@st.cache_data(show_spinner=False)
def _fetch_mention_markets_cached(cache_key: str) -> list[dict]:
    client = KalshiClient()
    return client.list_mention_markets()


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


def _group_by_title(markets: list[dict]) -> list[dict]:
    by_title: dict[str, list[dict]] = {}
    for m in markets:
        title = str(m.get("title", "")).strip()
        if not title:
            # If title missing, group by event_ticker as fallback
            title = str(m.get("event_ticker") or m.get("ticker") or "Unknown").strip()
        by_title.setdefault(title, []).append(m)
    groups = []
    for title, items in by_title.items():
        total_vol = sum(int(m.get("volume") or 0) for m in items)
        # Use the latest close_time among strikes
        end_times = [m.get("close_time") or m.get("end_date") or m.get("expiry_time") for m in items]
        # Choose max if available
        end_iso = None
        end_ts_epoch = None
        try:
            parsed = pd.to_datetime([e for e in end_times if e], utc=True, errors="coerce")
            parsed = [p for p in parsed if not pd.isna(p)]
            if parsed:
                # Latest end used for display; we will also store earliest for sorting
                end_latest = max(parsed)
                end_soonest = min(parsed)
                end_iso = end_latest.isoformat()
                end_ts_epoch = int(end_soonest.timestamp())
        except Exception:
            end_iso = None
        groups.append(
            {
                "title": title,
                "num_strikes": len(items),
                "total_volume": total_vol,
                "end_date": _safe_parse_dt(end_iso or (end_times[0] if end_times else "")),
                "end_ts": end_ts_epoch if end_ts_epoch is not None else 2**31 - 1,
                "items": items,
            }
        )
    # Sort by soonest end date first
    groups.sort(key=lambda g: int(g.get("end_ts") or (2**31 - 1)))
    return groups


@st.cache_data(show_spinner=False)
def _prepare_groups_cached(markets_json_key: str, markets_payload: list[dict]) -> list[dict]:
    """
    Returns grouped markets with:
      - unique strikes per group (dedup by ticker)
      - groups with > 1 strike only
      - totals recomputed
      - sorted by soonest end date
    Cached by the markets_json_key to avoid recomputation on selection reruns.
    """
    groups = _group_by_title(markets_payload)
    filtered_groups = []
    for g in groups:
        by_ticker = {}
        for m in g["items"]:
            t = m.get("ticker")
            if t and t not in by_ticker:
                by_ticker[t] = m
        unique_items = list(by_ticker.values())
        if len(unique_items) <= 1:
            continue
        g_clean = {
            **g,
            "items": unique_items,
            "num_strikes": len(unique_items),
            "total_volume": sum(int(m.get("volume") or 0) for m in unique_items),
        }
        filtered_groups.append(g_clean)
    # Resort by soonest end date in case values changed
    filtered_groups.sort(key=lambda g: int(g.get("end_ts") or (2**31 - 1)))
    return filtered_groups


def main() -> None:
    st.set_page_config(page_title="Mention Markets", page_icon="💬", layout="wide")
    st.title("Mention Markets")
    st.caption("Live Kalshi mention markets and key stats.")

    with st.sidebar:
        st.subheader("Refresh")
        refresh_sec = st.slider("Auto-refresh interval (seconds)", min_value=0, max_value=300, value=60, step=15)
        _ = st.caption("Set to 0 to disable auto-refresh.")
        manual = st.button("Refresh now", type="primary", use_container_width=True)

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
            with st.spinner("Loading mention markets..."):
                markets = _fetch_mention_markets_cached(cache_key)
        except Exception as e:
            st.error(f"Failed to load markets: {e}")
            return

        if not markets:
            st.info("No mention markets found. Showing a sample of active markets to verify connectivity.")
            try:
                client = KalshiClient()
                any_resp = client.request_debug("GET", "/trade-api/v2/markets", params={"limit": 50, "status": "open"})
                mk_items = any_resp.get("data", {}).get("markets") or any_resp.get("markets") or any_resp.get("data") or []
                if isinstance(mk_items, dict) and "markets" in mk_items:
                    mk_items = mk_items["markets"]
                if mk_items:
                    markets = list(mk_items)[:50]
                else:
                    st.warning("Active markets sample request returned no items.")
            except Exception as e:
                st.error(f"Active markets sample error: {e}")

        if markets:
            # Cache the grouped structure by a stable JSON key for speed on reruns
            markets_key = hashlib.md5(json.dumps(markets, sort_keys=True).encode("utf-8")).hexdigest()
            # Reuse groups from session if payload hasn't changed to make card clicks instantaneous
            if (
                st.session_state.get("mm_groups_key") == markets_key
                and isinstance(st.session_state.get("mm_groups"), list)
            ):
                groups = st.session_state["mm_groups"]
            else:
                groups = _prepare_groups_cached(markets_key, markets)
                st.session_state["mm_groups_key"] = markets_key
                st.session_state["mm_groups"] = groups
                st.session_state["mm_group_map"] = {g["title"]: g for g in groups}

            # Top summary bar
            total_markets = len(groups)
            total_volume = sum(int(g["total_volume"]) for g in groups)
            s1, s2 = st.columns(2)
            with s1:
                st.metric("Total markets", total_markets)
            with s2:
                st.metric("Total volume", f"{total_volume:,}")

            st.subheader("Markets")
            # Render cards in grid
            cols_per_row = 4
            for i in range(0, len(groups), cols_per_row):
                row = groups[i : i + cols_per_row]
                cols = st.columns(len(row))
                for col, g in zip(cols, row):
                    with col:
                        card = st.container(border=True)
                        with card:
                            st.markdown(f"**{g['title']}**")
                            c1, c2 = st.columns(2)
                            with c1:
                                st.caption(f"Strikes: {g['num_strikes']}")
                            with c2:
                                st.caption(f"Volume: {int(g['total_volume']):,}")
                            st.caption(f"End: {g['end_date']}")
                            if st.button("View strikes", key=f"view_{i}_{g['title']}"):
                                st.session_state["mm_selected_title"] = g["title"]
                                st.session_state["mm_scrolled"] = False
                                st.rerun()

            # Details table below cards
            selected_title = st.session_state.get("mm_selected_title")
            if selected_title:
                st.divider()
                # Anchor for smooth scroll on selection
                st.markdown("<a id='strikes_anchor'></a>", unsafe_allow_html=True)
                if not st.session_state.get("mm_scrolled", False):
                    components.html(
                        """
                        <script>
                        const el = document.getElementById('strikes_anchor');
                        if (el) { el.scrollIntoView({behavior: 'smooth', block: 'start'}); }
                        </script>
                        """,
                        height=0,
                    )
                    st.session_state["mm_scrolled"] = True

                st.subheader(f"Strikes – {selected_title}")
                group_map = st.session_state.get("mm_group_map") or {g["title"]: g for g in groups}
                st.session_state["mm_group_map"] = group_map
                g = group_map.get(selected_title)
                if g:
                    rows = []
                    for m in g["items"]:
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
                                "End Date": m.get("close_time") or m.get("end_date") or m.get("expiry_time"),
                            }
                        )
                    df = pd.DataFrame(rows)
                    if "End Date" in df.columns:
                        df["End Date"] = df["End Date"].apply(_safe_parse_dt)
                    # Sort by Yes Bid descending
                    if "Yes Bid (¢)" in df.columns:
                        df = df.sort_values(by="Yes Bid (¢)", ascending=False, na_position="last")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.info("No strikes available for the selected card.")

        # Raw data section removed per request


if __name__ == "__main__":
    main()


