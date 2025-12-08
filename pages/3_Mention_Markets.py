from __future__ import annotations

import pandas as pd
import streamlit as st

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


def main() -> None:
    st.set_page_config(page_title="Mention Markets", page_icon="💬", layout="wide")
    st.title("Mention Markets")
    st.caption("Live Kalshi mention markets and key stats.")

    with st.sidebar:
        st.subheader("Refresh")
        refresh_sec = st.slider("Auto-refresh interval (seconds)", min_value=0, max_value=300, value=60, step=15)
        _ = st.caption("Set to 0 to disable auto-refresh.")
        manual = st.button("Refresh now", type="primary", use_container_width=True)
        st.divider()
        st.subheader("Debug")
        debug_on = st.checkbox("Show Debug", value=False)
        search_term = st.text_input("Search term", value="mention", help="Used to filter titles in Debug tab.")
        series_filter = st.text_input("Series ticker filter (optional)", value="", help="If provided, fetch markets for this series.")

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

    tab_main, tab_debug = st.tabs(["Markets", "Debug"] if debug_on else ["Markets"])

    with tab_main:
        try:
            with st.spinner("Loading mention markets..."):
                markets = _fetch_mention_markets_cached(cache_key)
        except Exception as e:
            st.error(f"Failed to load markets: {e}")
            return

        if markets:
            df = _to_display_df(markets)
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No mention markets found. Showing a sample of active markets to verify connectivity.")
            try:
                client = KalshiClient()
                any_resp = client.request_debug("GET", "/trade-api/v2/markets", params={"limit": 50, "status": "active"})
                mk_items = any_resp.get("data", {}).get("markets") or any_resp.get("markets") or any_resp.get("data") or []
                if isinstance(mk_items, dict) and "markets" in mk_items:
                    mk_items = mk_items["markets"]
                if mk_items:
                    df_any = _to_display_df(list(mk_items)[:50])
                    st.dataframe(df_any, use_container_width=True, hide_index=True)
                else:
                    st.warning("Active markets sample request returned no items.")
            except Exception as e:
                st.error(f"Active markets sample error: {e}")

        with st.expander("Raw data"):
            st.json(markets)

    if debug_on:
        with tab_debug:
            client = KalshiClient()
            st.subheader("Series probe")
            st.caption(f"Base URL: {get_kalshi_api_base_url()}")
            try:
                series_resp = client.request_debug("GET", "/trade-api/v2/series", params={"limit": 200})
                st.write(f"Series status: {series_resp.get('status')}")
                series_items = series_resp.get("data", {}).get("series") or series_resp.get("data") or series_resp.get("series") or []
                if isinstance(series_items, dict) and "series" in series_items:
                    series_items = series_items["series"]
                s_rows = [{"ticker": s.get("ticker"), "title": s.get("title")} for s in (series_items or [])]
                s_df = pd.DataFrame(s_rows)
                if not s_df.empty:
                    if search_term.strip():
                        mask = s_df["title"].str.contains(search_term, case=False, na=False) | s_df["ticker"].str.contains(search_term, case=False, na=False)
                        st.write(f"Series matching '{search_term}':")
                        st.dataframe(s_df[mask], use_container_width=True, hide_index=True)
                    with st.expander("All series (first 200)"):
                        st.dataframe(s_df, use_container_width=True, hide_index=True)
                else:
                    st.info("No series returned.")
            except Exception as e:
                st.error(f"Series request error: {e}")

            st.divider()
            st.subheader("Markets probe")
            q_params = {"limit": 200, "status": "active"}
            if series_filter.strip():
                q_params["series_ticker"] = series_filter.strip()
            try:
                markets_resp = client.request_debug("GET", "/trade-api/v2/markets", params=q_params)
                st.write(f"Markets status: {markets_resp.get('status')}")
                mk_items = markets_resp.get("data", {}).get("markets") or markets_resp.get("markets") or markets_resp.get("data") or []
                if isinstance(mk_items, dict) and "markets" in mk_items:
                    mk_items = mk_items["markets"]
                m_rows = [{"ticker": m.get("ticker"), "title": m.get("title"), "status": m.get("status")} for m in (mk_items or [])]
                m_df = pd.DataFrame(m_rows)
                if not m_df.empty:
                    if search_term.strip():
                        mask = m_df["title"].str.contains(search_term, case=False, na=False)
                        st.write(f"Markets matching '{search_term}':")
                        st.dataframe(m_df[mask], use_container_width=True, hide_index=True)
                    with st.expander("All markets sample (first 200)"):
                        st.dataframe(m_df.head(200), use_container_width=True, hide_index=True)
                else:
                    st.info("No markets returned.")
            except Exception as e:
                st.error(f"Markets request error: {e}")


if __name__ == "__main__":
    main()


