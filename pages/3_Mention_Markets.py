from __future__ import annotations

import pandas as pd
import streamlit as st

from src.kalshi import KalshiClient


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

    try:
        with st.spinner("Loading mention markets..."):
            markets = _fetch_mention_markets_cached(cache_key)
    except Exception as e:
        st.error(f"Failed to load markets: {e}")
        return

    if not markets:
        st.info("No mention markets found.")
        return

    df = _to_display_df(markets)
    st.dataframe(df, use_container_width=True, hide_index=True)

    with st.expander("Raw data"):
        st.json(markets)


if __name__ == "__main__":
    main()


