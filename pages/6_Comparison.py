from __future__ import annotations

from typing import List, Dict

import pandas as pd
import streamlit as st

from src.ui_components import inject_dark_theme
from src.db import init_db


def _derive_description(m: dict) -> str:
	for k in ("subtitle", "yes_sub_title", "no_sub_title"):
		v = m.get(k)
		if v:
			return str(v)
	t = str(m.get("ticker", ""))
	if "-" in t:
		return t.split("-")[-1]
	return ""


def _build_event_strikes_df(event: dict) -> pd.DataFrame:
	rows = []
	for m in (event.get("items") or []):
		price = m.get("yes_bid") or m.get("yes_price") or (m.get("orderbook") or {}).get("yes_bid")
		try:
			price = float(price or 0)
		except Exception:
			price = 0.0
		rows.append(
			{
				"Word": _derive_description(m),
				"Yes Bid (%)": price,  # cents ~ percentage
				"Market": m.get("ticker"),
			}
		)
	df = pd.DataFrame(rows)
	if not df.empty:
		df = df.sort_values(by="Yes Bid (%)", ascending=False)
	return df


def _style_diff(df: pd.DataFrame) -> pd.io.formats.style.Styler:
	def color_row(row):
		bucket = row.get("Diff bucket", "")
		if bucket == "green":
			return ["background-color: #e8f5e9"] * len(row)
		if bucket == "blue":
			return ["background-color: #e3f2fd"] * len(row)
		if bucket == "red":
			return ["background-color: #ffebee"] * len(row)
		return [""] * len(row)

	return df.style.apply(color_row, axis=1)


def main() -> None:
	st.set_page_config(page_title="Comparison", page_icon="📊", layout="wide")
	inject_dark_theme()
	try:
		init_db()
	except Exception:
		pass

	st.title("Comparison")
	st.caption("Compare an active mention event against the current historical Strike Summary.")

	compare_event = st.session_state.get("compare_event")
	hist_df: pd.DataFrame | None = st.session_state.get("hist_summary_df")

	if not compare_event:
		st.info("No event staged for comparison. Go to Mention Markets and check 'Compare' on an event.")
		return
	if hist_df is None or hist_df.empty:
		st.info("Historical Strike Summary is empty. Go to Historical page and generate the summary first.")
		return

	left, right = st.columns(2)
	with left:
		st.subheader("Active event strikes")
		df_event = _build_event_strikes_df(compare_event)
		st.dataframe(df_event, width="stretch", hide_index=True)
	with right:
		st.subheader("Historical Strike Summary")
		st.dataframe(hist_df, width="stretch", hide_index=True)

	# Join on word
	if df_event.empty or hist_df.empty:
		return
	df_hist = hist_df.rename(columns={"Strike (word)": "Word", "% said": "% said"}).copy()
	join = pd.merge(df_event, df_hist[["Word", "% said"]], on="Word", how="inner")
	if join.empty:
		st.info("No overlapping words between event strikes and historical summary.")
		return
	join["Diff (%)"] = (join["Yes Bid (%)"] - join["% said"]).abs()
	def bucketize(x: float) -> str:
		if x <= 5.0:
			return "green"
		if x <= 15.0:
			return "blue"
		if x >= 25.0:
			return "red"
		return ""
	join["Diff bucket"] = join["Diff (%)"].apply(bucketize)
	join = join.sort_values(by="Diff (%)", ascending=False)

	st.subheader("Comparison (overlapping words)")
	st.dataframe(_style_diff(join[["Word", "Yes Bid (%)", "% said", "Diff (%)", "Diff bucket"]]), width="stretch", hide_index=True)


if __name__ == "__main__":
	main()


