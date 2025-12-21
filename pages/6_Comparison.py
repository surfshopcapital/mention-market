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
		ask = m.get("yes_ask") or (m.get("orderbook") or {}).get("yes_ask")
		vol = m.get("volume")
		try:
			price = float(price or 0)
		except Exception:
			price = 0.0
		try:
			ask = float(ask or 0)
		except Exception:
			ask = 0.0
		try:
			vol = int(vol or 0)
		except Exception:
			vol = 0
		rows.append(
			{
				"Word": _derive_description(m),
				"Yes Bid (%)": price,  # cents ~ percentage
				"Yes Ask (%)": ask,
				"Volume": vol,
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
			return ["background-color: #e8f5e9; color: #000"] * len(row)
		if bucket == "blue":
			return ["background-color: #e3f2fd; color: #000"] * len(row)
		if bucket == "red":
			return ["background-color: #ffebee; color: #000"] * len(row)
		return ["color: #000"] * len(row)

	return df.style.apply(color_row, axis=1)


def main() -> None:
	st.set_page_config(page_title="Comparison", page_icon="📊", layout="wide")
	inject_dark_theme()
	try:
		init_db()
	except Exception:
		pass

	st.title("Comparison")
	st.caption("Compare an active mention event against either the Historical Strike Summary or your Transcript Analysis output.")

	compare_event = st.session_state.get("compare_event")
	hist_df: pd.DataFrame | None = st.session_state.get("hist_summary_df")
	analysis_df: pd.DataFrame | None = st.session_state.get("analysis_keywords_df")
	pct_left = st.number_input("% game left", min_value=0.0, max_value=100.0, value=100.0, step=1.0)

	if not compare_event:
		st.info("No event staged for comparison. Go to Mention Markets and check 'Compare' on an event.")
		return

	source = st.radio("Compare against", options=["Historical Strike Summary", "Transcript Analysis Output"], horizontal=True, index=0)
	metric_name = "% said"
	if source == "Historical Strike Summary":
		if hist_df is None or hist_df.empty:
			st.info("Historical Strike Summary is empty. Go to Historical page and generate the summary first.")
			return
	else:
		# Transcript Analysis path
		if analysis_df is None or analysis_df.empty:
			st.info("Transcript Analysis output is empty. Go to Transcript Analysis, compute metrics, then return here.")
			return
		# Choose metric to compare
		with st.expander("Analysis comparison settings", expanded=True):
			options_map = {
				"pct_transcripts_with_mention (%)": "pct_transcripts_with_mention",
				"avg_mentions_per_transcript": "avg_mentions_per_transcript",
				"total_mentions": "total_mentions",
				"weighted_mentions": "weighted_mentions",
			}
			choice = st.selectbox("Metric to compare", list(options_map.keys()), index=0)
			metric_name = choice  # display label
			metric_col = options_map[choice]

	left, right = st.columns(2)
	with left:
		st.subheader("Active event strikes")
		df_event = _build_event_strikes_df(compare_event)
		st.dataframe(df_event, width="stretch", hide_index=True)
	with right:
		if source == "Historical Strike Summary":
			st.subheader("Historical Strike Summary")
			st.dataframe(hist_df, width="stretch", hide_index=True)
		else:
			st.subheader("Transcript Analysis Output")
			st.dataframe(analysis_df, width="stretch", hide_index=True)

	# Join on word
	if df_event.empty or hist_df.empty:
		# For historical path, we need hist_df; for analysis path we'll handle separately
		if source == "Historical Strike Summary":
			return

	if source == "Historical Strike Summary":
		df_hist = hist_df.rename(columns={"Strike (word)": "Word", "% said": "% said"}).copy()
		# Carry through extra context columns when present
		hcols = ["Word", "% said"]
		for extra in ["Times said", "Events possible", "Trend"]:
			if extra in df_hist.columns:
				hcols.append(extra)
		join = pd.merge(df_event, df_hist[hcols], on="Word", how="inner")
		target_col = "% said"
		scale = pct_left / 100.0
		join["Adj prob (%)"] = join[target_col].astype(float) * float(scale)
		display_col = "Adj prob (%)"
	else:
		# Prepare analysis table with 'Word' and the chosen metric
		df_ana = analysis_df.copy()
		if "keyword" in df_ana.columns:
			df_ana = df_ana.rename(columns={"keyword": "Word"})
		# Determine selected metric column
		target_col = options_map[choice]
		if target_col not in df_ana.columns:
			st.info("Selected metric not available in analysis output.")
			return
		join = pd.merge(df_event, df_ana[["Word", target_col]], on="Word", how="inner")
		display_col = target_col
		# Apply % game left only for percentage-like metrics
		if target_col == "pct_transcripts_with_mention":
			scale = pct_left / 100.0
			join["Adj prob (%)"] = join[target_col].astype(float) * float(scale)
			display_col = "Adj prob (%)"

	if join.empty:
		st.info("No overlapping words between event strikes and selected comparison table.")
		return
	join["Diff (%)"] = (join["Yes Bid (%)"] - join[display_col]).abs()
	def bucketize(x: float) -> str:
		if x <= 5.0:
			return "green"
		if x <= 20.0:
			return "blue"
		if x >= 20.0:
			return "red"
		return ""
	join["Diff bucket"] = join["Diff (%)"].apply(bucketize)
	join = join.sort_values(by="Diff (%)", ascending=False)

	st.subheader("Comparison (overlapping words)")
	cols = ["Word", "Yes Bid (%)"]
	# Add historical context if available
	for extra in ["Times said", "Events possible", "Trend"]:
		if extra in join.columns:
			cols.append(extra)
	cols += [display_col, "Diff (%)", "Diff bucket"]
	st.dataframe(_style_diff(join[[c for c in cols if c in join.columns]]), width="stretch", hide_index=True)

	# Expected PnL calculator (only meaningful when the comparison column is a probability %)
	st.divider()
	st.subheader("Expected PnL (using comparison probability)")
	if display_col not in join.columns:
		st.info("Probability column unavailable for PnL.")
		return

	# We treat display_col as a percentage probability when it looks like one
	try:
		_ = float(join[display_col].dropna().iloc[0]) if not join.empty else 0.0
	except Exception:
		st.info("Selected comparison metric is not a probability; PnL is only available for probability-based comparisons.")
		return

	c1, c2, c3 = st.columns([1, 1, 2])
	with c1:
		total_bet = st.number_input("Total bet ($)", min_value=0.0, value=1000.0, step=100.0)
	with c2:
		side = st.selectbox("Side", options=["YES", "NO"], index=0)
	with c3:
		dist = st.radio(
			"Allocation",
			options=[
				"Evenly distributed across each strike",
				"Only strikes where Yes Bid (%) > probability (betting NO value filter)",
			],
			index=0,
			horizontal=True,
		)

	# Select eligible strikes for allocation
	df_calc = join.copy()
	df_calc["prob_pct"] = pd.to_numeric(df_calc[display_col], errors="coerce")
	df_calc["yes_bid_pct"] = pd.to_numeric(df_calc["Yes Bid (%)"], errors="coerce")
	df_calc["yes_ask_pct"] = pd.to_numeric(df_calc.get("Yes Ask (%)", pd.Series([None] * len(df_calc))), errors="coerce")
	df_calc = df_calc.dropna(subset=["prob_pct", "yes_bid_pct"])
	if df_calc.empty:
		st.info("No rows available for PnL calculation.")
		return

	if dist.startswith("Only strikes"):
		df_calc = df_calc[df_calc["yes_bid_pct"] > df_calc["prob_pct"]]
		if df_calc.empty:
			st.info("No strikes matched the filter (Yes Bid > probability).")
			return

	# Allocate dollars
	n = len(df_calc)
	alloc_per = float(total_bet) / max(n, 1)
	df_calc["alloc_$"] = alloc_per

	# Price model (approximation): use YES ask if available for buying YES, else YES bid.
	# For NO, approximate price as (100 - YES bid)/100 (since NO book not available on comparison page).
	def _yes_price(p_bid: float, p_ask: float | None) -> float:
		if p_ask is not None and p_ask > 0:
			return float(p_ask) / 100.0
		return float(p_bid) / 100.0

	def _no_price(p_bid: float, p_ask: float | None) -> float:
		# Use opposite of YES bid/ask to approximate NO; prefer using YES bid for conservative pricing.
		base = float(p_bid)
		return max(0.001, (100.0 - base) / 100.0)

	def _expected_profit(alloc: float, prob_pct: float, yes_bid: float, yes_ask: float | None, side: str) -> float:
		p = max(0.0, min(1.0, float(prob_pct) / 100.0))
		if side == "YES":
			price = _yes_price(yes_bid, yes_ask)
			# expected payout = (alloc/price) * p ; profit = payout - alloc
			return (alloc / max(price, 0.001)) * p - alloc
		# NO
		price = _no_price(yes_bid, yes_ask)
		return (alloc / max(price, 0.001)) * (1.0 - p) - alloc

	exp = []
	for _, r in df_calc.iterrows():
		exp.append(
			_expected_profit(
				float(r["alloc_$"]),
				float(r["prob_pct"]),
				float(r["yes_bid_pct"]),
				(float(r["yes_ask_pct"]) if pd.notna(r["yes_ask_pct"]) else None),
				side,
			)
		)
	df_calc["expected_pnl_$"] = exp

	st.metric("Num strikes bet", int(len(df_calc)))
	st.metric("Expected PnL ($)", round(float(df_calc["expected_pnl_$"].sum()), 2))

	out_cols = ["Word", "alloc_$", "yes_bid_pct", "prob_pct", "expected_pnl_$"]
	st.dataframe(
		df_calc[out_cols].rename(
			columns={
				"alloc_$": "Allocated ($)",
				"yes_bid_pct": "Yes Bid (%)",
				"prob_pct": f"{display_col}",
				"expected_pnl_$": "Expected PnL ($)",
			}
		),
		width="stretch",
		hide_index=True,
	)


if __name__ == "__main__":
	main()


