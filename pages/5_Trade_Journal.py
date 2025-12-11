from __future__ import annotations

from typing import List

import pandas as pd
import streamlit as st

from src.db import get_session, init_db
from src.storage import list_trade_entries, set_trade_note
from src.ui_components import inject_dark_theme


def main() -> None:
	st.set_page_config(page_title="Trade Journal", page_icon="📝", layout="wide")
	inject_dark_theme()
	try:
		init_db()
	except Exception:
		pass

	st.title("Trade Journal")
	st.caption("Played markets and notes.")

	# Filters
	col1, col2, col3 = st.columns([2, 1, 1])
	with col1:
		q = st.text_input("Search (ticker/event/title/word/note)", value="")
	with col2:
		start = st.date_input("Start (optional)", value=None)
	with col3:
		end = st.date_input("End (optional)", value=None)

	start_dt = pd.Timestamp(start).to_pydatetime() if start else None
	end_dt = pd.Timestamp(end).to_pydatetime() if end else None

	with get_session() as sess:
		rows = list_trade_entries(sess, search=q.strip() or None, start=start_dt, end=end_dt)

	# Display
	if not rows:
		st.info("No played markets yet.")
		return

	data = []
	for r in rows:
		data.append(
			{
				"Played at (UTC)": r.played_at.strftime("%b %d, %Y %H:%M UTC"),
				"Market": r.market_ticker,
				"Event": r.event_ticker,
				"Title": r.title,
				"Word": r.word,
				"Note": r.note,
			}
		)
	df = pd.DataFrame(data)
	st.dataframe(df, width="stretch", hide_index=True)

	st.subheader("Edit notes")
	for r in rows[:50]:
		cols = st.columns([2, 5, 2])
		with cols[0]:
			st.caption(r.market_ticker)
		with cols[1]:
			key = f"tj_note_{r.market_ticker}"
			val = st.text_input("Note", value=r.note or "", key=key, label_visibility="collapsed")
		with cols[2]:
			if st.button("Save", key=f"tj_save_{r.market_ticker}"):
				with get_session() as sess:
					set_trade_note(sess, r.market_ticker, st.session_state[key])
				st.success("Saved")


if __name__ == "__main__":
	main()


