from __future__ import annotations

import json
import streamlit as st

from src.ui_components import inject_dark_theme
from src.db import init_db


def _get_note(key: str) -> str:
	val = st.session_state.get(key)
	return str(val) if val is not None else ""


def _render_notepad(title: str, key: str, *, accent: str = "#22d3ee", placeholder: str = "") -> None:
	st.markdown(
		f"""
		<div style="background:#0c1324;border:1px solid #1f2937;border-radius:14px;padding:12px;">
		  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
		    <div style="font-weight:700;color:{accent};font-size:16px">{title}</div>
		    <div style="display:flex;gap:8px">
		      <form>
		        <button type="button" disabled style="opacity:0.4;border:1px solid #1f2937;background:transparent;color:#e5e7eb;border-radius:8px;padding:4px 8px;cursor:default;">Notepad</button>
		      </form>
		    </div>
		  </div>
		</div>
		""",
		unsafe_allow_html=True,
	)
	col1, col2 = st.columns([4, 1])
	with col1:
		st.text_area(
			"",
			key=key,
			value=_get_note(key),
			height=220,
			label_visibility="collapsed",
			placeholder=placeholder,
		)
	with col2:
		# Controls: Clear, Download
		if st.button("Clear", key=f"clear_{key}", type="secondary"):
			st.session_state[key] = ""
			st.rerun()
		st.download_button(
			"Download",
			data=_get_note(key).encode("utf-8"),
			file_name=f"{title.replace(' ', '_').lower()}.txt",
			mime="text/plain",
		)


def main() -> None:
	st.set_page_config(page_title="Strategy Notebooks", page_icon="📝", layout="wide")
	inject_dark_theme()
	try:
		init_db()
	except Exception:
		# Notes are session-based; DB is optional here
		pass

	st.title("Strategy Notebooks")
	st.caption("Four focused notepads to capture ideas, rules, and changes.")

	with st.expander("Backup / Restore", expanded=False):
		col_b1, col_b2 = st.columns([1, 2])
		with col_b1:
			payload = {
				"strategies": _get_note("notepad_strategies"),
				"event_rules": _get_note("notepad_event_rules"),
				"vertical_rules": _get_note("notepad_vertical_rules"),
				"changelog": _get_note("notepad_changelog"),
			}
			st.download_button(
				"Download all (JSON)",
				data=(json.dumps(payload, ensure_ascii=False, indent=2)).encode("utf-8"),
				file_name="strategy_notebooks_backup.json",
				mime="application/json",
			)
		with col_b2:
			up = st.file_uploader("Restore from JSON backup", type=["json"], key="notepad_restore")
			if up is not None:
				try:
					data = json.loads(up.read().decode("utf-8", errors="ignore"))
					if isinstance(data, dict):
						if "strategies" in data:
							st.session_state["notepad_strategies"] = str(data.get("strategies") or "")
						if "event_rules" in data:
							st.session_state["notepad_event_rules"] = str(data.get("event_rules") or "")
						if "vertical_rules" in data:
							st.session_state["notepad_vertical_rules"] = str(data.get("vertical_rules") or "")
						if "changelog" in data:
							st.session_state["notepad_changelog"] = str(data.get("changelog") or "")
						st.success("Notes restored.")
						st.rerun()
				except Exception as e:
					st.warning(f"Failed to restore: {e}")

	st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

	# 2x2 grid of notepads
	row1 = st.columns(2)
	with row1[0]:
		_render_notepad(
			"Strategies",
			"notepad_strategies",
			accent="#60a5fa",
			placeholder="Alpha ideas, setups, context, execution checklists...",
		)
	with row1[1]:
		_render_notepad(
			"Event Rules / Tricks",
			"notepad_event_rules",
			accent="#34d399",
			placeholder="Catalyst playbook, quirks, wording gotchas, event-specific tactics...",
		)
	st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
	row2 = st.columns(2)
	with row2[0]:
		_render_notepad(
			"Vertical Rules / Tricks",
			"notepad_vertical_rules",
			accent="#fbbf24",
			placeholder="Strike selection, structure preferences, skew habits, hedging rules...",
		)
	with row2[1]:
		_render_notepad(
			"Changelog",
			"notepad_changelog",
			accent="#f87171",
			placeholder="What changed, why, and learnings to carry forward...",
		)


if __name__ == "__main__":
	main()


