from __future__ import annotations

import json
import streamlit as st

from src.ui_components import inject_dark_theme
from src.db import get_session, init_db
from src.storage import get_strategy_note, upsert_strategy_note


def _get_note(key: str) -> str:
	val = st.session_state.get(key)
	return str(val) if val is not None else ""

def _load_notes_from_db() -> None:
	# Hydrate session state from DB if the fields are missing/empty.
	# This prevents overwriting what the user is actively typing.
	try:
		with get_session() as sess:
			mapping = {
				"notepad_strategies": "strategies",
				"notepad_event_rules": "event_rules",
				"notepad_vertical_rules": "vertical_rules",
				"notepad_changelog": "changelog",
			}
			for sk, dbk in mapping.items():
				if sk not in st.session_state or st.session_state.get(sk) in (None, ""):
					st.session_state[sk] = get_strategy_note(sess, dbk)
	except Exception as e:
		st.session_state["notebooks_db_error"] = str(e)


def _save_note_to_db(note_key: str, db_key: str) -> None:
	try:
		with get_session() as sess:
			upsert_strategy_note(sess, key=db_key, content=_get_note(note_key))
		st.session_state["notebooks_db_error"] = ""
	except Exception:
		# Surface errors (don't silently claim "saved")
		import traceback as _tb
		st.session_state["notebooks_db_error"] = _tb.format_exc()


def _render_notepad(title: str, key: str, *, accent: str = "#22d3ee", placeholder: str = "", db_key: str) -> None:
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
			on_change=_save_note_to_db,
			args=(key, db_key),
		)
	with col2:
		# Controls: Save, Clear, Download
		if st.button("Save", key=f"save_{key}", type="primary"):
			_save_note_to_db(key, db_key)
			if st.session_state.get("notebooks_db_error"):
				st.error("Save failed (see DB error above).")
			else:
				st.success("Saved.")
		if st.button("Clear", key=f"clear_{key}", type="secondary"):
			st.session_state[key] = ""
			_save_note_to_db(key, db_key)
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
	init_db()

	st.title("Strategy Notebooks")
	st.caption("Four focused notepads to capture ideas, rules, and changes.")

	_load_notes_from_db()
	if st.session_state.get("notebooks_db_error"):
		with st.expander("DB error (notes may not persist)", expanded=True):
			st.code(str(st.session_state.get("notebooks_db_error")), language="text")

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
							_save_note_to_db("notepad_strategies", "strategies")
						if "event_rules" in data:
							st.session_state["notepad_event_rules"] = str(data.get("event_rules") or "")
							_save_note_to_db("notepad_event_rules", "event_rules")
						if "vertical_rules" in data:
							st.session_state["notepad_vertical_rules"] = str(data.get("vertical_rules") or "")
							_save_note_to_db("notepad_vertical_rules", "vertical_rules")
						if "changelog" in data:
							st.session_state["notepad_changelog"] = str(data.get("changelog") or "")
							_save_note_to_db("notepad_changelog", "changelog")
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
			db_key="strategies",
		)
	with row1[1]:
		_render_notepad(
			"Event Rules / Tricks",
			"notepad_event_rules",
			accent="#34d399",
			placeholder="Catalyst playbook, quirks, wording gotchas, event-specific tactics...",
			db_key="event_rules",
		)
	st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
	row2 = st.columns(2)
	with row2[0]:
		_render_notepad(
			"Vertical Rules / Tricks",
			"notepad_vertical_rules",
			accent="#fbbf24",
			placeholder="Strike selection, structure preferences, skew habits, hedging rules...",
			db_key="vertical_rules",
		)
	with row2[1]:
		_render_notepad(
			"Changelog",
			"notepad_changelog",
			accent="#f87171",
			placeholder="What changed, why, and learnings to carry forward...",
			db_key="changelog",
		)


if __name__ == "__main__":
	main()


