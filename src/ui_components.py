from __future__ import annotations

from typing import Iterable, List, Sequence, Dict

import pandas as pd
import streamlit as st

from .models import Transcript


def inject_dark_theme() -> None:
    """
    Inject a modern dark theme using CSS variables and light utility styles.
    Safe to call multiple times; does nothing after first injection per run.
    """
    key = "_dark_theme_injected"
    if not st.session_state.get(key):
        st.markdown(
            """
            <style>
              :root {
                --mm-bg: #0f172a;            /* slate-900 */
                --mm-panel: #111827;         /* gray-900 */
                --mm-panel-2: #0b1220;       /* deeper bg */
                --mm-text: #e5e7eb;          /* gray-200 */
                --mm-subtext: #9ca3af;       /* gray-400 */
                --mm-accent: #22d3ee;        /* cyan-400 */
                --mm-accent-2: #60a5fa;      /* blue-400 */
                --mm-border: #1f2937;        /* gray-800 */
                --mm-green: #34d399;         /* emerald-400 */
                --mm-red: #f87171;           /* red-400 */
                --mm-yellow: #fbbf24;        /* amber-400 */
                --mm-radius: 12px;
              }
              /* App background and base text */
              .stApp, .stApp header, .stApp footer { background: var(--mm-bg) !important; color: var(--mm-text) !important; }
              /* Cards / containers */
              .stMarkdown, .stDataFrame, .stTable, .stSelectbox, .stTextInput, .stTextArea, .stMultiSelect, .stNumberInput {
                color: var(--mm-text) !important;
              }
              /* Make expander panels look like cards */
              .streamlit-expanderHeader { background: var(--mm-panel) !important; color: var(--mm-text) !important; border-radius: var(--mm-radius) !important; }
              .streamlit-expanderContent { background: var(--mm-panel-2) !important; border: 1px solid var(--mm-border) !important; border-top: none !important; border-radius: 0 0 var(--mm-radius) var(--mm-radius) !important; }
              /* Buttons */
              .stButton>button[kind="primary"] { background: linear-gradient(90deg, var(--mm-accent), var(--mm-accent-2)); color: #0b1020; border: none; border-radius: 10px; font-weight: 600; }
              .stButton>button[kind="secondary"] { background: transparent; border: 1px solid var(--mm-border); color: var(--mm-text); border-radius: 10px; }
              .stButton>button:hover { filter: brightness(1.05); }
              /* Metrics */
              [data-testid="stMetricValue"] { color: var(--mm-accent) !important; }
              /* Tables/Dataframes */
              div[data-testid="stDataFrame"] { background: var(--mm-panel); border: 1px solid var(--mm-border); border-radius: var(--mm-radius); }
              /* Inputs */
              .stTextInput input, .stTextArea textarea, .stSelectbox select, .stMultiSelect div, .stNumberInput input {
                background: #0c1324 !important; color: var(--mm-text) !important; border: 1px solid var(--mm-border) !important; border-radius: 10px !important;
              }
            </style>
            """,
            unsafe_allow_html=True,
        )
        st.session_state[key] = True

def render_keyword_input(*, key: str) -> List[str]:
    cols = st.columns([1, 1])
    with cols[0]:
        text_input = st.text_area(
            "Keywords (comma-separated)",
            value="",
            key=f"{key}_text",
            height=120,
            placeholder="e.g., cpi, fomc, rate hike, powell",
        )
    with cols[1]:
        csv_file = st.file_uploader("Or upload CSV of keywords (use first column)", type=["csv"], key=f"{key}_csv")

    keywords: List[str] = []
    if text_input:
        keywords.extend([t.strip() for t in text_input.split(",") if t.strip()])
    if csv_file is not None:
        try:
            df = pd.read_csv(csv_file)
            if not df.empty:
                first_col = df.columns[0]
                from_csv = [str(v).strip() for v in df[first_col].dropna().tolist()]
                keywords.extend(from_csv)
        except Exception:
            st.warning("Failed to parse CSV – please ensure it's a valid CSV file.")

    # Deduplicate while preserving order
    seen = set()
    deduped = []
    for k in keywords:
        if k.lower() in seen:
            continue
        seen.add(k.lower())
        deduped.append(k)
    return deduped


def render_library_selector(transcripts: Sequence[Transcript], *, key: str, label: str = "Select transcripts") -> List[int]:
    options = [(f"{t.title} (#{t.id})", t.id) for t in transcripts]
    display_to_id = {label: tid for label, tid in options}
    selection = st.multiselect(label, options=[label for label, _ in options], key=f"{key}_multiselect")
    return [display_to_id[s] for s in selection]


def render_tag_editor(*, existing_tags: Sequence[str], selected_tags: Sequence[str]) -> List[str]:
    all_unique = sorted({*existing_tags, *selected_tags})
    selected = st.multiselect("Tags", options=all_unique, default=list(selected_tags))
    new_tag = st.text_input("Add a new tag", value="")
    if new_tag.strip():
        if new_tag not in selected:
            selected.append(new_tag.strip())
    return selected


def render_transcript_mapping_table(transcripts: Sequence[Transcript], index_by_id: Dict[int, int]) -> None:
    df = pd.DataFrame(
        [
            {
                "#": index_by_id.get(int(t.id), None),
                "Title": t.title,
                "Word Count": t.word_count,
                "File Type": t.file_type,
            }
            for t in transcripts
        ]
    ).sort_values(by="#")
    st.dataframe(df, width="stretch", hide_index=True)


def render_transcript_weights(transcripts: Sequence[Transcript], *, key: str) -> Dict[int, float]:
    """
    Render per-transcript percentage weights that sum to 100.
    Returns mapping transcript_id -> percentage (0-100).
    """
    default_pct = round(100.0 / max(len(transcripts), 1), 2)
    weights_state_key = f"{key}_weights"
    if weights_state_key not in st.session_state:
        st.session_state[weights_state_key] = {int(t.id): default_pct for t in transcripts}
    # Keep state consistent with current selection
    existing = st.session_state[weights_state_key]
    current_ids = {int(t.id) for t in transcripts}
    # Drop removed
    for tid in list(existing.keys()):
        if tid not in current_ids:
            existing.pop(tid, None)
    # Add new
    for t in transcripts:
        existing.setdefault(int(t.id), default_pct)

    cols = st.columns([3, 1])
    with cols[0]:
        for t in transcripts:
            tid = int(t.id)
            st.session_state[weights_state_key][tid] = st.number_input(
                f"Weight % – {t.title} (#{tid})",
                min_value=0.0,
                max_value=100.0,
                step=1.0,
                value=float(st.session_state[weights_state_key][tid]),
                key=f"{key}_w_{tid}",
            )
    with cols[1]:
        if st.button("Equal weights", key=f"{key}_equalize"):
            even = round(100.0 / max(len(transcripts), 1), 2)
            for t in transcripts:
                st.session_state[weights_state_key][int(t.id)] = even
            st.experimental_rerun()

    total = sum(st.session_state[weights_state_key].values())
    st.caption(f"Total: {total:.2f}% (must equal 100% to compute)")
    return st.session_state[weights_state_key]

