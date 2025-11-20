from __future__ import annotations

from typing import Iterable, List, Sequence

import pandas as pd
import streamlit as st

from .models import Transcript


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


