import io
from typing import List, Sequence

import pandas as pd
import streamlit as st

from src.db import get_session, init_db
from src.models import Transcript
from src.storage import (
    create_transcript,
    get_transcript,
    list_transcripts,
    set_transcript_tags,
)
from src.text_processing import (
    compute_keyword_stats,
    extract_text,
    extract_transcripts_from_json,
    normalize_text_for_counting,
    tokenize_words,
)
from src.ui_components import (
    render_keyword_input,
    render_library_selector,
    render_transcript_weights,
    render_transcript_mapping_table,
    inject_dark_theme,
)

def _save_uploaded_files(files: Sequence[object], *, auto_tags: list[str] | None = None) -> list[int]:
    new_ids: list[int] = []
    for f in files:
        file_type = (f.type or "").lower()
        # Map MIME or extension to simple type
        simple_type = "txt"
        if f.name.lower().endswith(".pdf") or "pdf" in file_type:
            simple_type = "pdf"
        elif f.name.lower().endswith(".docx") or "word" in file_type:
            simple_type = "docx"
        elif f.name.lower().endswith(".json") or "json" in file_type:
            simple_type = "json"

        file_bytes = f.getvalue()
        if simple_type == "json":
            # May contain multiple raw transcripts; extract all
            items = extract_transcripts_from_json(file_bytes)
            for title, text in items:
                normalized = normalize_text_for_counting(text or "")
                tokens = tokenize_words(normalized)
                with get_session() as session:
                    new_id = create_transcript(
                        session=session,
                        title=title or f.name,
                        original_filename=f.name,
                        storage_location="",
                        text_content=text or "",
                        word_count=len(tokens),
                        estimated_minutes=(len(tokens) / max(st.session_state.get("words_per_minute", 150), 1)),
                        file_type=simple_type,
                        notes="",
                    )
                    if auto_tags:
                        set_transcript_tags(session, new_id, auto_tags)
                    new_ids.append(new_id)
        else:
            text = extract_text(file_bytes, simple_type)
            normalized = normalize_text_for_counting(text)
            tokens = tokenize_words(normalized)

            with get_session() as session:
                new_id = create_transcript(
                    session=session,
                    title=f.name,
                    original_filename=f.name,
                    storage_location="",
                    text_content=text,
                    word_count=len(tokens),
                    estimated_minutes=(len(tokens) / max(st.session_state.get("words_per_minute", 150), 1)),
                    file_type=simple_type,
                    notes="",
                )
                if auto_tags:
                    set_transcript_tags(session, new_id, auto_tags)
                new_ids.append(new_id)
    return new_ids


def main() -> None:
    st.set_page_config(page_title="Transcript Analysis", page_icon="🔍", layout="wide")
    inject_dark_theme()
    init_db()

    st.title("Transcript Analysis")
    st.caption("Upload or select transcripts, define keywords, and compute metrics.")

    left, right = st.columns([1, 1])
    with left:
        st.subheader("Upload transcripts")
        auto_tags_raw = st.text_input("Auto-tag on upload (optional, comma-separated)", value="", key="analysis_auto_tags")
        uploaded = st.file_uploader(
            "Upload one or more transcripts (PDF, DOCX, TXT, or JSON). These will be saved to the library.",
            type=["pdf", "docx", "txt", "json"],
            accept_multiple_files=True,
        )
        new_ids: list[int] = []
        if uploaded:
            if st.button("Save uploaded transcripts to library", type="primary"):
                auto_tags = [t.strip() for t in auto_tags_raw.split(",") if t.strip()] if auto_tags_raw else None
                new_ids = _save_uploaded_files(uploaded, auto_tags=auto_tags)
                st.success(f"Saved {len(new_ids)} transcripts to library.")
                # Auto-select newly uploaded transcripts in the analysis selector
                if new_ids:
                    try:
                        with get_session() as session:
                            labels = []
                            for nid in new_ids:
                                t = get_transcript(session, int(nid))
                                if t:
                                    labels.append(f"{t.title} (#{int(t.id)})")
                        prev = list(st.session_state.get("analysis_selector_multiselect") or [])
                        merged = sorted(list({*prev, *labels}))
                        st.session_state["analysis_selector_multiselect"] = merged
                    except Exception:
                        pass

    with right:
        st.subheader("Select from library")
        with get_session() as session:
            all_transcripts = list_transcripts(session=session)
        selected_ids = render_library_selector(all_transcripts, key="analysis_selector")
        if new_ids:
            selected_ids = list(set(selected_ids) | set(new_ids))

        # Quick selection helpers
        with st.expander("Quick select", expanded=False):
            total = len(all_transcripts)
            cols_q = st.columns(4)
            with cols_q[0]:
                start_idx = st.number_input("Start (1-based)", min_value=1, max_value=max(total, 1), value=1, step=1, key="analysis_sel_start")
            with cols_q[1]:
                end_idx = st.number_input("End (1-based)", min_value=1, max_value=max(total, 1), value=min(10, max(total, 1)), step=1, key="analysis_sel_end")
            with cols_q[2]:
                tag_pick = st.text_input("Select all with tag", value="", key="analysis_sel_tag")
            with cols_q[3]:
                clear = st.button("Clear all", key="analysis_sel_clear")
            cols_btn = st.columns(2)
            with cols_btn[0]:
                apply_range = st.button("Select range", key="analysis_sel_apply_range")
            with cols_btn[1]:
                apply_tag = st.button("Select by tag", key="analysis_sel_apply_tag")

            if clear:
                st.session_state["analysis_selector_multiselect"] = []
                st.experimental_rerun()

            if apply_range and total > 0:
                s = int(start_idx); e = int(end_idx)
                if s > e: s, e = e, s
                s = max(1, s); e = min(total, e)
                # Derive labels and set in multiselect
                labels = [f"{t.title} (#{int(t.id)})" for t in all_transcripts[s-1:e]]
                prev = list(st.session_state.get("analysis_selector_multiselect") or [])
                st.session_state["analysis_selector_multiselect"] = sorted(list({*prev, *labels}))
                st.experimental_rerun()

            if apply_tag and tag_pick.strip():
                tag_lower = tag_pick.strip().lower()
                matches = []
                for t in all_transcripts:
                    if any((tg.name or "").lower() == tag_lower for tg in t.tags):
                        matches.append(f"{t.title} (#{int(t.id)})")
                prev = list(st.session_state.get("analysis_selector_multiselect") or [])
                st.session_state["analysis_selector_multiselect"] = sorted(list({*prev, *matches}))
                st.experimental_rerun()

    st.divider()

    st.subheader("Keywords")
    keywords = render_keyword_input(key="analysis_keywords")
    st.caption("Enter comma-separated terms or upload a CSV (first column is used).")

    st.divider()
    # Prepare selected transcript objects and weights UI
    selected_transcripts: List[Transcript] = []
    index_by_id: dict[int, int] = {}
    if selected_ids:
        with get_session() as session:
            lookup = {t.id: t for t in list_transcripts(session=session)}
        for i, tid in enumerate(selected_ids, start=1):
            if tid in lookup:
                selected_transcripts.append(lookup[tid])
                index_by_id[int(tid)] = i

    weights_fraction = {}
    sum_ok = False
    if selected_transcripts:
        st.subheader("Weights")
        adjust_weights = st.checkbox("Adjust weights manually", value=False, key="analysis_weights_toggle")
        if adjust_weights:
            weights_pct = render_transcript_weights(selected_transcripts, key="analysis_weights")
            weights_fraction = {tid: (pct / 100.0) for tid, pct in weights_pct.items()}
            sum_ok = abs(sum(weights_pct.values()) - 100.0) < 1e-6
        else:
            # Equal weights, hide controls
            equal = 1.0 / max(len(selected_transcripts), 1)
            weights_fraction = {int(t.id): equal for t in selected_transcripts}
            sum_ok = True

    compute = st.button(
        "Compute keyword metrics",
        type="primary",
        disabled=(len(selected_ids) == 0 or len(keywords) == 0 or not sum_ok),
    )
    if compute:
        if not selected_transcripts:
            st.warning("No valid transcripts selected.")
            return

        result = compute_keyword_stats(
            transcripts=selected_transcripts,
            keywords=keywords,
            words_per_minute=st.session_state.get("words_per_minute", 150),
            weights_by_transcript_id=weights_fraction,
            transcript_index_by_id=index_by_id,
        )

        st.subheader("Results")
        st.metric("Average transcript length (words)", int(result["avg_transcript_word_count"]))
        st.metric("Average transcript duration (minutes)", round(float(result["avg_transcript_minutes"]), 2))

        st.markdown("Transcript index mapping")
        render_transcript_mapping_table(selected_transcripts, index_by_id)

        df: pd.DataFrame = result["keywords_df"]
        st.dataframe(df, width="stretch", hide_index=True)
        # Persist for Comparison page (optional use)
        st.session_state["analysis_keywords_df"] = df

        st.caption("Tip: Download as CSV from the dataframe menu for later analysis.")


if __name__ == "__main__":
    main()


