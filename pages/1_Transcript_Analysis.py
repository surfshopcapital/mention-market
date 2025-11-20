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
)
from src.text_processing import (
    compute_keyword_stats,
    extract_text,
    normalize_text_for_counting,
    tokenize_words,
)
from src.ui_components import (
    render_keyword_input,
    render_library_selector,
)


def _save_uploaded_files(files: Sequence[object]) -> list[int]:
    new_ids: list[int] = []
    for f in files:
        file_type = (f.type or "").lower()
        # Map MIME or extension to simple type
        simple_type = "txt"
        if f.name.lower().endswith(".pdf") or "pdf" in file_type:
            simple_type = "pdf"
        elif f.name.lower().endswith(".docx") or "word" in file_type:
            simple_type = "docx"

        file_bytes = f.getvalue()
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
            new_ids.append(new_id)
    return new_ids


def main() -> None:
    st.set_page_config(page_title="Transcript Analysis", page_icon="🔍", layout="wide")
    init_db()

    st.title("Transcript Analysis")
    st.caption("Upload or select transcripts, define keywords, and compute metrics.")

    left, right = st.columns([1, 1])
    with left:
        st.subheader("Upload transcripts")
        uploaded = st.file_uploader(
            "Upload one or more transcripts (PDF, DOCX, or TXT). These will be saved to the library.",
            type=["pdf", "docx", "txt"],
            accept_multiple_files=True,
        )
        new_ids: list[int] = []
        if uploaded:
            if st.button("Save uploaded transcripts to library", type="primary"):
                new_ids = _save_uploaded_files(uploaded)
                st.success(f"Saved {len(new_ids)} transcripts to library.")

    with right:
        st.subheader("Select from library")
        with get_session() as session:
            all_transcripts = list_transcripts(session=session)
        selected_ids = render_library_selector(all_transcripts, key="analysis_selector")
        if new_ids:
            selected_ids = list(set(selected_ids) | set(new_ids))

    st.divider()

    st.subheader("Keywords")
    keywords = render_keyword_input(key="analysis_keywords")
    st.caption("Enter comma-separated terms or upload a CSV (first column is used).")

    st.divider()
    compute = st.button("Compute keyword metrics", type="primary", disabled=(len(selected_ids) == 0 or len(keywords) == 0))
    if compute:
        with get_session() as session:
            selected: List[Transcript] = [get_transcript(session, tid) for tid in selected_ids]
            selected = [t for t in selected if t is not None]

        if not selected:
            st.warning("No valid transcripts selected.")
            return

        result = compute_keyword_stats(
            transcripts=selected,
            keywords=keywords,
            words_per_minute=st.session_state.get("words_per_minute", 150),
        )

        st.subheader("Results")
        st.metric("Average transcript length (words)", int(result["avg_transcript_word_count"]))
        st.metric("Average transcript duration (minutes)", round(float(result["avg_transcript_minutes"]), 2))

        df: pd.DataFrame = result["keywords_df"]
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.caption("Tip: Download as CSV from the dataframe menu for later analysis.")


if __name__ == "__main__":
    main()


