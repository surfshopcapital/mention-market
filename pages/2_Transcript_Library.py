from typing import List, Optional

import pandas as pd
import streamlit as st

from src.db import get_session, init_db
from src.models import Transcript
from src.storage import (
    create_transcript,
    delete_transcript,
    list_transcripts,
    set_transcript_tags,
    update_transcript_title,
)
from src.text_processing import extract_text, normalize_text_for_counting, tokenize_words
from src.ui_components import render_library_selector, render_tag_editor


def _save_uploaded_files(files: list[object]) -> list[int]:
    new_ids: list[int] = []
    for f in files:
        file_type = (f.type or "").lower()
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


def _render_library_table(transcripts: List[Transcript]) -> None:
    df = pd.DataFrame(
        [
            {
                "ID": t.id,
                "Title": t.title,
                "Filename": t.original_filename,
                "File Type": t.file_type,
                "Word Count": t.word_count,
                "Est. Minutes": round(float(t.estimated_minutes or 0.0), 2),
                "Uploaded": t.uploaded_at,
                "Tags": ", ".join(sorted(tag.name for tag in t.tags)),
            }
            for t in transcripts
        ]
    )
    st.dataframe(df, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Transcript Library", page_icon="📚", layout="wide")
    init_db()

    st.title("Transcript Library")
    st.caption("Cloud-backed library of transcripts with tagging and metadata.")

    st.subheader("Upload transcript")
    files = st.file_uploader(
        "Upload one or more transcripts (PDF, DOCX, or TXT).",
        type=["pdf", "docx", "txt"],
        accept_multiple_files=True,
    )
    if files and st.button("Upload", type="primary"):
        ids = _save_uploaded_files(list(files))
        st.success(f"Uploaded {len(ids)} transcript(s).")

    st.divider()
    st.subheader("Library")
    with get_session() as session:
        transcripts = list_transcripts(session=session)

    _render_library_table(transcripts)

    st.divider()
    st.subheader("Manage transcript")
    selected_ids = render_library_selector(transcripts, key="library_selector", label="Select transcript(s)")
    selected_id: Optional[int] = selected_ids[0] if selected_ids else None

    if selected_id is not None:
        with get_session() as session:
            # Re-fetch to ensure relationships are loaded
            selected = [t for t in transcripts if t.id == selected_id][0]
        st.text_input("Title", value=selected.title, key="edit_title")
        if st.button("Save title"):
            with get_session() as session:
                update_transcript_title(session, selected_id, st.session_state["edit_title"].strip())
                st.success("Title updated.")

        st.markdown("Tags")
        with get_session() as session:
            # latest state
            updated_list = list_transcripts(session=session)
            selected = [t for t in updated_list if t.id == selected_id][0]
            all_tag_names = sorted({tag.name for t in updated_list for tag in t.tags})
        new_tags = render_tag_editor(existing_tags=all_tag_names, selected_tags=[tag.name for tag in selected.tags])
        if st.button("Save tags"):
            with get_session() as session:
                set_transcript_tags(session, selected_id, new_tags)
                st.success("Tags updated.")

        st.divider()
        if st.button("Delete transcript", type="secondary"):
            with get_session() as session:
                delete_transcript(session, selected_id)
                st.success("Transcript deleted.")


if __name__ == "__main__":
    main()


