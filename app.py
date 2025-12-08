import streamlit as st

from src.config import get_default_words_per_minute
from src.db import init_db


def _init_app_state() -> None:
    if "words_per_minute" not in st.session_state:
        st.session_state["words_per_minute"] = get_default_words_per_minute()


def main() -> None:
    st.set_page_config(page_title="Kalshi Mention Markets – Transcript Toolkit", page_icon="📈", layout="wide")
    _init_app_state()
    # Ensure DB schema exists (safe to call multiple times)
    init_db()

    st.title("Kalshi Mention Markets – Transcript Toolkit")
    st.caption("Prepare, analyze, and trade on Kalshi mention markets using transcript analysis.")

    st.markdown(
        """
        This multi-page Streamlit app helps you:
        - Upload and organize transcripts in a cloud-backed library
        - Compute deterministic keyword metrics across selected transcripts
        - Prepare mention-market strategies with transparent, auditable statistics
        """
    )

    st.subheader("Quick links")
    cols = st.columns(3)
    with cols[0]:
        st.page_link("pages/1_Transcript_Analysis.py", label="1) Transcript Analysis", icon="🔍")
    with cols[1]:
        st.page_link("pages/2_Transcript_Library.py", label="2) Transcript Library", icon="📚")
    with cols[2]:
        st.page_link("pages/3_Mention_Markets.py", label="3) Mention Markets", icon="💬")

    st.divider()
    st.subheader("Global settings")
    st.number_input(
        "Words per minute (for duration estimates)",
        min_value=60,
        max_value=400,
        value=st.session_state["words_per_minute"],
        step=10,
        key="words_per_minute",
        help="Used to estimate transcript durations from word counts.",
    )


if __name__ == "__main__":
    main()


