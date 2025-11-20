import math
from typing import List

from src.models import Transcript
from src.text_processing import compute_keyword_stats, normalize_text_for_counting, tokenize_words


def _t(text: str) -> Transcript:
    # Minimal Transcript instances for testing
    tr = Transcript(
        title="t",
        original_filename="t.txt",
        storage_location="",
        text_content=text,
        word_count=0,
        estimated_minutes=0.0,
        file_type="txt",
        notes="",
    )
    return tr


def test_normalize_and_tokenize():
    raw = "Hello,\n\nWorld!\tThis   is  a  test."
    norm = normalize_text_for_counting(raw)
    assert norm == "hello, world! this is a test."
    tokens = tokenize_words(norm)
    assert tokens == ["hello,", "world!", "this", "is", "a", "test."]


def test_compute_keyword_stats_simple():
    transcripts: List[Transcript] = [
        _t("The FOMC met today. Powell spoke. Rate hike talk."),
        _t("CPI is hot. The market expects no rate hike."),
    ]
    res = compute_keyword_stats(transcripts, ["FOMC", "rate hike", "Powell"], words_per_minute=150)
    df = res["keywords_df"]
    assert {"keyword", "total_mentions", "avg_mentions_per_transcript", "avg_relative_position_pct", "pct_transcripts_with_mention"} <= set(df.columns)

    row = df[df["keyword"] == "rate hike"].iloc[0]
    assert row["total_mentions"] == 2
    assert math.isclose(row["avg_mentions_per_transcript"], 1.0, rel_tol=1e-6)
    assert row["pct_transcripts_with_mention"] == 100.0


