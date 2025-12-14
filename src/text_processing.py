from __future__ import annotations

import io
import re
import json
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence, Tuple, Optional

import pandas as pd
from pypdf import PdfReader  # type: ignore
from docx import Document  # type: ignore

from .models import Transcript


def extract_text_from_pdf(file_bytes: bytes) -> str:
    buffer = io.BytesIO(file_bytes)
    reader = PdfReader(buffer)
    parts: List[str] = []
    for page in reader.pages:
        try:
            text = page.extract_text() or ""
            parts.append(text)
        except Exception:
            continue
    return "\n".join(parts).strip()


def extract_text_from_docx(file_bytes: bytes) -> str:
    buffer = io.BytesIO(file_bytes)
    doc = Document(buffer)
    paragraphs = [p.text for p in doc.paragraphs]
    return "\n".join(paragraphs).strip()


def extract_text_from_txt(file_bytes: bytes) -> str:
    # Try UTF-8, fallback to latin-1
    try:
        return file_bytes.decode("utf-8", errors="ignore").strip()
    except Exception:
        return file_bytes.decode("latin-1", errors="ignore").strip()


def extract_text(file_bytes: bytes, file_type: str) -> str:
    t = (file_type or "").lower().strip()
    if t == "pdf":
        return extract_text_from_pdf(file_bytes)
    if t == "docx":
        return extract_text_from_docx(file_bytes)
    return extract_text_from_txt(file_bytes)


def normalize_text_for_counting(text: str) -> str:
    lowered = text.lower()
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered


def tokenize_words(normalized_text: str) -> List[str]:
    if not normalized_text:
        return []
    return [t for t in normalized_text.split(" ") if t]


def _compile_keyword_pattern(keyword: str) -> re.Pattern[str]:
    """
    Compile a case-insensitive regex matching the keyword as a whole word or phrase.
    Whitespace within phrases is matched flexibly.
    """
    keyword = keyword.strip()
    # Escape and allow single or multiple spaces for phrase gaps
    escaped = re.escape(keyword)
    escaped = escaped.replace(r"\ ", r"\s+")
    pattern = rf"\b{escaped}\b"
    return re.compile(pattern, flags=re.IGNORECASE)


def _token_start_offsets(tokens: Sequence[str]) -> List[int]:
    offsets: List[int] = []
    cursor = 0
    for token in tokens:
        offsets.append(cursor)
        cursor += len(token) + 1  # plus single space in normalized text
    return offsets


def _char_index_to_token_index(char_index: int, token_offsets: Sequence[int]) -> int:
    # Binary search for rightmost offset <= char_index
    lo, hi = 0, len(token_offsets) - 1
    best = 0
    while lo <= hi:
        mid = (lo + hi) // 2
        if token_offsets[mid] <= char_index:
            best = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return best


def compute_keyword_stats(
    transcripts: List[Transcript],
    keywords: List[str],
    words_per_minute: int = 150,
    weights_by_transcript_id: Optional[Dict[int, float]] = None,
    transcript_index_by_id: Optional[Dict[int, int]] = None,
) -> Dict[str, object]:
    """
    Compute deterministic keyword metrics across transcripts.
    Returns dict with:
      - keywords_df: pd.DataFrame with columns:
            keyword, total_mentions, avg_mentions_per_transcript,
            avg_relative_position_pct, pct_transcripts_with_mention,
            per_transcript_counts, weighted_mentions
      - avg_transcript_word_count: float
      - avg_transcript_minutes: float
    """
    cleaned_keywords = sorted({kw.strip() for kw in keywords if kw and kw.strip()})
    if not transcripts or not cleaned_keywords:
        return {
            "keywords_df": pd.DataFrame(
                columns=[
                    "keyword",
                    "total_mentions",
                    "avg_mentions_per_transcript",
                    "avg_relative_position_pct",
                    "pct_transcripts_with_mention",
                    "per_transcript_counts",
                    "weighted_mentions",
                ]
            ),
            "avg_transcript_word_count": 0.0,
            "avg_transcript_minutes": 0.0,
        }

    num_transcripts = len(transcripts)
    per_kw_total_mentions: Dict[str, int] = {kw: 0 for kw in cleaned_keywords}
    per_kw_transcripts_with_mention: Dict[str, int] = {kw: 0 for kw in cleaned_keywords}
    per_kw_relative_positions: Dict[str, List[float]] = {kw: [] for kw in cleaned_keywords}
    per_kw_counts_by_transcript: Dict[str, Dict[int, int]] = {kw: {} for kw in cleaned_keywords}

    word_counts: List[int] = []

    compiled_patterns = {kw: _compile_keyword_pattern(kw) for kw in cleaned_keywords}

    for t in transcripts:
        text_original = t.text_content or ""
        normalized = normalize_text_for_counting(text_original)
        tokens = tokenize_words(normalized)
        token_count = len(tokens)
        word_counts.append(token_count)

        if token_count == 0:
            continue

        normalized_joined = " ".join(tokens)
        token_offsets = _token_start_offsets(tokens)

        for kw in cleaned_keywords:
            pattern = compiled_patterns[kw]
            matches = list(pattern.finditer(normalized_joined))
            if not matches:
                continue

            per_kw_total_mentions[kw] += len(matches)
            per_kw_transcripts_with_mention[kw] += 1
            per_kw_counts_by_transcript[kw][int(t.id)] = per_kw_counts_by_transcript[kw].get(int(t.id), 0) + len(matches)
            # For relative position, take the first token index of the match
            for m in matches:
                start_char = m.start()
                token_index = _char_index_to_token_index(start_char, token_offsets)
                relative = (token_index + 1) / token_count
                per_kw_relative_positions[kw].append(relative)

    avg_word_count = float(sum(word_counts) / len(word_counts)) if word_counts else 0.0
    avg_minutes = (avg_word_count / max(words_per_minute, 1)) if avg_word_count > 0 else 0.0

    rows = []
    for kw in cleaned_keywords:
        total_mentions = per_kw_total_mentions.get(kw, 0)
        denom = per_kw_transcripts_with_mention.get(kw, 0)
        avg_mentions = (total_mentions / denom) if denom > 0 else 0.0
        rel_positions = per_kw_relative_positions.get(kw, [])
        avg_rel_pct = (sum(rel_positions) / len(rel_positions) * 100.0) if rel_positions else 0.0
        pct_with_mention = (
            per_kw_transcripts_with_mention.get(kw, 0) / num_transcripts * 100.0 if num_transcripts > 0 else 0.0
        )
        # Build per-transcript breakdown like "#1 (3), #2 (7)"
        counts_map = per_kw_counts_by_transcript.get(kw, {})
        breakdown_parts: List[str] = []
        weighted_sum = 0.0
        # Sort by transcript index if provided, otherwise by transcript id
        def _sort_key(item: Tuple[int, int]) -> Tuple[int, int]:
            tid, _ = item
            if transcript_index_by_id and tid in transcript_index_by_id:
                return (transcript_index_by_id[tid], tid)
            return (tid, tid)

        for tid, count in sorted(counts_map.items(), key=_sort_key):
            idx = transcript_index_by_id.get(tid, None) if transcript_index_by_id else None
            label = f"#{idx}" if idx is not None else f"id:{tid}"
            breakdown_parts.append(f"{label} ({count})")
            if weights_by_transcript_id is not None:
                weight = float(weights_by_transcript_id.get(tid, 0.0))
                weighted_sum += weight * float(count)

        rows.append(
            {
                "keyword": kw,
                "total_mentions": int(total_mentions),
                "avg_mentions_per_transcript": float(avg_mentions),
                "avg_relative_position_pct": float(avg_rel_pct),
                "pct_transcripts_with_mention": float(pct_with_mention),
                "per_transcript_counts": ", ".join(breakdown_parts),
                "weighted_mentions": float(weighted_sum) if weights_by_transcript_id is not None else float(total_mentions),
            }
        )

    df = pd.DataFrame(rows).sort_values(by=["total_mentions", "keyword"], ascending=[False, True], ignore_index=True)
    return {
        "keywords_df": df,
        "avg_transcript_word_count": avg_word_count,
        "avg_transcript_minutes": avg_minutes,
    }


def extract_transcripts_from_json(file_bytes: bytes) -> List[Tuple[str, str]]:
    """
    Extract a list of (title, text) tuples from a JSON or JSONL payload.
    The function is resilient to a variety of shapes:
      - JSON array of objects with text fields (preferred keys: 'text', 'transcript', 'content', 'body')
      - JSON object with a top-level list under keys like 'transcripts', 'items', 'data'
      - JSON object mapping ids -> text
      - JSON string: treated as a single transcript
      - JSONL (newline-delimited JSON): each line an object with text fields
    Titles are derived from 'title'/'name'/'id'/'ticker' when available, otherwise a positional label.
    """
    def _derive_text(obj: object) -> Optional[str]:
        if obj is None:
            return None
        if isinstance(obj, str):
            return obj.strip()
        if isinstance(obj, dict):
            # Common text field names
            for k in ("text", "transcript", "content", "body"):
                v = obj.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
            # Sometimes text is provided as a list of segments
            for k in ("segments", "lines", "paragraphs"):
                segs = obj.get(k)
                if isinstance(segs, list):
                    joined = "\n".join([str(s or "") for s in segs]).strip()
                    if joined:
                        return joined
        return None

    def _derive_title(obj: object, idx: int) -> str:
        if isinstance(obj, dict):
            for k in ("title", "name", "id", "ticker", "filename"):
                v = obj.get(k)
                if v is not None:
                    return str(v)[:200]
        return f"item_{idx+1}"

    # Best-effort UTF-8 decode with fallback
    try:
        text = file_bytes.decode("utf-8", errors="ignore")
    except Exception:
        text = file_bytes.decode("latin-1", errors="ignore")
    text = text.strip()
    if not text:
        return []

    # 1) Try standard JSON parse
    try:
        data = json.loads(text)
        items: List[Tuple[str, str]] = []
        if isinstance(data, list):
            for i, entry in enumerate(data):
                val = _derive_text(entry)
                if val:
                    items.append((_derive_title(entry, i), val))
        elif isinstance(data, dict):
            # Object with nested list
            for key in ("transcripts", "items", "data", "records"):
                lst = data.get(key)
                if isinstance(lst, list):
                    for i, entry in enumerate(lst):
                        val = _derive_text(entry)
                        if val:
                            items.append((_derive_title(entry, i), val))
                    break
            else:
                # Mapping id -> text
                collected = []
                for k, v in data.items():
                    val = _derive_text(v) if not isinstance(v, str) else v.strip()
                    if val:
                        collected.append((str(k)[:200], val))
                if collected:
                    items.extend(collected)
                # Single-string payload
                if not items:
                    lone = _derive_text(data)
                    if lone:
                        items.append(("transcript", lone))
        elif isinstance(data, str):
            if data.strip():
                items.append(("transcript", data.strip()))
        if items:
            return items
    except Exception:
        pass

    # 2) Try JSONL (newline-delimited JSON)
    results: List[Tuple[str, str]] = []
    for i, line in enumerate(text.splitlines()):
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
            val = _derive_text(obj)
            if val:
                results.append((_derive_title(obj, i), val))
        except Exception:
            # skip non-JSON lines
            continue
    if results:
        return results

    # 3) Fallback: treat entire file as a single transcript
    return [("transcript", text)]


