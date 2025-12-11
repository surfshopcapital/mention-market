from __future__ import annotations

from typing import Iterable, List, Optional, Sequence

from sqlalchemy import asc, desc, select
from sqlalchemy.orm import Session

from .models import MarketTag, Tag, Transcript, TradeEntry, transcript_tag_association
from datetime import datetime


def create_transcript(
    session: Session,
    *,
    title: str,
    original_filename: str,
    storage_location: str,
    text_content: str,
    word_count: int,
    estimated_minutes: float,
    file_type: str,
    notes: str = "",
) -> int:
    transcript = Transcript(
        title=title or original_filename,
        original_filename=original_filename,
        storage_location=storage_location,
        text_content=text_content,
        word_count=word_count,
        estimated_minutes=float(estimated_minutes),
        file_type=file_type,
        notes=notes or "",
    )
    session.add(transcript)
    session.flush()
    return int(transcript.id)


def update_transcript_title(session: Session, transcript_id: int, new_title: str) -> None:
    stmt = select(Transcript).where(Transcript.id == transcript_id)
    transcript = session.scalars(stmt).first()
    if not transcript:
        return
    transcript.title = new_title
    session.add(transcript)


def _get_or_create_tags(session: Session, names: Iterable[str]) -> List[Tag]:
    names_clean = sorted({n.strip() for n in names if n and n.strip()})
    if not names_clean:
        return []
    existing = session.scalars(select(Tag).where(Tag.name.in_(names_clean))).all()
    by_name = {t.name: t for t in existing}
    to_create = [n for n in names_clean if n not in by_name]
    for name in to_create:
        tag = Tag(name=name)
        session.add(tag)
        session.flush()
        by_name[name] = tag
    return [by_name[n] for n in names_clean]


def set_transcript_tags(session: Session, transcript_id: int, tag_names: Sequence[str]) -> None:
    stmt = select(Transcript).where(Transcript.id == transcript_id)
    transcript = session.scalars(stmt).first()
    if not transcript:
        return
    tags = _get_or_create_tags(session, tag_names)
    transcript.tags = tags
    session.add(transcript)


def delete_transcript(session: Session, transcript_id: int) -> None:
    transcript = session.get(Transcript, transcript_id)
    if not transcript:
        return
    session.delete(transcript)


def list_transcripts(
    session: Session,
    *,
    tag_filters_any: Optional[Sequence[str]] = None,
    search_title: Optional[str] = None,
) -> List[Transcript]:
    stmt = select(Transcript).order_by(desc(Transcript.uploaded_at), asc(Transcript.id))
    results = session.scalars(stmt).unique().all()

    filtered = results
    if search_title:
        needle = search_title.lower()
        filtered = [t for t in filtered if needle in (t.title or "").lower()]
    if tag_filters_any:
        tag_set = {t.strip().lower() for t in tag_filters_any if t.strip()}
        if tag_set:
            filtered = [
                t
                for t in filtered
                if any((tag.name or "").lower() in tag_set for tag in t.tags)
            ]
    return filtered


def get_transcript(session: Session, transcript_id: int) -> Optional[Transcript]:
    return session.get(Transcript, transcript_id)


# Market tagging helpers
def get_market_tags(session: Session, market_ticker: str) -> List[str]:
    rows = session.scalars(select(MarketTag).where(MarketTag.market_ticker == market_ticker)).all()
    return [r.tag for r in rows]


def add_market_tags(session: Session, market_ticker: str, tag_names: Sequence[str]) -> List[str]:
    clean = sorted({(t or "").strip() for t in tag_names if t and (t or "").strip()})
    if not clean:
        return get_market_tags(session, market_ticker)
    existing = session.scalars(select(MarketTag).where(MarketTag.market_ticker == market_ticker)).all()
    existing_set = {r.tag for r in existing}
    to_add = [t for t in clean if t not in existing_set]
    for tag in to_add:
        session.add(MarketTag(market_ticker=market_ticker, tag=tag))
    session.flush()
    return get_market_tags(session, market_ticker)


def get_market_tags_bulk(session: Session, tickers: Sequence[str]) -> dict[str, List[str]]:
    """
    Fetch tags for many market tickers in a single query.
    """
    tickers_clean = sorted({str(t) for t in tickers if t})
    if not tickers_clean:
        return {}
    rows = session.scalars(select(MarketTag).where(MarketTag.market_ticker.in_(tickers_clean))).all()
    mapping: dict[str, List[str]] = {t: [] for t in tickers_clean}
    for r in rows:
        mapping.setdefault(r.market_ticker, []).append(r.tag)
    # Sort tag lists for stability
    for k in list(mapping.keys()):
        mapping[k] = sorted(list({*mapping.get(k, [])}))
    return mapping


# Trade journal helpers
def upsert_trade_entry(
    session: Session,
    *,
    market_ticker: str,
    event_ticker: str = "",
    title: str = "",
    word: str = "",
    note: str = "",
) -> None:
    existing = session.scalars(select(TradeEntry).where(TradeEntry.market_ticker == market_ticker)).first()
    if existing:
        # Update note and metadata; refresh played_at to now
        existing.event_ticker = event_ticker or existing.event_ticker
        existing.title = title or existing.title
        existing.word = word or existing.word
        if note is not None:
            existing.note = note
        existing.played_at = datetime.utcnow()
        session.add(existing)
        return
    entry = TradeEntry(
        market_ticker=market_ticker,
        event_ticker=event_ticker or "",
        title=title or "",
        word=word or "",
        note=note or "",
        played_at=datetime.utcnow(),
    )
    session.add(entry)


def set_trade_note(session: Session, market_ticker: str, note: str) -> None:
    entry = session.scalars(select(TradeEntry).where(TradeEntry.market_ticker == market_ticker)).first()
    if not entry:
        entry = TradeEntry(market_ticker=market_ticker, note=note or "", played_at=datetime.utcnow())
    else:
        entry.note = note or ""
    session.add(entry)


def list_trade_entries(
    session: Session,
    *,
    search: Optional[str] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> List[TradeEntry]:
    rows = session.scalars(select(TradeEntry)).all()
    results = rows
    if search:
        needle = search.lower()
        results = [
            r
            for r in results
            if needle in (r.market_ticker or "").lower()
            or needle in (r.event_ticker or "").lower()
            or needle in (r.title or "").lower()
            or needle in (r.word or "").lower()
            or needle in (r.note or "").lower()
        ]
    if start:
        results = [r for r in results if r.played_at >= start]
    if end:
        results = [r for r in results if r.played_at <= end]
    # Sort newest first
    results.sort(key=lambda r: r.played_at, reverse=True)
    return results

