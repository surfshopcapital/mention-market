from datetime import datetime
from typing import List

from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String, Table, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base


transcript_tag_association = Table(
    "transcript_tag_association",
    Base.metadata,
    Column("transcript_id", ForeignKey("transcripts.id", ondelete="CASCADE"), primary_key=True),
    Column("tag_id", ForeignKey("tags.id", ondelete="CASCADE"), primary_key=True),
)


class Transcript(Base):
    __tablename__ = "transcripts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    original_filename: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    storage_location: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    text_content: Mapped[str] = mapped_column(Text, nullable=False)
    word_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    estimated_minutes: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    uploaded_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    file_type: Mapped[str] = mapped_column(String(16), nullable=False, default="txt")
    notes: Mapped[str] = mapped_column(Text, nullable=False, default="")

    tags: Mapped[List["Tag"]] = relationship(
        "Tag",
        secondary=transcript_tag_association,
        back_populates="transcripts",
        lazy="joined",
    )


class Tag(Base):
    __tablename__ = "tags"
    __table_args__ = (UniqueConstraint("name", name="uq_tag_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)

    transcripts: Mapped[List[Transcript]] = relationship(
        "Transcript",
        secondary=transcript_tag_association,
        back_populates="tags",
        lazy="selectin",
    )


