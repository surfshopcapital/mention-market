from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from .config import get_database_url


class Base(DeclarativeBase):
    pass


_ENGINE = create_engine(get_database_url(), future=True, pool_pre_ping=True)
_SessionFactory = sessionmaker(bind=_ENGINE, autoflush=False, autocommit=False, expire_on_commit=False, class_=Session)


@contextmanager
def get_session() -> Iterator[Session]:
    session = _SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db() -> None:
    # Import models to register metadata
    from . import models as _  # noqa: F401

    Base.metadata.create_all(_ENGINE)


