import os
from typing import Optional

from dotenv import load_dotenv

# Load local .env if present
load_dotenv()


def _get_streamlit_secret(key: str) -> Optional[str]:
    try:
        import streamlit as st  # type: ignore

        return st.secrets.get(key)  # type: ignore[no-any-return]
    except Exception:
        return None


def _normalize_db_url(url: str) -> str:
    """
    Normalize common DB URL issues:
    - postgres://  -> postgresql://
    - ensure sslmode=require is present
    """
    value = (url or "").strip()
    if value.startswith("postgres://"):
        value = "postgresql://" + value[len("postgres://") :]
    # Prefer explicit driver not required, but allowed. Leave as postgresql://.
    # Ensure sslmode=require
    if "sslmode=" not in value:
        sep = "&" if "?" in value else "?"
        value = f"{value}{sep}sslmode=require"
    return value


def get_database_url() -> str:
    # Prefer Streamlit secrets on Cloud
    secret_val = _get_streamlit_secret("DATABASE_URL")
    if secret_val:
        return _normalize_db_url(secret_val)
    env_val = os.getenv("DATABASE_URL")
    if env_val:
        return _normalize_db_url(env_val)
    raise RuntimeError(
        "DATABASE_URL is not set. Provide it via environment variable or Streamlit secrets."
    )


def get_default_words_per_minute() -> int:
    raw = os.getenv("DEFAULT_WPM", "150").strip()
    try:
        val = int(raw)
    except ValueError:
        val = 150
    return max(60, min(val, 400))


