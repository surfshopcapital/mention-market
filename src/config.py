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


def get_kalshi_api_base_url() -> str:
    """
    Returns the Kalshi API base URL.
    Prefer Streamlit secrets, then environment variable KALSHI_API_BASE_URL,
    defaulting to https://api.elections.kalshi.com for production.
    """
    secret_val = _get_streamlit_secret("KALSHI_API_BASE_URL")
    if secret_val:
        return secret_val.strip()
    env_val = os.getenv("KALSHI_API_BASE_URL")
    if env_val:
        return env_val.strip()
    return "https://api.elections.kalshi.com"


def get_kalshi_api_key_id() -> str:
    """
    Returns the Kalshi API Key ID.
    Must be provided via Streamlit secrets (KALSHI_API_KEY_ID) or env var.
    """
    secret_val = _get_streamlit_secret("KALSHI_API_KEY_ID")
    if secret_val:
        return secret_val.strip()
    env_val = os.getenv("KALSHI_API_KEY_ID")
    if env_val:
        return env_val.strip()
    raise RuntimeError("KALSHI_API_KEY_ID is not set. Add to Streamlit secrets or environment.")


def get_kalshi_private_key_pem() -> str:
    """
    Returns the Kalshi RSA private key PEM for signing.
    Must be provided via Streamlit secrets (KALSHI_PRIVATE_KEY) or env var.
    """
    secret_val = _get_streamlit_secret("KALSHI_PRIVATE_KEY")
    if secret_val:
        return secret_val
    env_val = os.getenv("KALSHI_PRIVATE_KEY")
    if env_val:
        return env_val
    raise RuntimeError("KALSHI_PRIVATE_KEY is not set. Add to Streamlit secrets or environment.")


