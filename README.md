## Kalshi Mention Markets – Transcript Toolkit

A multi-page Streamlit app to prepare, analyze, and trade on Kalshi mention markets using transcript analysis. It provides:

- Transcript Analysis: upload/select transcripts and compute deterministic keyword metrics
- Transcript Library: cloud-backed transcript store with tagging and metadata

### Tech stack

- Python 3.11+
- Streamlit (multi-page app)
- PostgreSQL (cloud-hosted)
- SQLAlchemy ORM
- python-dotenv for local `.env`
- pypdf, python-docx for parsing; pandas for tabular stats

### Project structure

```
app.py
pages/
  1_Transcript_Analysis.py
  2_Transcript_Library.py
src/
  __init__.py
  config.py
  db.py
  models.py
  storage.py
  text_processing.py
  ui_components.py
requirements.txt
tests/
  test_text_processing.py
```

### Environment configuration

The app requires a PostgreSQL connection string in `DATABASE_URL`. For local development:

1. Create a `.env` file:

```
DATABASE_URL=postgresql+psycopg2://USER:PASSWORD@HOST:PORT/DBNAME
DEFAULT_WPM=150
```

2. Install dependencies and run:

```
pip install -r requirements.txt
streamlit run app.py
```

On Streamlit Cloud, set `DATABASE_URL` in `Secrets`. The app will automatically use `st.secrets["DATABASE_URL"]` when available.

### Data model

- `Transcript`: id, title, original_filename, storage_location, text_content, word_count, estimated_minutes, uploaded_at, file_type, notes
- `Tag`: id, name (unique)
- Many-to-many association: `transcript_tag_association`

The library stores parsed text and metadata in PostgreSQL. Original file bytes are not persisted; later you can extend `storage_location` to use an object store (e.g., S3).

### Deterministic metrics

Given selected transcripts and user-provided keywords/phrases, the app computes:
- Total mentions per keyword (case-insensitive)
- Average mentions per transcript
- Average relative position of mentions (as a percentage of transcript length in words)
- Percentage of transcripts containing at least one mention
- Average transcript length (words and estimated minutes at configurable WPM)

### Tests

Run tests with:

```
pytest -q
```

### Deployment notes

- Ensure `DATABASE_URL` is reachable from the hosting environment (e.g., Neon/Railway/Render).
- The schema is created automatically at startup via SQLAlchemy metadata.
- Streamlit Cloud users: set the secret in the console and redeploy.


