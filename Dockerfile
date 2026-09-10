# Bibliagraphia API + UI.
#
# A single FastAPI app serves both the JSON API (app/api.py) and the
# single-page frontend (app/static/), so the whole thing is one container on
# port 8000. The DuckDB graph is built from the bundled JSON at image build
# time and also auto-rebuilt on first run if the volume is empty.
FROM python:3.12-slim

# Keep the image lean.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install Python dependencies first (better layer caching).
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the repo in its real layout so REPO_ROOT (resolved from __file__ in
# app/api.py and scripts/build_db.py) points at /app and the relative
# paths (data/, app/static/, scripts/) all resolve.
COPY app/        /app/app/
COPY scripts/    /app/scripts/
COPY bibliagraphia/ /app/bibliagraphia/
COPY sql/        /app/sql/
COPY data/       /app/data/

# Build the DuckDB graph at image time so the container starts fast and
# works with no mounted volume. The DB lands at /app/data/bible.db (the
# REPO_ROOT fallback the API uses when BIBLE_DB_PATH isn't set).
RUN python /app/scripts/build_db.py

EXPOSE 8000

# Serve the API + UI. uvicorn is a runtime dep (uvicorn[standard]).
# BIBLE_DB_PATH points at the mounted volume when one is supplied via
# compose; if absent, the image-built /app/data/bible.db is used.
CMD ["sh", "-c", "BIBLE_DB_PATH=${BIBLE_DB_PATH:-/app/data/bible.db} exec uvicorn app.api:app --host 0.0.0.0 --port 8000"]
