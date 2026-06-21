FROM python:3.14-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System deps for cryptography wheel
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libssl-dev libffi-dev && \
    rm -rf /var/lib/apt/lists/*

# ── Dependencies layer ─────────────────────────────────────────────────────────
FROM base AS deps
COPY pyproject.toml .
RUN pip install --upgrade pip && pip install .

# ── Final image ───────────────────────────────────────────────────────────────
FROM deps AS final
COPY app/ ./app/
COPY scheduler/ ./scheduler/
COPY migrations/ ./migrations/
COPY alembic.ini .

# Non-root user — chown happens before USER switches so the app can still
# write its demo-mode SQLite file (DATABASE_URL/Postgres is the real
# production path; this just keeps DEMO_MODE usable inside the container too)
RUN useradd -m -u 1001 hsmsvc && chown -R hsmsvc:hsmsvc /app
USER hsmsvc

EXPOSE 3005

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD python -c "import httpx; httpx.get('http://localhost:3005/api/sensec/hsm/v1/admin/health').raise_for_status()"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "3005", "--workers", "2"]
