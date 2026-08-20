# ---------------------------------------------------------------------------
# OmniWatch — Incident Prioritization Engine Dockerfile
# Phase: 8
# Purpose: Container image for the FastAPI-based incident prioritization service
#          (severity classification → dedup → publish to omniwatch.incidents.created)
# ---------------------------------------------------------------------------

# == Stage 1: builder — install Python dependencies ==========================
FROM python:3.11-slim AS builder

WORKDIR /build

COPY prioritization/requirements.txt requirements.txt
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# == Stage 2: runtime — lean production image ================================
FROM python:3.11-slim AS runtime

# -- System dependencies (curl for healthcheck) --
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# -- Copy installed Python packages from builder --
COPY --from=builder /install /usr/local

# -- Application working directory --
WORKDIR /app

# -- Copy service code + shared storage package --
COPY prioritization/ ./prioritization/
COPY storage/ ./storage/
COPY ingestion/ ./ingestion/

# -- Ensure prioritization package is on Python path --
ENV PYTHONPATH=/app

# -- Runtime --
WORKDIR /app/prioritization
EXPOSE 8009

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8009/health || exit 1

CMD ["uvicorn", "prioritization.prioritization_engine:app", "--host", "0.0.0.0", "--port", "8009"]
