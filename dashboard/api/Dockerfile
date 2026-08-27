# ---------------------------------------------------------------------------
# OmniWatch — Dashboard API Dockerfile
# Phase: 11
# Purpose: Container image for the FastAPI-based dashboard API service
#          (43 endpoints serving ClickHouse/Neo4j/MinIO/Kafka data)
# ---------------------------------------------------------------------------

# == Stage 1: builder — install Python dependencies ==========================
FROM python:3.11-slim AS builder

WORKDIR /build

COPY dashboard/api/requirements.txt requirements.txt
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

# -- Copy service code (full dashboard/ package for module import) --
COPY dashboard/ ./dashboard/

# -- Ensure dashboard package is on Python path --
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# -- Runtime --
EXPOSE 8011

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8011/health || exit 1

CMD ["uvicorn", "dashboard.api.main:app", "--host", "0.0.0.0", "--port", "8011"]
