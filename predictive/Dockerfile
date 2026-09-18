# ---------------------------------------------------------------------------
# OmniWatch — Predictive Intelligence Service Dockerfile
# Phase: 6
# Purpose: Container image for the FastAPI-based anomaly detection service
# ---------------------------------------------------------------------------

# == Stage 1: builder — install Python dependencies ==========================
# SKLEARN SPLIT: numpy>=1.26 + scikit-learn>=1.5 here (py3.14); causal keeps
# numpy==1.23.5 + scikit-learn<1.2 (py3.10). Never mix the two sets.
FROM python:3.14-slim AS builder

WORKDIR /build

COPY predictive/requirements.txt requirements.txt
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# == Stage 2: runtime — lean production image ================================
FROM python:3.14-slim AS runtime

# -- System dependencies (curl for healthcheck) --
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# -- Copy installed Python packages from builder --
COPY --from=builder /install /usr/local

# -- Application working directory --
WORKDIR /app

# -- Copy service code + shared storage package + shared common lib (IND-7) --
COPY predictive/ ./predictive/
COPY storage/ ./storage/
COPY common/ ./common/

# -- Ensure predictive package is on Python path --
ENV PYTHONPATH=/app

# -- Runtime --
WORKDIR /app/predictive
EXPOSE 8007

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8007/health || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8007"]
