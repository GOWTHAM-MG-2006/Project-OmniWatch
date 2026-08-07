# ---------------------------------------------------------------------------
# OmniWatch — Causal Graph Engine Dockerfile
# Phase: 7
# Purpose: Container image for the PyRCA-based causal root-cause service
# ---------------------------------------------------------------------------

# == Stage 1: builder — install Python dependencies ==========================
# PyRCA (sfr-pyrca 1.0.1) requires scikit-learn < 1.2 which ships no cp314
# wheels, so the image must be python:3.10-slim (NOT 3.14 like predictive).
FROM python:3.10-slim AS builder

WORKDIR /build

COPY causal/requirements.txt requirements.txt
# sfr-pyrca 1.0.1 declares javabridge (JDK-only) in its METADATA. We never
# import the Java analyzers, so install it --no-deps and install the rest of
# the requirements WITHOUT the sfr-pyrca line (pip 23+ would otherwise try to
# rebuild javabridge to "complete" the already-installed package).
RUN grep -v '^sfr-pyrca' requirements.txt > /tmp/requirements-nopca.txt \
    && pip install --no-cache-dir --prefix=/install --no-deps sfr-pyrca==1.0.1 \
    && pip install --no-cache-dir --prefix=/install -r /tmp/requirements-nopca.txt \
    && rm /tmp/requirements-nopca.txt

# == Stage 2: runtime — lean production image ================================
FROM python:3.10-slim AS runtime

# -- System dependencies (curl for healthcheck) --
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# -- Copy installed Python packages from builder --
COPY --from=builder /install /usr/local

# -- Application working directory --
WORKDIR /app

# -- Copy service code + shared storage package --
COPY causal/ ./causal/
COPY storage/ ./storage/

# -- Ensure causal package is on Python path --
ENV PYTHONPATH=/app

# -- Runtime --
WORKDIR /app/causal
EXPOSE 8008

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8008/health || exit 1

# The __main__ block in causal_engine.py wires the Kafka consumer thread
# BEFORE starting uvicorn; running uvicorn directly would skip consumer
# startup and the service would never publish omniwatch.incidents.causal.
CMD ["python", "causal_engine.py"]
