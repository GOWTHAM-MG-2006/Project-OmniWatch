# ---------------------------------------------------------------------------
# OmniWatch — Identity Service Dockerfile
# Phase: entry-point (Wave 1)
# Purpose: Container image for the FastAPI register/login/JWT service (:8012)
# Pattern: mirrors learning/Dockerfile (builder + non-root runtime,
#          PYTHONPATH=/app, curl healthcheck)
# ---------------------------------------------------------------------------

# == Stage 1: builder — install Python dependencies ==========================
FROM python:3.11-slim AS builder

WORKDIR /build

COPY identity/requirements.txt requirements.txt
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

# -- Copy service code (self-contained: no storage/ copy needed) --
COPY identity/ ./identity/

# -- Ensure identity package is on Python path --
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# -- Runtime --
EXPOSE 8012

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8012/health || exit 1

CMD ["uvicorn", "identity.main:app", "--host", "0.0.0.0", "--port", "8012"]
