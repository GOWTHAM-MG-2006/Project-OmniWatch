# ---------------------------------------------------------------------------
# OmniWatch — Identity Service Dockerfile
# Phase: entry-point (Wave 1)
# Purpose: Container image for the FastAPI register/login/JWT service (:8012)
# Pattern: mirrors learning/Dockerfile (builder + non-root runtime,
#          PYTHONPATH=/app, curl healthcheck)
# ---------------------------------------------------------------------------

# == Stage 0: agent-builder — Linux exporter binary for bundles ============
# The /exporters/bundle zip ships this binary so iron/VM provisioning is one
# download (config + binary + install script). Stripped to keep it small.
FROM golang:1.23-alpine AS agent-builder

WORKDIR /build
COPY omniwatch-agent/go.mod omniwatch-agent/go.sum ./
RUN go mod download
COPY omniwatch-agent/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /agent-bin/omniwatch-agent ./cmd/agent

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

# -- Exporter binary shipped inside workspace bundles --
COPY --from=agent-builder /agent-bin/omniwatch-agent /opt/omniwatch-agent/omniwatch-agent
ENV OMNIWATCH_AGENT_BIN=/opt/omniwatch-agent/omniwatch-agent

# -- Ensure identity package is on Python path --
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# -- Runtime --
EXPOSE 8012

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8012/health || exit 1

CMD ["uvicorn", "identity.main:app", "--host", "0.0.0.0", "--port", "8012"]
