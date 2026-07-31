# ---------------------------------------------------------------------------
# OmniWatch — Feature Store API Dockerfile
# Phase: 4
# Purpose: Container image for the FastAPI-based Feature Store API service
# ---------------------------------------------------------------------------

FROM python:3.11-slim

# -- System dependencies --
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# -- Application working directory --
WORKDIR /app

# -- Install Python dependencies --
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# -- Copy service code --
COPY . .

# -- Runtime --
EXPOSE 8005

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl --fail http://localhost:8005/health || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8005"]
