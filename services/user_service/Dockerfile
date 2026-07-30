# ---------------------------------------------------------------------------
# OmniWatch — User Service Dockerfile
# Phase: 1
# Purpose: Container image for the FastAPI-based user management service
# ---------------------------------------------------------------------------

FROM python:3.11-slim

# -- System dependencies --
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# -- Application working directory --
WORKDIR /app

# -- Install Python dependencies --
COPY services/user_service/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# -- Copy service code --
COPY services/ ./services/

# -- Ensure shared services package is on Python path --
ENV PYTHONPATH=/app

# -- Runtime --
WORKDIR /app/services/user_service
EXPOSE 8001

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl --fail http://localhost:8001/health || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
