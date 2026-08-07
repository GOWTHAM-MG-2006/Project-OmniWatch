# ---------------------------------------------------------------------------
# OmniWatch — Generative AI Layer Dockerfile
# Phase: 10
# Purpose: Container image for the GenAI service (grounded LLM client,
#          compliance report generator, output validator)
# ---------------------------------------------------------------------------

FROM python:3.11-slim AS runtime

# -- System dependencies (curl for healthcheck) --
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# -- Application working directory --
WORKDIR /app

# -- Copy service code --
COPY genai/ ./genai/

# -- Install dependencies --
COPY genai/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# -- Ensure genai package is on Python path --
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# -- Runtime --
EXPOSE 8020

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8020/health || exit 1

CMD ["uvicorn", "genai.generation_engine:app", "--host", "0.0.0.0", "--port", "8020"]
