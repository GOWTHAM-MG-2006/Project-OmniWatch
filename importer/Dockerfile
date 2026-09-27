FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY importer/requirements.txt ./importer/requirements.txt
RUN pip install --no-cache-dir -r importer/requirements.txt

COPY importer/ ./importer/

EXPOSE 4320

CMD ["python", "-m", "uvicorn", "importer.main:app", "--host", "0.0.0.0", "--port", "4320"]
