FROM python:3.11-slim

WORKDIR /app

# Install Kafka dependencies
COPY ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY ingestion/kafka_bus.py .
COPY ingestion/stream_processor.py .

# Run the stream processor
ENTRYPOINT ["python", "stream_processor.py"]
