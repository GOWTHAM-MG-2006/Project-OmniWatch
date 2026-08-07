"""
OmniWatch — Generative AI Layer
Component: Settings (Pydantic v2 BaseSettings)
Phase: 10
Purpose: Centralized configuration for Ollama/vLLM LLM endpoint, ClickHouse,
         MinIO, Kafka, and API port — loaded from env vars / .env file.
Inputs: Environment variables (uppercase) and .env file
Outputs: Settings instance with typed, validated fields
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Pydantic v2 settings for the Generative AI layer.

    Field names map to uppercase env vars (e.g. ``ollama_url``
    reads from ``OLLAMA_URL``).  Defaults match the
    docker-compose.yml local-dev configuration.
    """

    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    # LLM Backend
    llm_backend: Literal["ollama", "vllm"] = "ollama"
    ollama_url: str = "http://localhost:11434"
    vllm_base: str = "http://localhost:8000"
    llm_model: str = "qwen3:8b"
    llm_max_tokens: int = 2048
    llm_temperature: float = 0.3
    llm_concurrency: int = 2

    # ClickHouse
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_db: str = "omniwatch"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""

    # MinIO
    minio_endpoint: str = "localhost:9010"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_secure: bool = False
    minio_auto_create: bool = True

    # Kafka
    kafka_bootstrap: str = Field(
        default="localhost:9092",
        validation_alias="KAFKA_BOOTSTRAP_SERVERS",
        description="Kafka bootstrap servers (repo-wide env convention)",
    )
    kafka_group: str = "omniwatch-genai-group"

    # FastAPI
    genai_api_port: int = 8020
