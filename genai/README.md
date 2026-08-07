# genai — Generative AI Layer (Phase 10)

## Purpose

Implements the Generative AI layer for OmniWatch, providing:

1. **Grounded LLM Client (GAP4):** Async httpx client to Ollama that generates
   root-cause analysis strictly grounded in the input RootCauseObject. Uses
   entity validation to prevent hallucination.

2. **Output Validator (GAP4):** Post-generation entity validation ensuring every
   entity referenced in LLM output exists in the input RootCauseObject.

3. **Compliance Report Generator (GAP2):** Generates SOC2/ISO27001/HIPAA/PCI-DSS
   evidence packages from ClickHouse incident records and MinIO audit logs.

## Components

| File | Responsibility |
|------|----------------|
| `settings.py` | Pydantic v2 settings (Ollama, ClickHouse, MinIO) |
| `models.py` | Pydantic v2 models (RootCauseObject, GroundedAnalysis, ValidationReport) |
| `grounded_llm_client.py` | Async Ollama /api/generate client with grounded output |
| `output_validator.py` | Post-generation entity validation |
| `compliance_reporter.py` | Compliance report generation + FastAPI + CLI |

## Inputs

- **RootCauseObject** from causal engine (Phase 7)
- **ClickHouse** `omniwatch.incidents` table
- **MinIO** `omniwatch-audit-logs` bucket

## Outputs

- **GroundedAnalysis** — validated JSON from LLM
- **Compliance Markdown reports** → MinIO `omniwatch-audit-logs` bucket
- **FastAPI health endpoint** — GET /health

## How to Run

### Docker Compose

```bash
docker compose up -d genai-service
```

### Standalone (development)

```bash
# From the repo root
uvicorn genai.compliance_reporter:app --host 0.0.0.0 --port 8020
```

### CLI — Generate Report

```bash
python -m genai.compliance_reporter generate-report <incident_id> [report_type]
```

Report types:
- `Incident Response Evidence` (default)
- `Security Event Summary`
- `SLA Compliance Report`

### Health Check

```bash
curl http://localhost:8020/health
```

## Environment Variables

All variables have defaults matching `docker-compose.yml`.

| Variable | Default | Description |
|----------|---------|-------------|
| `OLLAMA_URL` | `http://localhost:11434` | Ollama API endpoint |
| `LLM_MODEL` | `qwen3:8b` | LLM model name |
| `LLM_MAX_TOKENS` | `2048` | Max tokens for generation |
| `LLM_TEMPERATURE` | `0.3` | Generation temperature |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host |
| `CLICKHOUSE_PORT` | `8123` | ClickHouse HTTP port |
| `MINIO_ENDPOINT` | `localhost:9010` | MinIO endpoint |
| `GENAI_API_PORT` | `8020` | Service API port |
