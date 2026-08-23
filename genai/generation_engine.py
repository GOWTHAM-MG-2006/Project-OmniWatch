"""
OmniWatch — Generative AI Layer
Component: Generation Engine (FastAPI)
Phase: 10
Purpose: Unified FastAPI app exposing /health, /stats, and /generate endpoints
         for on-demand artifact generation (summary, runbook, report, postmortem).
Inputs: HTTP requests with RootCauseObject payloads
Outputs: Generated artifacts via GroundedLLMClient, persisted to MinIO
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from genai.grounded_llm_client import GroundedLLMClient
from genai.incident_summary import IncidentSummaryGenerator
from genai.models import RootCauseObject
from genai.runbook_generator import RunbookGenerator

logger = logging.getLogger(__name__)

_GENAI_PORT = int(os.getenv("GENAI_API_PORT", "8020"))

_stats: dict[str, int] = {
    "requests": 0,
    "generated": 0,
    "errors": 0,
}
_client: GroundedLLMClient | None = None
_summary_gen: IncidentSummaryGenerator | None = None
_runbook_gen: RunbookGenerator | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _client, _summary_gen, _runbook_gen
    _client = GroundedLLMClient()
    _summary_gen = IncidentSummaryGenerator(_client)
    _runbook_gen = RunbookGenerator(_client)
    logger.info(json.dumps({"event": "generation_engine_started", "port": _GENAI_PORT}))
    yield
    if _client:
        await _client.close()
    logger.info(json.dumps({"event": "generation_engine_stopped"}))


app = FastAPI(
    title="OmniWatch Generation Engine",
    version="1.0.0",
    lifespan=lifespan,
)


class GenerateRequest(BaseModel):
    root_cause: RootCauseObject
    artifact_type: str = Field(
        default="summary",
        pattern="^(summary|runbook|report|postmortem)$",
    )

    model_config = {"extra": "forbid"}


class GenerateResponse(BaseModel):
    artifact_type: str
    incident_id: str
    content: str
    generated_at: str

    model_config = {"extra": "forbid"}


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "healthy",
        "service": "generation-engine",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/stats")
async def stats() -> dict[str, int]:
    return dict(_stats)


@app.post("/generate", response_model=GenerateResponse)
async def generate(req: GenerateRequest) -> GenerateResponse:
    _stats["requests"] += 1
    rc = req.root_cause

    if _summary_gen is None or _runbook_gen is None or _client is None:
        raise HTTPException(status_code=503, detail="Generation engine not ready")

    try:
        if req.artifact_type == "summary":
            result = await _summary_gen.generate(rc)
            content = result.summary
        elif req.artifact_type == "runbook":
            runbook = await _runbook_gen.generate(rc)
            content = "\n".join(runbook.steps)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"artifact_type '{req.artifact_type}' not yet implemented",
            )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001 — generate fallback
        _stats["errors"] += 1
        logger.error(json.dumps({"event": "generate_error", "error": str(exc)}))
        raise HTTPException(status_code=500, detail=str(exc))

    _stats["generated"] += 1
    return GenerateResponse(
        artifact_type=req.artifact_type,
        incident_id=rc.incident_id,
        content=content,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=_GENAI_PORT)
