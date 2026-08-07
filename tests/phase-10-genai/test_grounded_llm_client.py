"""
OmniWatch — Generative AI Layer
Component: Grounded LLM Client Tests
Phase: 10
Purpose: Unit tests for grounded_llm_client.py — mocks Ollama /api/generate,
         validates grounded JSON output, retry on hallucination, fallback on
         exhaustion, thinking block stripping, and concurrency control.
Inputs: None (uses fixtures from conftest.py)
Outputs: Test results via pytest
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from genai.grounded_llm_client import GroundedLLMClient
from genai.models import GroundedAnalysis, RootCauseObject


def _make_response(body: dict[str, Any]) -> MagicMock:
    """Create a mock httpx.Response — json() is sync in httpx."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = body
    resp.raise_for_status.return_value = None
    return resp


class TestGroundedLLMClientGenerate:
    """Tests for GroundedLLMClient.generate — the main generation method."""

    @pytest.mark.asyncio
    async def test_generate_valid_grounded_output(
        self,
        root_cause_factory: RootCauseObject,
        mock_ollama_response: dict[str, Any],
    ) -> None:
        """Valid grounded output returns GroundedAnalysis on first attempt."""
        client = GroundedLLMClient(
            ollama_url="http://mock:11434",
            model="qwen3:8b",
        )
        resp = _make_response(mock_ollama_response)

        with patch.object(client._client, "post", new_callable=AsyncMock, return_value=resp):
            result = await client.generate(root_cause_factory)

        assert isinstance(result, GroundedAnalysis)
        assert result.root_cause_entity == "postgresql-database"
        assert "postgresql-database" in result.impacted_entities

        await client.close()

    @pytest.mark.asyncio
    async def test_generate_retries_on_hallucination(
        self,
        root_cause_factory: RootCauseObject,
        mock_ollama_hallucinated_response: dict[str, Any],
        mock_ollama_response: dict[str, Any],
    ) -> None:
        """Hallucinated output triggers retry with corrected prompt, then succeeds."""
        client = GroundedLLMClient(
            ollama_url="http://mock:11434",
            model="qwen3:8b",
        )
        hallucinated_resp = _make_response(mock_ollama_hallucinated_response)
        grounded_resp = _make_response(mock_ollama_response)

        call_count = 0

        async def mock_post(*args: Any, **kwargs: Any) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return hallucinated_resp
            return grounded_resp

        with patch.object(client._client, "post", side_effect=mock_post):
            with patch("genai.grounded_llm_client.asyncio.sleep", new_callable=AsyncMock):
                result = await client.generate(root_cause_factory)

        assert isinstance(result, GroundedAnalysis)
        assert call_count == 2

        await client.close()

    @pytest.mark.asyncio
    async def test_generate_fallback_after_max_retries(
        self,
        root_cause_factory: RootCauseObject,
        mock_ollama_hallucinated_response: dict[str, Any],
    ) -> None:
        """Returns deterministic fallback when all retries produce hallucinated output."""
        client = GroundedLLMClient(
            ollama_url="http://mock:11434",
            model="qwen3:8b",
        )
        resp = _make_response(mock_ollama_hallucinated_response)

        with patch.object(client._client, "post", new_callable=AsyncMock, return_value=resp):
            with patch("genai.grounded_llm_client.asyncio.sleep", new_callable=AsyncMock):
                result = await client.generate(root_cause_factory)

        assert isinstance(result, GroundedAnalysis)
        assert result.confidence == 0.0
        assert "fallback" in result.summary.lower()
        assert result.root_cause_entity == root_cause_factory.root_cause_entity

        await client.close()

    @pytest.mark.asyncio
    async def test_generate_handles_json_parse_error(
        self,
        root_cause_factory: RootCauseObject,
    ) -> None:
        """Handles invalid JSON from LLM by retrying."""
        client = GroundedLLMClient(
            ollama_url="http://mock:11434",
            model="qwen3:8b",
        )
        bad_resp = _make_response({"response": "this is not json at all", "done": True})

        grounded_output = {
            "summary": "postgresql-database",
            "root_cause_entity": "postgresql-database",
            "confidence": 90.0,
            "recommended_actions": ["postgresql-database"],
            "impacted_entities": ["postgresql-database"],
            "reasoning": "postgresql-database",
        }
        good_resp = _make_response({"response": json.dumps(grounded_output), "done": True})

        call_count = 0

        async def mock_post(*args: Any, **kwargs: Any) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return bad_resp
            return good_resp

        with patch.object(client._client, "post", side_effect=mock_post):
            with patch("genai.grounded_llm_client.asyncio.sleep", new_callable=AsyncMock):
                result = await client.generate(root_cause_factory)

        assert isinstance(result, GroundedAnalysis)
        assert call_count == 2

        await client.close()

    @pytest.mark.asyncio
    async def test_generate_strips_thinking_blocks(
        self,
        root_cause_factory: RootCauseObject,
    ) -> None:
        """FM-1: Strips <thinking> blocks before JSON parsing."""
        client = GroundedLLMClient(
            ollama_url="http://mock:11434",
            model="qwen3:8b",
        )
        grounded_output = {
            "summary": "postgresql-database",
            "root_cause_entity": "postgresql-database",
            "confidence": 90.0,
            "recommended_actions": ["postgresql-database"],
            "impacted_entities": ["postgresql-database"],
            "reasoning": "postgresql-database",
        }
        with_thinking = (
            "<thinking>\nLet me analyze this...\n</thinking>\n"
            + json.dumps(grounded_output)
        )
        resp = _make_response({"response": with_thinking, "done": True})

        with patch.object(client._client, "post", new_callable=AsyncMock, return_value=resp):
            result = await client.generate(root_cause_factory)

        assert isinstance(result, GroundedAnalysis)
        assert result.root_cause_entity == "postgresql-database"

        await client.close()

    @pytest.mark.asyncio
    async def test_generate_strips_markdown_fences(
        self,
        root_cause_factory: RootCauseObject,
    ) -> None:
        """Handles LLM output wrapped in markdown code fences."""
        client = GroundedLLMClient(
            ollama_url="http://mock:11434",
            model="qwen3:8b",
        )
        grounded_output = {
            "summary": "postgresql-database",
            "root_cause_entity": "postgresql-database",
            "confidence": 90.0,
            "recommended_actions": ["postgresql-database"],
            "impacted_entities": ["postgresql-database"],
            "reasoning": "postgresql-database",
        }
        fenced = "```json\n" + json.dumps(grounded_output) + "\n```"
        resp = _make_response({"response": fenced, "done": True})

        with patch.object(client._client, "post", new_callable=AsyncMock, return_value=resp):
            result = await client.generate(root_cause_factory)

        assert isinstance(result, GroundedAnalysis)
        assert result.root_cause_entity == "postgresql-database"

        await client.close()

    @pytest.mark.asyncio
    async def test_generate_sends_think_false(
        self,
        root_cause_factory: RootCauseObject,
        mock_ollama_response: dict[str, Any],
    ) -> None:
        """Verifies think:false is sent to prevent thinking channel corruption."""
        client = GroundedLLMClient(
            ollama_url="http://mock:11434",
            model="qwen3:8b",
        )
        resp = _make_response(mock_ollama_response)

        with patch.object(client._client, "post", new_callable=AsyncMock, return_value=resp) as mock_post:
            await client.generate(root_cause_factory)

        call_kwargs = mock_post.call_args
        payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert payload is not None
        assert payload.get("think") is False

        await client.close()


class TestGroundedLLMClientParseJson:
    """Tests for GroundedLLMClient._parse_json_response static method."""

    def test_parse_valid_json(self) -> None:
        """Valid JSON string is parsed correctly."""
        data = {"key": "value"}
        result = GroundedLLMClient._parse_json_response(json.dumps(data))
        assert result == data

    def test_parse_json_with_fences(self) -> None:
        """JSON wrapped in markdown fences is parsed correctly."""
        data = {"key": "value"}
        fenced = "```json\n" + json.dumps(data) + "\n```"
        result = GroundedLLMClient._parse_json_response(fenced)
        assert result == data

    def test_parse_json_with_whitespace(self) -> None:
        """JSON with leading/trailing whitespace is parsed correctly."""
        data = {"key": "value"}
        result = GroundedLLMClient._parse_json_response("  \n" + json.dumps(data) + "\n  ")
        assert result == data

    def test_parse_json_with_thinking_blocks(self) -> None:
        """FM-1: JSON with <thinking> blocks is parsed correctly."""
        data = {"key": "value"}
        with_thinking = (
            "<thinking>\nLet me think about this...\n</thinking>\n"
            + json.dumps(data)
        )
        result = GroundedLLMClient._parse_json_response(with_thinking)
        assert result == data

    def test_parse_invalid_json_raises(self) -> None:
        """Invalid JSON raises JSONDecodeError."""
        with pytest.raises(json.JSONDecodeError):
            GroundedLLMClient._parse_json_response("not json at all")
