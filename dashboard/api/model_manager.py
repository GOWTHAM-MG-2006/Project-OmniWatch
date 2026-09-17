"""
OmniWatch — Model Manager
Component: LLM Provider Abstraction
Phase: 11 (Dashboard + Continuous Learning)
Purpose: Abstract multiple LLM providers behind a unified interface with settings persistence
Inputs: Settings JSON from disk, prompts/messages from API endpoints
Outputs: LLM responses (streaming or non-streaming), connection test results
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Any, AsyncGenerator

import httpx
from fastapi.responses import StreamingResponse

_LOG = logging.getLogger("omniwatch.model_manager")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_SETTINGS_PATH = os.getenv(
    "MODEL_SETTINGS_PATH",
    str(Path(__file__).resolve().parents[2] / ".omniwatch" / "model-settings.json"),
)

_OLLAMA_LOCAL_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
_OLLAMA_CLOUD_URL = "https://api.ollama.com"
_OPENROUTER_URL = "https://openrouter.ai/api/v1"
_GROQ_URL = "https://api.groq.com/openai/v1"


# ---------------------------------------------------------------------------
# Provider enum
# ---------------------------------------------------------------------------

class ModelProvider(str, Enum):
    """Supported LLM providers."""
    OLLAMA = "ollama"
    OLLAMA_LOCAL = "ollama_local"
    OLLAMA_CLOUD = "ollama_cloud"
    OPENROUTER = "openrouter"
    GROQ = "groq"
    CUSTOM = "custom"


# ---------------------------------------------------------------------------
# Settings dataclass
# ---------------------------------------------------------------------------

@dataclass
class ModelSettings:
    """LLM provider configuration."""
    provider: ModelProvider = ModelProvider.OLLAMA
    model_name: str = "qwen3:8b"
    api_key: str = ""
    base_url: str = ""  # derived from provider if empty
    temperature: float = 0.7
    max_tokens: int = 2048

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["provider"] = self.provider.value
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ModelSettings":
        provider_val = data.get("provider", "ollama")
        try:
            provider = ModelProvider(provider_val)
        except ValueError:
            # Unknown provider strings fall back to canonical OLLAMA.
            # Legacy "ollama_local"/"ollama_cloud" are valid enum members
            # and are preserved as-is (treated as Ollama at dispatch).
            provider = ModelProvider.OLLAMA
        return cls(
            provider=provider,
            model_name=data.get("model_name", "qwen3:8b"),
            api_key=data.get("api_key", ""),
            base_url=data.get("base_url", ""),
            temperature=float(data.get("temperature", 0.7)),
            max_tokens=int(data.get("max_tokens", 2048)),
        )


# ---------------------------------------------------------------------------
# SSE helper
# ---------------------------------------------------------------------------

def sse_response(generator: AsyncGenerator[str, None]) -> StreamingResponse:
    """Wrap an async token generator into a Server-Sent Events StreamingResponse."""
    async def event_stream():
        async for token in generator:
            yield f"data: {json.dumps({'token': token, 'done': False})}\n\n"
        yield f"data: {json.dumps({'token': '', 'done': True})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# ModelManager
# ---------------------------------------------------------------------------

class ModelManager:
    """Unified LLM provider interface with settings persistence."""

    def __init__(self, settings_path: str | None = None) -> None:
        self._settings_path = Path(settings_path or _DEFAULT_SETTINGS_PATH)
        self._settings = self._load()

    # -- Persistence --------------------------------------------------------

    def _load(self) -> ModelSettings:
        """Load settings from disk; return defaults if missing."""
        if not self._settings_path.exists():
            _LOG.info("Settings file not found at %s — using defaults", self._settings_path)
            return ModelSettings()
        try:
            raw = json.loads(self._settings_path.read_text(encoding="utf-8"))
            return ModelSettings.from_dict(raw)
        except Exception:
            _LOG.warning("Failed to parse settings at %s — using defaults", self._settings_path)
            return ModelSettings()

    def get_settings(self) -> ModelSettings:
        """Return current in-memory settings."""
        return self._settings

    def update_settings(self, settings: ModelSettings) -> None:
        """Update in-memory settings and persist to disk."""
        self._settings = settings
        self._save()

    def _save(self) -> None:
        """Persist current settings to disk, creating parent dir if needed."""
        self._settings_path.parent.mkdir(parents=True, exist_ok=True)
        self._settings_path.write_text(
            json.dumps(self._settings.to_dict(), indent=2),
            encoding="utf-8",
        )
        _LOG.info("Settings saved to %s", self._settings_path)

    # -- Provider helpers ---------------------------------------------------

    @staticmethod
    def get_base_url(provider: ModelProvider) -> str:
        """Return the base URL for a given provider."""
        urls = {
            ModelProvider.OLLAMA: _OLLAMA_LOCAL_URL,
            ModelProvider.OLLAMA_LOCAL: _OLLAMA_LOCAL_URL,
            ModelProvider.OLLAMA_CLOUD: _OLLAMA_LOCAL_URL,
            ModelProvider.OPENROUTER: _OPENROUTER_URL,
            ModelProvider.GROQ: _GROQ_URL,
        }
        if provider == ModelProvider.CUSTOM:
            return ""
        return urls[provider]

    def _resolve_base_url(self) -> str:
        """Return base URL from settings or derived from provider."""
        provider = self._settings.provider
        if provider in (
            ModelProvider.OLLAMA,
            ModelProvider.OLLAMA_LOCAL,
            ModelProvider.OLLAMA_CLOUD,
        ):
            return _OLLAMA_LOCAL_URL
        if provider == ModelProvider.CUSTOM:
            return self._settings.base_url or ""
        if self._settings.base_url:
            return self._settings.base_url
        return self.get_base_url(provider)

    def _auth_headers(self) -> dict[str, str]:
        """Return Authorization header if api_key is set."""
        if self._settings.api_key:
            return {"Authorization": f"Bearer {self._settings.api_key}"}
        return {}

    # -- Unified API --------------------------------------------------------

    async def generate(self, prompt: str, stream: bool = False) -> str | AsyncGenerator[str, None]:
        """Unified text generation entry point.

        Non-streaming returns a plain string.
        Streaming returns an async generator of tokens.
        """
        messages = [{"role": "user", "content": prompt}]
        return await self.chat(messages, stream=stream)

    async def chat(
        self, messages: list[dict[str, str]], stream: bool = False
    ) -> str | AsyncGenerator[str, None]:
        """Unified chat entry point.

        Dispatches to the appropriate provider method.
        Non-streaming returns a plain string.
        Streaming returns an async generator of tokens.
        """
        provider = self._settings.provider
        if provider in (
            ModelProvider.OLLAMA,
            ModelProvider.OLLAMA_LOCAL,
            ModelProvider.OLLAMA_CLOUD,
        ):
            return await self._call_ollama(messages, stream)
        elif provider == ModelProvider.OPENROUTER:
            return await self._call_openrouter(messages, stream)
        elif provider == ModelProvider.GROQ:
            return await self._call_groq(messages, stream)
        elif provider == ModelProvider.CUSTOM:
            return await self._call_custom(messages, stream)
        else:
            raise ValueError(f"Unknown provider: {provider}")

    # -- Ollama (unified local daemon, no auth) -------------------------------

    async def _call_ollama(
        self, messages: list[dict[str, str]], stream: bool
    ) -> str | AsyncGenerator[str, None]:
        # Cloud models (e.g. minimax-m2.7:cloud) require Bearer auth against api.ollama.com.
        # When a key is pasted in the UI we call the cloud endpoint directly so
        # the user never needs `ollama signin` inside the container.
        is_cloud = self._settings.model_name.endswith(":cloud")
        if is_cloud and self._settings.api_key:
            base_url = _OLLAMA_CLOUD_URL
            headers: dict[str, str] = {"Authorization": f"Bearer {self._settings.api_key}"}
        else:
            base_url = self._resolve_base_url()
            headers = {}
        url = f"{base_url}/api/chat"
        body = {
            "model": self._settings.model_name,
            "messages": messages,
            "stream": stream,
            "think": False,
            "options": {
                "temperature": self._settings.temperature,
                "num_predict": self._settings.max_tokens,
            },
        }
        if stream:
            return self._stream_ollama_ndjson(url, body, auth_headers=headers)
        return await self._post_ollama_ndjson(url, body, auth_headers=headers)

    async def _call_ollama_local(
        self, messages: list[dict[str, str]], stream: bool
    ) -> str | AsyncGenerator[str, None]:
        return await self._call_ollama(messages, stream)

    # -- Ollama Cloud -------------------------------------------------------

    async def _call_ollama_cloud(
        self, messages: list[dict[str, str]], stream: bool
    ) -> str | AsyncGenerator[str, None]:
        return await self._call_ollama(messages, stream)

    # -- OpenRouter (OpenAI-compatible) -------------------------------------

    async def _call_openrouter(
        self, messages: list[dict[str, str]], stream: bool
    ) -> str | AsyncGenerator[str, None]:
        base_url = self._resolve_base_url().rstrip("/")
        url = f"{base_url}/chat/completions"
        body: dict[str, Any] = {
            "model": self._settings.model_name,
            "messages": messages,
            "stream": stream,
            "temperature": self._settings.temperature,
            "max_tokens": self._settings.max_tokens,
        }
        headers = self._auth_headers()
        if stream:
            return self._stream_openai_sse(url, body, auth_headers=headers)
        return await self._post_openai_json(url, body, auth_headers=headers)

    # -- Groq (OpenAI-compatible) -------------------------------------------

    async def _call_groq(
        self, messages: list[dict[str, str]], stream: bool
    ) -> str | AsyncGenerator[str, None]:
        base_url = self._resolve_base_url().rstrip("/")
        url = f"{base_url}/chat/completions"
        body: dict[str, Any] = {
            "model": self._settings.model_name,
            "messages": messages,
            "stream": stream,
            "temperature": self._settings.temperature,
            "max_tokens": self._settings.max_tokens,
        }
        headers = self._auth_headers()
        if stream:
            return self._stream_openai_sse(url, body, auth_headers=headers)
        return await self._post_openai_json(url, body, auth_headers=headers)

    async def _call_custom(
        self, messages: list[dict[str, str]], stream: bool
    ) -> str | AsyncGenerator[str, None]:
        base_url = self._resolve_base_url().rstrip("/")
        url = f"{base_url}/chat/completions"
        body: dict[str, Any] = {
            "model": self._settings.model_name,
            "messages": messages,
            "stream": stream,
            "temperature": self._settings.temperature,
            "max_tokens": self._settings.max_tokens,
        }
        headers = self._auth_headers()
        if stream:
            return self._stream_openai_sse(url, body, auth_headers=headers)
        return await self._post_openai_json(url, body, auth_headers=headers)

    # -- Low-level HTTP helpers ---------------------------------------------

    async def _post_ollama_ndjson(
        self, url: str, body: dict, auth_headers: dict[str, str]
    ) -> str:
        """POST to Ollama NDJSON endpoint, return accumulated text."""
        async with httpx.AsyncClient(timeout=240.0) as client:
            resp = await client.post(url, json=body, headers=auth_headers)
            resp.raise_for_status()
            data = resp.json()
            return data.get("message", {}).get("content", "")

    async def _stream_ollama_ndjson(
        self, url: str, body: dict, auth_headers: dict[str, str]
    ) -> AsyncGenerator[str, None]:
        """Stream Ollama NDJSON — each line is a JSON object with message.content."""
        async with httpx.AsyncClient(timeout=240.0) as client:
            async with client.stream("POST", url, json=body, headers=auth_headers) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if not line.strip():
                        continue
                    try:
                        chunk = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if chunk.get("done", False):
                        break
                    content = chunk.get("message", {}).get("content", "")
                    if content:
                        yield content

    async def _post_openai_json(
        self, url: str, body: dict, auth_headers: dict[str, str]
    ) -> str:
        """POST to OpenAI-compatible endpoint, return accumulated text."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(url, json=body, headers=auth_headers)
            resp.raise_for_status()
            data = resp.json()
            choices = data.get("choices", [])
            if choices:
                return choices[0].get("message", {}).get("content", "")
            return ""

    async def _stream_openai_sse(
        self, url: str, body: dict, auth_headers: dict[str, str]
    ) -> AsyncGenerator[str, None]:
        """Stream OpenAI-compatible SSE — lines starting with 'data: '."""
        async with httpx.AsyncClient(timeout=240.0) as client:
            async with client.stream("POST", url, json=body, headers=auth_headers) as resp:
                resp.raise_for_status()
                async for raw_line in resp.aiter_lines():
                    line = raw_line.strip()
                    if not line or not line.startswith("data: "):
                        continue
                    payload = line[len("data: "):]
                    if payload.strip() == "[DONE]":
                        break
                    try:
                        chunk = json.loads(payload)
                    except json.JSONDecodeError:
                        continue
                    choices = chunk.get("choices", [])
                    if choices:
                        delta = choices[0].get("delta", {})
                        content = delta.get("content", "")
                        if content:
                            yield content

    # -- Connection test ----------------------------------------------------

    async def test_connection(self, settings: ModelSettings | None = None) -> dict[str, Any]:
        """Test connection to the configured (or overridden) provider.

        When *settings* is provided the test runs against those values
        without persisting anything.  The response always reflects the
        effective provider/model that was actually tested.

        Returns:
            dict with keys: success, provider, model, latency_ms, error (optional)
        """
        prev_settings = self._settings
        if settings is not None:
            self._settings = settings
        try:
            active = self._settings
            provider_name = active.provider.value
            model = active.model_name
            test_messages = [{"role": "user", "content": "Say 'ok' in one word."}]

            start = time.monotonic()
            try:
                # Fast path for local Ollama: check tags, no full generate needed
                # — avoids 80-180s cold-load for a simple "Say ok" test
                if provider_name in ("ollama", "ollama_local", "ollama_cloud") and not model.endswith(":cloud"):
                    try:
                        async with httpx.AsyncClient(timeout=5.0) as client:
                            resp = await client.get(f"{_OLLAMA_LOCAL_URL}/api/tags")
                            resp.raise_for_status()
                            data = resp.json()
                            raw = data.get("models", []) if isinstance(data, dict) else []
                            names = [m.get("name", "") for m in raw if isinstance(m, dict)]
                            if any(model == n or model in n or n in model for n in names):
                                latency_ms = round((time.monotonic() - start) * 1000)
                                return {
                                    "success": True,
                                    "provider": provider_name,
                                    "model": model,
                                    "latency_ms": latency_ms,
                                    "response_preview": "model available locally",
                                }
                    except Exception:
                        pass
                result = await self.chat(test_messages, stream=False)
            except Exception as exc:
                latency_ms = round((time.monotonic() - start) * 1000)
                err_msg = str(exc)
                if "401" in err_msg and model.endswith(":cloud"):
                    err_msg += " — paste your Ollama API key from ollama.com/settings/keys into the Ollama API Key field on this page and Save."
                return {
                    "success": False,
                    "provider": provider_name,
                    "model": model,
                    "latency_ms": latency_ms,
                    "error": err_msg,
                }
        finally:
            self._settings = prev_settings
