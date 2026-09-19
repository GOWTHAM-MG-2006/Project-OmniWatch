"""
OmniWatch — Unified Storage Layer
Component: Configuration
Phase: 5
Purpose: Centralized connection configuration for the three storage backends
         (ClickHouse, Neo4j, MinIO) plus the shared Kafka-topic, MinIO-bucket,
         service-port, LLM, and cloud-default registries, loaded from
         environment variables with defaults that match docker-compose.yml
         for local development.
Inputs: OMNIWATCH_* env vars (primary) with old bare names as deprecated
        fallback, e.g. OMNIWATCH_KAFKA_BOOTSTRAP_SERVERS -> KAFKA_BOOTSTRAP_SERVERS
Outputs: A StorageConfig dataclass instance (via StorageConfig.from_env())
         consumed by the storage/clickhouse, storage/neo4j, and storage/minio
         clients in later Phase 5 tasks; module-level KAFKA_TOPICS /
         MINIO_BUCKETS / SERVICE_PORTS registries consumed by all services.
"""

from __future__ import annotations

import logging
import os
from dataclasses import MISSING, dataclass, field, fields
from typing import Any, Dict

_log = logging.getLogger("omniwatch.storage.config")


def _env(primary: str, fallback: str, default: str) -> str:
    """Resolve ``OMNIWATCH_*`` primary with old bare-name fallback.

    Backwards-compatibility contract (DYN-python lane): every lookup is
    ``os.getenv("OMNIWATCH_X", os.getenv("OLD_BARE_NAME", "<today's default>"))``
    so existing compose files, K8s manifests, and tests that set the old bare
    names keep working unchanged.
    """
    return os.getenv(primary, os.getenv(fallback, default))


def _env_int(primary: str, fallback: str, default: int) -> int:
    """Typed int variant of :func:`_env` (fail-fast ValueError on garbage)."""
    return int(_env(primary, fallback, str(default)))


def _env_float(primary: str, fallback: str, default: float) -> float:
    """Typed float variant of :func:`_env` (fail-fast ValueError on garbage)."""
    return float(_env(primary, fallback, str(default)))


def _env_bool(primary: str, fallback: str, default: bool) -> bool:
    """Typed bool variant of :func:`_env` (accepts 1/true/yes/on)."""
    return _env(primary, fallback, str(default)).lower() in ("1", "true", "yes", "on")


# --------------------------------------------------------------------------- #
# Shared registries — single source of truth for topic / bucket / port names.
# Values are resolved from the environment at import time (env-overridable);
# services that need runtime re-resolution should call the ``*_from_env()``
# helpers or read StorageConfig fields instead. No new topics/buckets/tables
# may be invented here — this maps the EXISTING names to env vars.
# --------------------------------------------------------------------------- #

def kafka_topics_from_env() -> Dict[str, str]:
    """Return the Kafka topic registry with env overrides applied."""
    return {
        "anomalies": _env(
            "OMNIWATCH_KAFKA_TOPICS_ANOMALIES", "KAFKA_TOPIC_ANOMALIES",
            "omniwatch.anomalies.detected",
        ),
        "causal": _env(
            "OMNIWATCH_KAFKA_TOPICS_CAUSAL", "KAFKA_TOPIC_CAUSAL",
            "omniwatch.incidents.causal",
        ),
        "created": _env(
            "OMNIWATCH_KAFKA_TOPICS_CREATED", "KAFKA_TOPIC_CREATED",
            "omniwatch.incidents.created",
        ),
        "actions": _env(
            "OMNIWATCH_KAFKA_TOPICS_ACTIONS", "KAFKA_TOPIC_ACTIONS",
            "omniwatch.remediation.actions",
        ),
        "security": _env(
            "OMNIWATCH_KAFKA_TOPICS_SECURITY", "KAFKA_TOPIC_SECURITY",
            "omniwatch.security.events",
        ),
        "features": _env(
            "OMNIWATCH_KAFKA_TOPICS_FEATURES", "KAFKA_TOPIC_FEATURES",
            "omniwatch.features.windowed",
        ),
        "entities": _env(
            "OMNIWATCH_KAFKA_TOPICS_ENTITIES", "KAFKA_TOPIC_ENTITIES",
            "omniwatch.entities.resolved",
        ),
        "summaries": _env(
            "OMNIWATCH_KAFKA_TOPICS_SUMMARIES", "KAFKA_TOPIC_SUMMARIES",
            "omniwatch.generated.summaries",
        ),
        "runbooks": _env(
            "OMNIWATCH_KAFKA_TOPICS_RUNBOOKS", "KAFKA_TOPIC_RUNBOOKS",
            "omniwatch.generated.runbooks",
        ),
        "reports": _env(
            "OMNIWATCH_KAFKA_TOPICS_REPORTS", "KAFKA_TOPIC_REPORTS",
            "omniwatch.generated.reports",
        ),
    }


def minio_buckets_from_env() -> Dict[str, str]:
    """Return the MinIO bucket registry with env overrides applied."""
    return {
        "telemetry": _env(
            "OMNIWATCH_MINIO_BUCKETS_TELEMETRY", "MINIO_BUCKET_TELEMETRY",
            "omniwatch-telemetry-archive",
        ),
        "incidents": _env(
            "OMNIWATCH_MINIO_BUCKETS_INCIDENTS", "MINIO_BUCKET_INCIDENTS",
            "omniwatch-incidents",
        ),
        "audit": _env(
            "OMNIWATCH_MINIO_BUCKETS_AUDIT", "MINIO_BUCKET_AUDIT",
            "omniwatch-audit-logs",
        ),
        "runbooks": _env(
            "OMNIWATCH_MINIO_BUCKETS_RUNBOOKS", "MINIO_BUCKET_RUNBOOKS",
            "omniwatch-runbooks",
        ),
        "ml": _env(
            "OMNIWATCH_MINIO_BUCKETS_ML", "MINIO_BUCKET_ML",
            "omniwatch-ml-datasets",
        ),
        "dashboards": _env(
            "OMNIWATCH_MINIO_BUCKETS_DASHBOARDS", "MINIO_BUCKET_DASHBOARDS",
            "omniwatch-dashboards",
        ),
    }


def service_ports_from_env() -> Dict[str, int]:
    """Return the service serve-port registry with env overrides applied."""
    return {
        "predictive": _env_int(
            "OMNIWATCH_PREDICTIVE_PORT", "PREDICTIVE_API_PORT", 8007),
        "causal": _env_int(
            "OMNIWATCH_CAUSAL_PORT", "CAUSAL_API_PORT", 8008),
        "prioritization": _env_int(
            "OMNIWATCH_PRIORITIZATION_PORT", "PRIORITIZATION_API_PORT", 8009),
        "orchestration": _env_int(
            "OMNIWATCH_ORCHESTRATION_PORT", "ORCHESTRATION_API_PORT", 8010),
        "dashboard": _env_int(
            "OMNIWATCH_DASHBOARD_PORT", "DASHBOARD_PORT", 8011),
        "feature_store": _env_int(
            "OMNIWATCH_FEATURE_STORE_PORT", "FEATURE_STORE_API_PORT", 8005),
        "genai": _env_int(
            "OMNIWATCH_GENAI_PORT", "GENAI_API_PORT", 8020),
        "learning": _env_int(
            "OMNIWATCH_LEARNING_PORT", "LEARNING_API_PORT", 8030),
    }


KAFKA_TOPICS: Dict[str, str] = kafka_topics_from_env()
MINIO_BUCKETS: Dict[str, str] = minio_buckets_from_env()
SERVICE_PORTS: Dict[str, int] = service_ports_from_env()


# --------------------------------------------------------------------------- #
# Workspace isolation helpers — ENTRY-2 contract (docs/workspace-isolation.md).
# These EXTEND the registries above; KAFKA_TOPICS / MINIO_BUCKETS values are
# never replaced. The `default` workspace maps to today's bare names so
# single-tenant behavior stays byte-identical; every other slug gets the
# isolated mapping. Code must match docs/workspace-isolation.md verbatim.
# --------------------------------------------------------------------------- #

#: Bootstrap constants: first-boot user + workspace (zero-config legacy path).
DEFAULT_WORKSPACE_SLUG = "default"
DEFAULT_BOOTSTRAP_USER_EMAIL = "local-dev"

#: Kafka topic prefix for non-default workspaces: ws_<slug>.omniwatch.*
WORKSPACE_TOPIC_PREFIX_TEMPLATE = "ws_{slug}."


def workspace_kafka_prefix(slug: str) -> str:
    """Kafka topic prefix for a workspace slug.

    Non-default: ``ws_<slug>.`` (prepended to the bare topic name).
    ``default``: ``""`` (bare names, e.g. ``omniwatch.anomalies.detected``).
    """
    if slug == DEFAULT_WORKSPACE_SLUG:
        return ""
    return WORKSPACE_TOPIC_PREFIX_TEMPLATE.format(slug=slug)


def workspace_topic(slug: str, base_topic: str) -> str:
    """Scope a bare Kafka topic name to a workspace.

    ``workspace_topic("acme", "omniwatch.anomalies.detected")`` →
    ``"ws_acme.omniwatch.anomalies.detected"``; ``default`` returns the
    bare topic unchanged.
    """
    return f"{workspace_kafka_prefix(slug)}{base_topic}"


def workspace_topics(slug: str) -> Dict[str, str]:
    """Return the full KAFKA_TOPICS registry scoped to a workspace."""
    return {key: workspace_topic(slug, base)
            for key, base in KAFKA_TOPICS.items()}


def workspace_database(slug: str, base_db: str = "omniwatch") -> str:
    """ClickHouse database for a workspace slug.

    Non-default: ``omniwatch_ws_<slug>``; ``default``: the bare ``omniwatch``.
    """
    if slug == DEFAULT_WORKSPACE_SLUG:
        return base_db
    return f"{base_db}_ws_{slug}"


def workspace_prefix(slug: str) -> str:
    """MinIO key prefix for a workspace slug.

    Non-default: ``workspaces/<slug>/`` inside the EXISTING buckets (no new
    buckets); ``default``: ``""`` (bare keys, today's layout).
    """
    if slug == DEFAULT_WORKSPACE_SLUG:
        return ""
    return f"workspaces/{slug}/"


def workspace_k8s_namespace(slug: str) -> str:
    """K8s namespace mapping for a workspace slug (documented, NOT provisioned).

    Non-default: ``omniwatch-ws-<slug>``; ``default``: ``omniwatch``.
    """
    if slug == DEFAULT_WORKSPACE_SLUG:
        return "omniwatch"
    return f"omniwatch-ws-{slug}"


def workspace_neo4j_match(slug: str) -> str:
    """Neo4j scoping-node match properties for a workspace slug.

    Entity nodes carry ``BELONGS_TO`` edges to ``(:Workspace{slug: ...})``.
    """
    return slug


@dataclass(init=False)
class StorageConfig:
    """Connection parameters for all Unified Storage Layer backends.

    Every field defaults to the value from docker-compose.yml (local dev),
    and is overridable via its OMNIWATCH_* variable (primary) or the old
    bare name (deprecated fallback). Never store secrets in code — passwords
    are only ever env-var defaults for the local dev compose stack.

    The constructor also accepts the legacy ``clickhouse_port=`` keyword
    (alias for ``clickhouse_http_port=``) so existing callers and tests keep
    working unchanged.
    """

    # ------------------------------------------------------------------ #
    # ClickHouse (omniwatch-clickhouse) — HTTP 8123 / native 9000
    # ------------------------------------------------------------------ #
    clickhouse_host: str = "localhost"
    clickhouse_http_port: int = 8123
    clickhouse_native_port: int = 9000
    clickhouse_db: str = "omniwatch"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_timezone: str = "UTC"

    # ------------------------------------------------------------------ #
    # Neo4j (omniwatch-neo4j) — bolt 7687 / browser 7474
    # ------------------------------------------------------------------ #
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "omniwatch"

    # ------------------------------------------------------------------ #
    # MinIO (omniwatch-minio) — API :9010, console :9001
    # ------------------------------------------------------------------ #
    minio_endpoint: str = "localhost:9010"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_secure: bool = False
    minio_console_port: int = 9001

    # ------------------------------------------------------------------ #
    # Shared registries (topics / buckets / cross-service endpoints)
    # ------------------------------------------------------------------ #
    kafka_bootstrap_servers: str = "localhost:9092"
    opa_url: str = "http://localhost:8181"
    llm_url: str = "http://localhost:11434"
    llm_model: str = "qwen3:8b"
    vllm_base: str = "http://localhost:8000"
    learning_url: str = "http://localhost:8030"
    genai_url: str = "http://localhost:8020"
    orchestration_url: str = "http://localhost:8010"
    simulation_endpoint: str = "http://localhost:8010/api/v1/simulate-action"
    default_cloud_provider: str = "unknown"
    default_region: str = "unknown"

    # ------------------------------------------------------------------ #
    # Env-var names per backend (single source of truth for from_env)
    # ------------------------------------------------------------------ #
    _env_map: Dict[str, str] = field(default_factory=dict, init=False, repr=False)

    def __init__(self, **kwargs: Any) -> None:
        # Legacy alias: clickhouse_port=... means the HTTP port, unless the
        # explicit clickhouse_http_port= is also given (explicit wins).
        if "clickhouse_port" in kwargs and "clickhouse_http_port" not in kwargs:
            kwargs["clickhouse_http_port"] = kwargs.pop("clickhouse_port")
        else:
            kwargs.pop("clickhouse_port", None)
        for f in fields(type(self)):
            if f.name in kwargs:
                setattr(self, f.name, kwargs.pop(f.name))
            elif f.default is not MISSING:
                setattr(self, f.name, f.default)
            elif getattr(f, "default_factory", MISSING) is not MISSING:  # type: ignore[attr-defined]
                setattr(self, f.name, f.default_factory())  # type: ignore[attr-defined]
            else:
                raise TypeError(
                    f"StorageConfig missing required argument: {f.name!r}")
        if kwargs:
            raise TypeError(
                "StorageConfig got unexpected keyword arguments: "
                + ", ".join(sorted(kwargs)))

    @property
    def clickhouse_port(self) -> int:
        """Backwards-compatible alias for the ClickHouse HTTP port.

        Older consumers use ``cfg.clickhouse_port`` (HTTP 8123); the native
        9000 port is exposed separately as ``clickhouse_native_port``.
        """
        return self.clickhouse_http_port

    @clickhouse_port.setter
    def clickhouse_port(self, value: int) -> None:
        self.clickhouse_http_port = int(value)

    @classmethod
    def from_env(cls) -> "StorageConfig":
        """Build a StorageConfig from environment variables.

        OMNIWATCH_* names take precedence; old bare names are honored as a
        deprecated fallback; unset variables fall back to the docker-compose
        defaults. Numeric (port) and boolean (MINIO_SECURE) fields are parsed
        to their typed value; malformed values raise ValueError rather than
        silently misbehaving at connect time.
        """
        cfg = cls()
        cfg.clickhouse_host = _env(
            "OMNIWATCH_CLICKHOUSE_HOST", "CLICKHOUSE_HOST", cfg.clickhouse_host)
        cfg.clickhouse_http_port = _env_int(
            "OMNIWATCH_CLICKHOUSE_HTTP_PORT", "CLICKHOUSE_PORT",
            cfg.clickhouse_http_port)
        cfg.clickhouse_native_port = _env_int(
            "OMNIWATCH_CLICKHOUSE_NATIVE_PORT", "CLICKHOUSE_NATIVE_PORT",
            cfg.clickhouse_native_port)
        cfg.clickhouse_db = _env(
            "OMNIWATCH_CLICKHOUSE_DB", "CLICKHOUSE_DB", cfg.clickhouse_db)
        cfg.clickhouse_user = _env(
            "OMNIWATCH_CLICKHOUSE_USER", "CLICKHOUSE_USER", cfg.clickhouse_user)
        cfg.clickhouse_password = _env(
            "OMNIWATCH_CLICKHOUSE_PASSWORD", "CLICKHOUSE_PASSWORD",
            cfg.clickhouse_password)
        cfg.clickhouse_timezone = _env(
            "OMNIWATCH_CLICKHOUSE_TIMEZONE", "CLICKHOUSE_TIMEZONE",
            cfg.clickhouse_timezone)

        cfg.neo4j_uri = _env("OMNIWATCH_NEO4J_URI", "NEO4J_URI", cfg.neo4j_uri)
        cfg.neo4j_user = _env("OMNIWATCH_NEO4J_USER", "NEO4J_USER", cfg.neo4j_user)
        cfg.neo4j_password = _env(
            "OMNIWATCH_NEO4J_PASSWORD", "NEO4J_PASSWORD", cfg.neo4j_password)

        cfg.minio_endpoint = _env(
            "OMNIWATCH_MINIO_ENDPOINT", "MINIO_ENDPOINT", cfg.minio_endpoint)
        cfg.minio_access_key = _env(
            "OMNIWATCH_MINIO_ACCESS_KEY", "MINIO_ACCESS_KEY", cfg.minio_access_key)
        cfg.minio_secret_key = _env(
            "OMNIWATCH_MINIO_SECRET_KEY", "MINIO_SECRET_KEY", cfg.minio_secret_key)
        cfg.minio_secure = _env_bool(
            "OMNIWATCH_MINIO_SECURE", "MINIO_SECURE", cfg.minio_secure)
        cfg.minio_console_port = _env_int(
            "OMNIWATCH_MINIO_CONSOLE_PORT", "MINIO_CONSOLE_PORT",
            cfg.minio_console_port)

        cfg.kafka_bootstrap_servers = _env(
            "OMNIWATCH_KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BOOTSTRAP_SERVERS",
            cfg.kafka_bootstrap_servers)
        cfg.opa_url = _env("OMNIWATCH_OPA_URL", "OPA_URL", cfg.opa_url)
        cfg.llm_url = _env("OMNIWATCH_LLM_URL", "OLLAMA_URL", cfg.llm_url)
        cfg.llm_model = _env("OMNIWATCH_LLM_MODEL", "LLM_MODEL", cfg.llm_model)
        cfg.vllm_base = _env("OMNIWATCH_VLLM_BASE", "VLLM_BASE", cfg.vllm_base)
        cfg.learning_url = _env(
            "OMNIWATCH_LEARNING_URL", "LEARNING_URL", cfg.learning_url)
        cfg.genai_url = _env("OMNIWATCH_GENAI_URL", "GENAI_URL", cfg.genai_url)
        cfg.orchestration_url = _env(
            "OMNIWATCH_ORCHESTRATION_URL", "ORCHESTRATION_URL", cfg.orchestration_url)
        cfg.simulation_endpoint = _env(
            "OMNIWATCH_SIMULATION_ENDPOINT", "SIMULATION_ENDPOINT",
            cfg.simulation_endpoint)
        cfg.default_cloud_provider = _env(
            "OMNIWATCH_DEFAULT_CLOUD_PROVIDER", "DEFAULT_CLOUD_PROVIDER",
            cfg.default_cloud_provider)
        cfg.default_region = _env(
            "OMNIWATCH_DEFAULT_REGION", "DEFAULT_REGION", cfg.default_region)

        if cfg.minio_access_key == "minioadmin" or cfg.minio_secret_key == "minioadmin":
            _log.warning(
                "storage config using default MinIO dev credentials; set "
                "OMNIWATCH_MINIO_ACCESS_KEY / OMNIWATCH_MINIO_SECRET_KEY in prod")
        if cfg.neo4j_password == "omniwatch":
            _log.warning(
                "storage config using default Neo4j dev password; set "
                "OMNIWATCH_NEO4J_PASSWORD in prod")

        cfg._env_map = {
            "OMNIWATCH_CLICKHOUSE_HOST": cfg.clickhouse_host,
            "OMNIWATCH_CLICKHOUSE_HTTP_PORT": str(cfg.clickhouse_http_port),
            "OMNIWATCH_CLICKHOUSE_NATIVE_PORT": str(cfg.clickhouse_native_port),
            "OMNIWATCH_CLICKHOUSE_DB": cfg.clickhouse_db,
            "OMNIWATCH_CLICKHOUSE_USER": cfg.clickhouse_user,
            "OMNIWATCH_CLICKHOUSE_PASSWORD": cfg.clickhouse_password,
            "OMNIWATCH_CLICKHOUSE_TIMEZONE": cfg.clickhouse_timezone,
            "OMNIWATCH_NEO4J_URI": cfg.neo4j_uri,
            "OMNIWATCH_NEO4J_USER": cfg.neo4j_user,
            "OMNIWATCH_NEO4J_PASSWORD": cfg.neo4j_password,
            "OMNIWATCH_MINIO_ENDPOINT": cfg.minio_endpoint,
            "OMNIWATCH_MINIO_ACCESS_KEY": cfg.minio_access_key,
            "OMNIWATCH_MINIO_SECRET_KEY": cfg.minio_secret_key,
            "OMNIWATCH_MINIO_SECURE": str(cfg.minio_secure),
            "OMNIWATCH_MINIO_CONSOLE_PORT": str(cfg.minio_console_port),
            "OMNIWATCH_KAFKA_BOOTSTRAP_SERVERS": cfg.kafka_bootstrap_servers,
            "OMNIWATCH_OPA_URL": cfg.opa_url,
            "OMNIWATCH_LLM_URL": cfg.llm_url,
            "OMNIWATCH_LLM_MODEL": cfg.llm_model,
            "OMNIWATCH_DEFAULT_CLOUD_PROVIDER": cfg.default_cloud_provider,
            "OMNIWATCH_DEFAULT_REGION": cfg.default_region,
        }
        return cfg

    def require_production_secrets(self) -> None:
        """Fail fast when dev-default secrets are in use (prod gate).

        Raises:
            ValueError: With a clear message naming the exact env var to set
                when any secret still holds its local-dev default (or is
                empty where a value is required). Call this from production
                entrypoints; local-dev and tests must NOT call it so today's
                defaults keep booting unchanged.
        """
        missing: list[str] = []
        if self.minio_access_key in ("", "minioadmin"):
            missing.append("OMNIWATCH_MINIO_ACCESS_KEY")
        if self.minio_secret_key in ("", "minioadmin"):
            missing.append("OMNIWATCH_MINIO_SECRET_KEY")
        if self.neo4j_password in ("", "omniwatch"):
            missing.append("OMNIWATCH_NEO4J_PASSWORD")
        if missing:
            raise ValueError(
                "refusing to connect with dev-default secrets: "
                + ", ".join(missing)
                + " — set non-default values via environment")
        if self.clickhouse_password == "":
            _log.warning(
                "OMNIWATCH_CLICKHOUSE_PASSWORD is empty; connecting without a "
                "ClickHouse password (ok for local dev only)")

    def env(self) -> Dict[str, str]:
        """Return the effective env-var mapping for this config instance.

        Useful for health checks / diagnostics and for the E2E test that
        asserts the storage layer boots against the same values.
        """
        return self._env_map
