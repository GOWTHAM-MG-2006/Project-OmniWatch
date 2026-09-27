"""
OmniWatch — Entry-Point / Exporter Layer
Component: Exporter inventory + pre-filled bundle download (mounted under /exporters)
Phase: exporter-importer-split
Purpose: N exporters per workspace, each with a unique auto-assigned number +
         user-given name. Bundle download returns a zip with the importer's
         endpoint URL, workspace slug, and exporter identity pre-filled
         (the API token is pasted by the owner from the once-shown value).
Inputs: JWT via resolve_caller; workspace ownership via workspace registry
Outputs: Exporter records (metadata only, never tokens); zip bundle;
         403 (never 404-leak)
"""

from __future__ import annotations

import io
import logging
import os
import threading
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from identity.importers import get_importer_registry
from identity.workspaces import get_registry, resolve_caller

logger = logging.getLogger("omniwatch.identity.exporters")

router = APIRouter(prefix="/exporters", tags=["exporters"])


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ExporterRecord:
    workspace_id: str
    user_id: str
    number: int
    name: str
    created_at: datetime = field(default_factory=utcnow)


class ExporterRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_workspace: dict[str, list[ExporterRecord]] = {}

    def reset(self) -> None:
        with self._lock:
            self._by_workspace.clear()

    def restore(self, record: ExporterRecord) -> None:
        """Load one deduplicated row (boot reload from ClickHouse mirror)."""
        with self._lock:
            self._by_workspace.setdefault(record.workspace_id, []).append(record)

    def register(self, user_id: str, workspace_id: str, name: str) -> ExporterRecord:
        clean = (name or "").strip()
        if not clean or len(clean) > 64:
            raise ValueError("exporter name must be 1-64 chars")
        with self._lock:
            existing = self._by_workspace.setdefault(workspace_id, [])
            numbers = {r.number for r in existing}
            number = 1
            while number in numbers:
                number += 1
            record = ExporterRecord(
                workspace_id=workspace_id, user_id=user_id,
                number=number, name=clean,
            )
            existing.append(record)
            try:
                from identity import persistence as persistence_module

                persistence_module.mirror_exporter(record)
            except Exception:  # noqa: BLE001 - mirror must never fail register
                logger.warning("exporter mirror degraded", exc_info=True)
            return record

    def list_owned(
        self, user_id: str, workspace_id: str
    ) -> Optional[list[ExporterRecord]]:
        with self._lock:
            records = self._by_workspace.get(workspace_id)
            if records is None:
                return []
            if any(r.user_id != user_id for r in records):
                return None
            if records and records[0].user_id != user_id:
                return None
            return list(records)

    def get_owned(
        self, user_id: str, workspace_id: str, number: int
    ) -> Optional[ExporterRecord]:
        listed = self.list_owned(user_id, workspace_id)
        if listed is None:
            return None
        for record in listed:
            if record.number == number:
                return record
        return None

    def delete(
        self, user_id: str, workspace_id: str, number: int
    ) -> Optional[ExporterRecord]:
        """Remove one exporter (owner only). Returns the removed record."""
        with self._lock:
            records = self._by_workspace.get(workspace_id)
            if records is None:
                return None
            if any(r.user_id != user_id for r in records):
                return None
            for i, record in enumerate(records):
                if record.number == number:
                    del records[i]
                    try:
                        from identity import persistence as persistence_module

                        persistence_module.mirror_exporter(record, deleted=True)
                    except Exception:  # noqa: BLE001 - mirror never fails delete
                        logger.warning("exporter mirror degraded", exc_info=True)
                    return record
            return None


_REGISTRY = ExporterRegistry()


def get_exporter_registry() -> ExporterRegistry:
    return _REGISTRY


def reset_exporter_registry() -> None:
    _REGISTRY.reset()


class ExporterCreate(BaseModel):
    workspace_id: str
    name: str


class ExporterInfo(BaseModel):
    workspace_id: str
    number: int
    name: str
    endpoint_url: str
    created_at: str


def _forbidden() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="exporter not found or access denied",
    )


def _endpoint_for(user_id: str, workspace_id: str, slug: str) -> str:
    record = get_importer_registry().get_owned(user_id, workspace_id)
    if record is not None:
        return record.endpoint_url
    owned = get_registry().get_owned(workspace_id, user_id)
    if owned is None:
        raise _forbidden()
    try:
        record, _ = get_importer_registry().ensure_for_workspace(
            user_id, workspace_id, slug)
        return record.endpoint_url
    except Exception:  # noqa: BLE001 - backfill race -> re-read
        from identity.importers import DuplicateImporterError

        record = get_importer_registry().get_owned(user_id, workspace_id)
        if record is None:
            raise _forbidden()
        return record.endpoint_url


def _to_info(record: ExporterRecord, endpoint_url: str) -> ExporterInfo:
    return ExporterInfo(
        workspace_id=record.workspace_id,
        number=record.number,
        name=record.name,
        endpoint_url=endpoint_url,
        created_at=record.created_at.isoformat(),
    )


@router.post("", response_model=ExporterInfo, status_code=status.HTTP_201_CREATED)
def register_exporter(body: ExporterCreate, ctx: Any = Depends(resolve_caller)) -> ExporterInfo:
    owned = get_registry().get_owned(body.workspace_id, ctx.user_id)
    if owned is None:
        raise _forbidden()
    try:
        record = get_exporter_registry().register(
            ctx.user_id, body.workspace_id, body.name)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)) from exc
    endpoint = _endpoint_for(ctx.user_id, body.workspace_id, owned.slug)
    logger.info("exporter registered ws=%s number=%d name=%s",
                owned.slug, record.number, record.name)
    return _to_info(record, endpoint)


@router.get("/by-workspace/{workspace_id}", response_model=list[ExporterInfo])
def list_exporters(workspace_id: str, ctx: Any = Depends(resolve_caller)) -> list[ExporterInfo]:
    owned = get_registry().get_owned(workspace_id, ctx.user_id)
    if owned is None:
        raise _forbidden()
    records = get_exporter_registry().list_owned(ctx.user_id, workspace_id)
    if records is None:
        raise _forbidden()
    endpoint = _endpoint_for(ctx.user_id, workspace_id, owned.slug)
    return [_to_info(r, endpoint) for r in records]


class ExporterDeleted(BaseModel):
    workspace_id: str
    number: int
    deleted: bool = True


@router.delete("/{workspace_id}/{number}", response_model=ExporterDeleted)
def delete_exporter(
    workspace_id: str, number: int, ctx: Any = Depends(resolve_caller)
) -> ExporterDeleted:
    owned = get_registry().get_owned(workspace_id, ctx.user_id)
    if owned is None:
        raise _forbidden()
    removed = get_exporter_registry().delete(ctx.user_id, workspace_id, number)
    if removed is None:
        raise _forbidden()
    logger.info("exporter deleted ws=%s number=%d name=%s",
                owned.slug, removed.number, removed.name)
    return ExporterDeleted(workspace_id=workspace_id, number=number)


def _agent_bin_path() -> str:
    return os.getenv(
        "OMNIWATCH_AGENT_BIN", "/opt/omniwatch-agent/omniwatch-agent")


def _agent_binary() -> Optional[bytes]:
    """Linux exporter binary for the bundle; None when not packaged.

    The stock identity image builds it in (Dockerfile stage 0). Source
    checkouts and unit tests have no binary — the bundle then ships
    config-only and the README says how to build it.
    """
    path = _agent_bin_path()
    try:
        with open(path, "rb") as fh:
            return fh.read()
    except OSError:
        return None


def _bundle_zip(slug: str, endpoint_url: str, number: int, name: str) -> bytes:
    config_yaml = (
        f"# OmniWatch exporter bundle — workspace '{slug}', exporter #{number} ({name})\n"
        f"# QUICK START: run ./install.sh and paste the importer API token when asked.\n"
        f"# (Manual alternative: replace PASTE_IMPORTER_TOKEN_HERE below, then run\n"
        f"#  ./omniwatch-agent --config exporter-config.yaml -health-addr :8082)\n"
        "importer:\n"
        f"  endpoint: \"{endpoint_url}\"\n"
        "  api_token: \"PASTE_IMPORTER_TOKEN_HERE\"\n"
        "  # Optional: real app telemetry from local docker (Phase 1).\n"
        "  # Uncomment + list container names to tail logs / poll stats.\n"
        "  # Absent or empty = heartbeat-only mode (backward compatible).\n"
        "  #docker:\n"
        "  #  socket_path: \"/var/run/docker.sock\"\n"
        "  #  containers: [\"aws-product-catalog\", \"aws-frontend\"]\n"
        "  #  log_tail: 100\n"
        "exporter:\n"
        f"  exporter_number: {number}\n"
        f"  exporter_name: \"{name}\"\n"
        f"  entity_id: \"exporter-{number}-{slug}\"\n"
        "telemetry_types:\n"
        "  - metrics\n"
        "  - logs\n"
        "  - traces\n"
        "  - metadata\n"
        "  - state\n"
        "  - audit_logs\n"
        "  - profiling\n"
        "  - siem\n"
        "  - auth_logs\n"
        "  - security_alerts\n"
    )
    install_sh = (
        "#!/bin/sh\n"
        "# OmniWatch exporter install — fully automatic.\n"
        "# Usage: unzip the bundle, then run:\n"
        "#    ./install.sh   (or: sh install.sh)\n"
        "# It asks for ONE thing — the importer API token shown once in\n"
        "# the dashboard (Omni-Agent tab) — then patches\n"
        "# exporter-config.yaml, starts the agent in the background with\n"
        "# nohup, and verifies it is exporting telemetry.\n"
        "# Env shortcut (skips the prompt):\n"
        "#    OMNIWATCH_IMPORTER_API_TOKEN=<token> ./install.sh\n"
        "set -eu\n"
        "cd \"$(dirname \"$0\")\"\n"
        "BIN=\"./omniwatch-agent\"\n"
        "CFG=\"./exporter-config.yaml\"\n"
        "LOG=\"./agent.log\"\n"
        "PIDFILE=\"./agent.pid\"\n"
        "if [ ! -f \"$BIN\" ]; then\n"
        "  echo \"ERROR: omniwatch-agent binary not found next to install.sh.\"\n"
        "  echo \"Build it, then re-run:\"\n"
        "  echo \"  cd omniwatch-agent\"\n"
        "  echo \"  CGO_ENABLED=0 GOOS=linux go build -o omniwatch-agent ./cmd/agent\"\n"
        "  exit 1\n"
        "fi\n"
        "if [ ! -f \"$CFG\" ]; then\n"
        "  echo \"ERROR: $CFG not found next to install.sh.\"\n"
        "  exit 1\n"
        "fi\n"
        "# Preflight: docker app sources (warn-only — heartbeat mode works regardless).\n"
        "if [ -r /var/run/docker.sock ] || (command -v docker >/dev/null 2>&1 && docker ps >/dev/null 2>&1); then\n"
        "  echo \"docker: reachable (container logs + stats available if configured).\"\n"
        "  if command -v docker >/dev/null 2>&1; then\n"
        "    DRV=\"$(docker info --format '{{.LoggingDriver}}' 2>/dev/null || true)\"\n"
        "    case \"$DRV\" in\n"
        "      \"\"|json-file|local) ;;\n"
        "      *)\n"
        "        echo \"WARNING: docker logging driver is '$DRV' (not json-file/local).\"\n"
        "        echo \"  Container-log tailing needs json-file or local; container metrics still work.\"\n"
        "        ;;\n"
        "    esac\n"
        "  fi\n"
        "else\n"
        "  echo \"WARNING: docker not reachable (socket /var/run/docker.sock unreadable).\"\n"
        "  echo \"  Agent runs in heartbeat-only mode. To enable app logs/metrics:\"\n"
        "  echo \"  add this user to the docker group, re-login, re-run ./install.sh.\"\n"
        "fi\n"
        "chmod +x \"$BIN\"\n"
        "TOKEN=\"${OMNIWATCH_IMPORTER_API_TOKEN:-}\"\n"
        "if [ -z \"$TOKEN\" ]; then\n"
        "  printf 'Paste importer API token (shown once in dashboard Omni-Agent tab): '\n"
        "  if [ -t 0 ]; then\n"
        "    stty -echo 2>/dev/null || true\n"
        "    IFS= read -r TOKEN || true\n"
        "    stty echo 2>/dev/null || true\n"
        "    printf '\\n'\n"
        "  else\n"
        "    IFS= read -r TOKEN || true\n"
        "  fi\n"
        "fi\n"
        "if [ -z \"$TOKEN\" ]; then\n"
        "  echo \"ERROR: empty token — copy it from the dashboard and re-run ./install.sh.\"\n"
        "  exit 1\n"
        "fi\n"
        "# Patch api_token into exporter-config.yaml (python3 avoids sed-escaping bugs).\n"
        "if command -v python3 >/dev/null 2>&1; then\n"
        "  OMNIWATCH_TOKEN=\"$TOKEN\" python3 - \"$CFG\" <<'PYEOF'\n"
        "import os, sys\n"
        "path, token = sys.argv[1], os.environ[\"OMNIWATCH_TOKEN\"]\n"
        "text = open(path).read()\n"
        "needle = \"PASTE_IMPORTER_TOKEN_HERE\"\n"
        "if needle in text:\n"
        "    text = text.replace(needle, token)\n"
        "elif \"api_token:\" in text:\n"
        "    import re\n"
        "    text = re.sub(r'(api_token:\\s*\")[^\"]*(\")', r\"\\g<1>\" + token + r\"\\g<2>\", text)\n"
        "else:\n"
        "    sys.exit(\"api_token line not found in \" + path)\n"
        "open(path, \"w\").write(text)\n"
        "print(\"patched api_token into \" + path)\n"
        "PYEOF\n"
        "else\n"
        "  echo \"ERROR: python3 is required to patch the token. Install it and re-run.\"\n"
        "  exit 1\n"
        "fi\n"
        "# Pick a free health port (AWS-slice boxes already own :8080, so start at :8082).\n"
        "HEALTH_ADDR=\"${OMNIWATCH_HEALTH_ADDR:-:8082}\"\n"
        "port_free() {\n"
        "  p=\"$1\"\n"
        "  if command -v ss >/dev/null 2>&1; then\n"
        "    ! ss -ltn 2>/dev/null | grep -q \":$p \"\n"
        "  elif command -v netstat >/dev/null 2>&1; then\n"
        "    ! netstat -ltn 2>/dev/null | grep -q \":$p \"\n"
        "  else\n"
        "    return 0\n"
        "  fi\n"
        "}\n"
        "PORT=\"$(printf '%s' \"$HEALTH_ADDR\" | sed 's/^.*://')\"\n"
        "if ! port_free \"$PORT\"; then\n"
        "  for PORT in 8082 8083 8084 8085 8090; do\n"
        "    if port_free \"$PORT\"; then break; fi\n"
        "  done\n"
        "  HEALTH_ADDR=\":$PORT\"\n"
        "fi\n"
        "echo \"health endpoint: $HEALTH_ADDR (free)\"\n"
        "# Stop any previous agent started by this script.\n"
        "if [ -f \"$PIDFILE\" ]; then\n"
        "  OLD=\"$(cat \"$PIDFILE\" 2>/dev/null || true)\"\n"
        "  if [ -n \"${OLD:-}\" ] && kill -0 \"$OLD\" 2>/dev/null; then\n"
        "    echo \"stopping previous agent (pid $OLD) ...\"\n"
        "    kill \"$OLD\" 2>/dev/null || true\n"
        "    sleep 2\n"
        "  fi\n"
        "  rm -f \"$PIDFILE\"\n"
        "fi\n"
        "echo \"starting agent in background ...\"\n"
        "nohup \"$BIN\" --config \"$CFG\" -health-addr \"$HEALTH_ADDR\" > \"$LOG\" 2>&1 &\n"
        "echo \"$!\" > \"$PIDFILE\"\n"
        "sleep 5\n"
        "NEWPID=\"$(cat \"$PIDFILE\")\"\n"
        "if ! kill -0 \"$NEWPID\" 2>/dev/null; then\n"
        "  echo \"ERROR: agent exited right after start. Last 30 log lines:\"\n"
        "  tail -n 30 \"$LOG\" || true\n"
        "  exit 1\n"
        "fi\n"
        "echo \"agent alive (pid $NEWPID), checking telemetry + health ...\"\n"
        "sleep 5\n"
        "HPORT=\"$(printf '%s' \"$HEALTH_ADDR\" | sed 's/^.*://')\"\n"
        "if command -v curl >/dev/null 2>&1; then\n"
        "  curl -sf \"http://localhost:$HPORT/health\" >/dev/null 2>&1 && echo \"health OK on :$HPORT\" || echo \"health not yet up on :$HPORT (check $LOG)\"\n"
        "elif command -v wget >/dev/null 2>&1; then\n"
        "  wget -qO- \"http://localhost:$HPORT/health\" >/dev/null 2>&1 && echo \"health OK on :$HPORT\" || echo \"health not yet up on :$HPORT (check $LOG)\"\n"
        "fi\n"
        "if grep -q -E \"direct export succeeded|heartbeat emitted\" \"$LOG\" 2>/dev/null; then\n"
        "  echo \"telemetry FLOWING (direct export / heartbeat seen in $LOG).\"\n"
        "else\n"
        "  echo \"agent running, but no heartbeat logged yet — tail $LOG for 30s;\"\n"
        "  echo \"if DNS errors mention trycloudflare, the tunnel URL rotated:\"\n"
        "  echo \"run ./update-endpoint.sh and paste the fresh URL from the dashboard.\"\n"
        "fi\n"
        "echo \"--- last 15 log lines ---\"\n"
        "tail -n 15 \"$LOG\" || true\n"
        "echo \"--- done: agent in background (pid $NEWPID, logs $LOG) ---\"\n"
        "echo \"stop:  kill $(cat \"$PIDFILE\") && rm -f \\\"$PIDFILE\\\"\"\n"
        "echo \"logs:  tail -f \\\"$LOG\\\"\"\n"
    )
    update_endpoint_sh = (
        "#!/bin/sh\n"
        "# OmniWatch exporter — point at a new importer (tunnel) URL.\n"
        "# Usage: ./update-endpoint.sh   (or: sh update-endpoint.sh)\n"
        "# Asks for ONE thing — the new importer endpoint URL shown in the\n"
        "# dashboard (Omni-Agent tab) after the tunnel rotated — then patches\n"
        "# the endpoint: line in exporter-config.yaml and re-runs\n"
        "# ./install.sh, which restarts the agent in the background\n"
        "# (config is read at startup, so a restart is required).\n"
        "# The saved token is read back out of exporter-config.yaml and fed\n"
        "# to install.sh, so you are NOT asked for the token again.\n"
        "set -eu\n"
        "cd \"$(dirname \"$0\")\"\n"
        "CFG=\"./exporter-config.yaml\"\n"
        "if [ ! -f \"$CFG\" ]; then\n"
        "  echo \"ERROR: $CFG not found next to update-endpoint.sh.\"\n"
        "  exit 1\n"
        "fi\n"
        "printf 'Paste new importer endpoint URL (dashboard Omni-Agent tab): '\n"
        "IFS= read -r NEWURL || true\n"
        "if [ -z \"$NEWURL\" ]; then\n"
        "  echo \"ERROR: empty URL — copy it from the dashboard and re-run ./update-endpoint.sh.\"\n"
        "  exit 1\n"
        "fi\n"
        "case \"$NEWURL\" in\n"
        "  http://*|https://*) ;;\n"
        "  *) echo \"ERROR: URL must start with http:// or https:// — got: $NEWURL\"; exit 1 ;;\n"
        "esac\n"
        "# Patch the endpoint: line (python3 avoids sed-escaping bugs).\n"
        "if command -v python3 >/dev/null 2>&1; then\n"
        "  OMNIWATCH_URL=\"$NEWURL\" python3 - \"$CFG\" <<'PYEOF'\n"
        "import os, re, sys\n"
        "path, url = sys.argv[1], os.environ[\"OMNIWATCH_URL\"]\n"
        "text = open(path).read()\n"
        "new, n = re.subn(r'(endpoint:\\s*\")[^\"]*(\")', r\"\\g<1>\" + url + r\"\\g<2>\", text, count=1)\n"
        "if n == 0:\n"
        "    sys.exit(\"endpoint line not found in \" + path)\n"
        "open(path, \"w\").write(new)\n"
        "print(\"patched endpoint into \" + path)\n"
        "PYEOF\n"
        "else\n"
        "  echo \"ERROR: python3 is required. Install it and re-run.\"\n"
        "  exit 1\n"
        "fi\n"
        "# Read the saved token back out and feed it to install.sh (no second prompt).\n"
        "SAVED=\"\"\n"
        "if command -v python3 >/dev/null 2>&1; then\n"
        "  SAVED=\"$(python3 - \"$CFG\" <<'PYEOF'\n"
        "import re, sys\n"
        "text = open(sys.argv[1]).read()\n"
        "m = re.search(r'api_token:\\s*\"([^\"]+)\"', text)\n"
        "print(m.group(1) if m else \"\")\n"
        "PYEOF\n"
        ")\"\n"
        "fi\n"
        "case \"$SAVED\" in\n"
        "  \"\"|PASTE_IMPORTER_TOKEN_HERE)\n"
        "    echo \"no token saved in $CFG yet — install.sh will ask for it once.\"\n"
        "    exec ./install.sh\n"
        "    ;;\n"
        "  *)\n"
        "    echo \"reusing saved token (not shown) — restarting agent ...\"\n"
        "    OMNIWATCH_IMPORTER_API_TOKEN=\"$SAVED\" exec ./install.sh\n"
        "    ;;\n"
        "esac\n"
    )
    delete_agent_sh = (
        "#!/bin/sh\n"
        "# OmniWatch exporter uninstaller — removes the agent and everything\n"
        "# attached to it, like an application uninstaller.\n"
        "# Usage: ./delete_agent.sh   (or: sh delete_agent.sh [--yes])\n"
        "# What it does:\n"
        "#   1. stops the background agent (agent.pid, pkill fallback)\n"
        "#   2. deletes agent files: omniwatch-agent, exporter-config.yaml,\n"
        "#      agent.log, agent.pid\n"
        "#   3. optionally deletes this whole folder too (the .sh scripts\n"
        "#      and README) — say 'y' only if you will re-download the\n"
        "#      bundle for any future reinstall.\n"
        "# NOTE: the Exporter-N registration in the dashboard (Omni-Agent\n"
        "# tab) is NOT touched — click Delete there too if you will not\n"
        "# reinstall, otherwise just re-download the bundle and the same\n"
        "# exporter number keeps working.\n"
        "set -eu\n"
        "cd \"$(dirname \"$0\")\"\n"
        "BIN=\"./omniwatch-agent\"\n"
        "CFG=\"./exporter-config.yaml\"\n"
        "LOG=\"./agent.log\"\n"
        "PIDFILE=\"./agent.pid\"\n"
        "YES=\"${OMNIWATCH_DELETE_YES:-}\"\n"
        "for a in \"$@\"; do\n"
        "  case \"$a\" in --yes|-y) YES=1 ;; esac\n"
        "done\n"
        "if [ -z \"$YES\" ]; then\n"
        "  printf 'Delete the exporter agent and all its files? [y/N] '\n"
        "  IFS= read -r ANS || true\n"
        "  case \"$ANS\" in y|Y|yes|YES) ;; *) echo \"aborted.\"; exit 0 ;; esac\n"
        "fi\n"
        "echo \"stopping agent ...\"\n"
        "if [ -f \"$PIDFILE\" ]; then\n"
        "  OLD=\"$(cat \"$PIDFILE\" 2>/dev/null || true)\"\n"
        "  if [ -n \"${OLD:-}\" ] && kill -0 \"$OLD\" 2>/dev/null; then\n"
        "    kill \"$OLD\" 2>/dev/null || true\n"
        "    for _ in 1 2 3 4 5; do\n"
        "      kill -0 \"$OLD\" 2>/dev/null || break\n"
        "      sleep 1\n"
        "    done\n"
        "    if kill -0 \"$OLD\" 2>/dev/null; then\n"
        "      echo \"agent did not stop, forcing ...\"\n"
        "      kill -9 \"$OLD\" 2>/dev/null || true\n"
        "      sleep 1\n"
        "    fi\n"
        "    echo \"agent stopped (was pid $OLD).\"\n"
        "  else\n"
        "    echo \"no live agent for pid file (stale $PIDFILE removed).\"\n"
        "  fi\n"
        "  rm -f \"$PIDFILE\"\n"
        "fi\n"
        "if command -v pkill >/dev/null 2>&1; then\n"
        "  if pkill -f \"omniwatch-agent\" 2>/dev/null; then\n"
        "    echo \"stopped leftover omniwatch-agent process(es).\"\n"
        "    sleep 2\n"
        "  fi\n"
        "fi\n"
        "echo \"removing agent files ...\"\n"
        "rm -f \"$BIN\" \"$CFG\" \"$LOG\" \"$PIDFILE\"\n"
        "for f in \"$BIN\" \"$CFG\" \"$LOG\" \"$PIDFILE\"; do\n"
        "  if [ -e \"$f\" ]; then echo \"WARNING: could not remove $f\"; fi\n"
        "done\n"
        "echo \"agent files removed.\"\n"
        "HERE=\"$(pwd)\"\n"
        "if [ -z \"$YES\" ]; then\n"
        "  printf 'Also delete this whole folder (%s)? [y/N] ' \"$HERE\"\n"
        "  IFS= read -r ANS2 || true\n"
        "  case \"$ANS2\" in y|Y|yes|YES) RMDIR=1 ;; *) RMDIR= ;; esac\n"
        "else\n"
        "  RMDIR=1\n"
        "fi\n"
        "if [ -n \"${RMDIR:-}\" ]; then\n"
        "  echo \"removing folder $HERE ...\"\n"
        "  cd /tmp\n"
        "  rm -rf \"$HERE\"\n"
        "  echo \"folder removed. To reinstall, re-download the bundle from\"\n"
        "  echo \"the dashboard (Omni-Agent tab) and run ./install.sh.\"\n"
        "else\n"
        "  echo \"folder kept (install.sh, update-endpoint.sh, delete_agent.sh,\"\n"
        "  echo \"README.txt remain). To reinstall, re-download the bundle\"\n"
        "  echo \"for a fresh exporter-config.yaml, then run ./install.sh.\"\n"
        "fi\n"
        "echo \"--- done: exporter agent uninstalled ---\"\n"
    )
    binary = _agent_binary()
    readme_txt = (
        f"OmniWatch exporter bundle\n"
        f"Workspace: {slug}\n"
        f"Exporter: #{number} ({name})\n"
        f"Importer endpoint: {endpoint_url}\n\n"
        + (
            "This bundle INCLUDES the Linux exporter binary "
            "(omniwatch-agent).\n"
            if binary is not None
            else "This bundle does NOT include the exporter binary — build it:\n"
            "  cd omniwatch-agent\n"
            "  CGO_ENABLED=0 GOOS=linux go build -o omniwatch-agent ./cmd/agent\n"
            "and place it next to install.sh.\n"
        )
        + "\nQUICK START (automatic):\n"
        "  1. unzip the bundle, cd into the folder\n"
        "  2. run: ./install.sh   (or: sh install.sh)\n"
        "  3. paste the importer API token when asked\n"
        "  install.sh patches exporter-config.yaml, starts the agent in\n"
        "  the background (nohup, logs to agent.log), picks a free health\n"
        "  port (8082+), and verifies telemetry is flowing.\n"
        "\nTUNNEL ROTATED (endpoint changed)? Do NOT re-download:\n"
        "  run: ./update-endpoint.sh\n"
        "  paste ONLY the new importer URL — the saved token is reused\n"
        "  and the agent is restarted automatically.\n"
        "\nUNINSTALL (remove the agent)? Run the uninstaller:\n"
        "  run: ./delete_agent.sh   (or: ./delete_agent.sh --yes)\n"
        "  stops the background agent and deletes omniwatch-agent,\n"
        "  exporter-config.yaml, agent.log and agent.pid; optionally\n"
        "  deletes the whole folder too. Reinstall = re-download the\n"
        "  bundle and run ./install.sh. Also click Delete on the\n"
        "  exporter in the dashboard Omni-Agent tab if not reinstalling.\n"
        "\nNOT CONNECTED (by design — this agent does not collect these):\n"
        "  - CloudWatch / AWS APIs (needs IAM credentials + a dependency\n"
        "    this box does not have)\n"
        "  - Kubernetes (this box is not a K8s node; nothing to watch)\n"
        "  - SIEM beyond this host (only this host's auth log is tailed)\n"
        "  - Application profiling beyond the agent's own pprof self-\n"
        "    snapshot (no app profiler is attached)\n"
        "  - Distributed traces (apps here log no trace IDs; trace_id\n"
        "    extraction from app logs is a future step)\n"
        "What IS collected: host metrics, docker container logs + stats\n"
        "(when the optional docker: section is enabled in\n"
        "exporter-config.yaml), this host's auth log, agent self-profile,\n"
        "and the heartbeat.\n"
        "\nThe importer API token is NOT in this bundle (it is shown only\n"
        "once at workspace creation or importer rotation). Have it ready\n"
        "from the dashboard Omni-Agent tab before running install.sh.\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("exporter-config.yaml", config_yaml)
        info_sh = zipfile.ZipInfo("install.sh")
        info_sh.external_attr = 0o755 << 16
        info_sh.compress_type = zipfile.ZIP_DEFLATED
        zf.writestr(info_sh, install_sh)
        info_up = zipfile.ZipInfo("update-endpoint.sh")
        info_up.external_attr = 0o755 << 16
        info_up.compress_type = zipfile.ZIP_DEFLATED
        zf.writestr(info_up, update_endpoint_sh)
        info_del = zipfile.ZipInfo("delete_agent.sh")
        info_del.external_attr = 0o755 << 16
        info_del.compress_type = zipfile.ZIP_DEFLATED
        zf.writestr(info_del, delete_agent_sh)
        zf.writestr("README.txt", readme_txt)
        if binary is not None:
            info = zipfile.ZipInfo("omniwatch-agent")
            info.external_attr = 0o755 << 16
            # STORED, not deflated: a stripped Go binary barely compresses
            # and deflating ~50MB stalls the download for a minute+.
            info.compress_type = zipfile.ZIP_STORED
            zf.writestr(info, binary)
    return buf.getvalue()


@router.get("/bundle/{workspace_id}")
def download_bundle(
    workspace_id: str,
    number: int = Query(default=1, ge=1),
    ctx: Any = Depends(resolve_caller),
) -> StreamingResponse:
    owned = get_registry().get_owned(workspace_id, ctx.user_id)
    if owned is None:
        raise _forbidden()
    record = get_exporter_registry().get_owned(ctx.user_id, workspace_id, number)
    if record is None:
        # Auto-register unnamed exporter so a fresh workspace can download
        # immediately (number requested must be 1 when none exist).
        existing = get_exporter_registry().list_owned(ctx.user_id, workspace_id)
        if existing is None:
            raise _forbidden()
        if existing or number != 1:
            raise _forbidden()
        record = get_exporter_registry().register(
            ctx.user_id, workspace_id, f"exporter-{number}")
    endpoint = _endpoint_for(ctx.user_id, workspace_id, owned.slug)
    payload = _bundle_zip(owned.slug, endpoint, record.number, record.name)
    return StreamingResponse(
        io.BytesIO(payload),
        media_type="application/zip",
        headers={"Content-Disposition": (
            f"attachment; filename=omniwatch-exporter-{owned.slug}"
            f"-{record.number}.zip")},
    )
