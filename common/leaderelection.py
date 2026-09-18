"""
OmniWatch — Shared Leader Election
Component: Leader Election (K8s Lease singleton for daemon replicas)
Phase: Industry-hardening (IND-7)
Purpose: Guarantee that exactly one replica of a singleton daemon
         (causal / predictive / learning) processes at a time, using a
         Kubernetes ``coordination.k8s.io/v1`` Lease. Followers idle and log;
         on leader loss a follower takes over within ``lease_duration``.
Inputs: OMNIWATCH_LEADER_ELECTION_* env vars; K8s Lease object (when enabled
        and running in-cluster); nothing when disabled (standalone fallback).
Outputs: is_leader() gate + run_leader_gated() supervisor for processing loops.
         /health endpoints must stay UNGATED — gate only the processing loop.
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Protocol

_LOG = logging.getLogger("omniwatch.common.leaderelection")

#: Default lease hold time. Followers may take over this long after the
#: leader stops renewing (Task 9 E2E failover scenario keys off this).
DEFAULT_LEASE_DURATION_SECONDS = 15.0

#: How often followers re-log the waiting message (avoids log spam).
_FOLLOWER_LOG_THROTTLE_SECONDS = 30.0


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class LeaderElectionConfig:
    """Leader election settings, loaded from the environment.

    Env vars (all optional)::

        OMNIWATCH_LEADER_ELECTION_ENABLED=false
        OMNIWATCH_LEADER_ELECTION_LEASE_NAME=<service>-leader
        OMNIWATCH_LEADER_ELECTION_LEASE_NAMESPACE=<pod ns or omniwatch>
        OMNIWATCH_LEADER_ELECTION_LEASE_DURATION=15  (seconds)
    """

    enabled: bool = False
    lease_name: str = "omniwatch-leader"
    lease_namespace: str = "omniwatch"
    lease_duration_seconds: float = DEFAULT_LEASE_DURATION_SECONDS
    renew_interval_seconds: float = field(
        default_factory=lambda: DEFAULT_LEASE_DURATION_SECONDS / 3.0
    )

    @classmethod
    def from_env(cls, default_lease_name: str = "omniwatch-leader") -> LeaderElectionConfig:
        """Build config from OMNIWATCH_LEADER_ELECTION_* env vars."""
        enabled = os.environ.get("OMNIWATCH_LEADER_ELECTION_ENABLED", "false").lower() in (
            "1",
            "true",
            "yes",
        )
        lease_name = os.environ.get("OMNIWATCH_LEADER_ELECTION_LEASE_NAME", default_lease_name)
        namespace = os.environ.get(
            "OMNIWATCH_LEADER_ELECTION_LEASE_NAMESPACE",
            os.environ.get("POD_NAMESPACE", "omniwatch"),
        )
        try:
            duration = float(
                os.environ.get(
                    "OMNIWATCH_LEADER_ELECTION_LEASE_DURATION",
                    str(DEFAULT_LEASE_DURATION_SECONDS),
                )
            )
        except ValueError:
            duration = DEFAULT_LEASE_DURATION_SECONDS
        if duration <= 0:
            duration = DEFAULT_LEASE_DURATION_SECONDS
        return cls(
            enabled=enabled,
            lease_name=lease_name,
            lease_namespace=namespace,
            lease_duration_seconds=duration,
            renew_interval_seconds=duration / 3.0,
        )


# --------------------------------------------------------------------------- #
# Lease backends
# --------------------------------------------------------------------------- #

@dataclass
class LeaseRecord:
    """One Lease object snapshot (holder + renew timestamp + fencing version)."""

    holder: str
    renew_time: float  # monotonic seconds (in-memory) or epoch seconds (k8s)
    duration_seconds: float
    resource_version: str = "0"

    def is_expired(self, now: float) -> bool:
        """True when the holder's lease has lapsed (failover may proceed)."""
        return (now - self.renew_time) >= self.duration_seconds


class LeaseBackend(Protocol):
    """Pluggable Lease store: K8s API in prod, in-memory in tests."""

    def read(self, name: str, namespace: str) -> LeaseRecord | None: ...
    def create(self, name: str, namespace: str, record: LeaseRecord) -> bool: ...
    def update(
        self, name: str, namespace: str, record: LeaseRecord, expected_version: str
    ) -> bool: ...
    def delete(self, name: str, namespace: str, holder: str) -> None: ...


class InMemoryLeaseBackend:
    """Thread-safe in-process Lease store (tests + local dev without K8s)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._leases: dict[tuple[str, str], LeaseRecord] = {}
        self._versions: dict[tuple[str, str], int] = {}

    def read(self, name: str, namespace: str) -> LeaseRecord | None:
        with self._lock:
            record = self._leases.get((namespace, name))
            if record is None:
                return None
            return LeaseRecord(
                holder=record.holder,
                renew_time=record.renew_time,
                duration_seconds=record.duration_seconds,
                resource_version=record.resource_version,
            )

    def create(self, name: str, namespace: str, record: LeaseRecord) -> bool:
        with self._lock:
            if (namespace, name) in self._leases:
                return False
            stored = LeaseRecord(
                holder=record.holder,
                renew_time=record.renew_time,
                duration_seconds=record.duration_seconds,
                resource_version="1",
            )
            self._leases[(namespace, name)] = stored
            self._versions[(namespace, name)] = 1
            return True

    def update(
        self, name: str, namespace: str, record: LeaseRecord, expected_version: str
    ) -> bool:
        with self._lock:
            current = self._leases.get((namespace, name))
            if current is None or current.resource_version != expected_version:
                return False  # lost the race — another replica won
            version = self._versions.get((namespace, name), 0) + 1
            self._versions[(namespace, name)] = version
            self._leases[(namespace, name)] = LeaseRecord(
                holder=record.holder,
                renew_time=record.renew_time,
                duration_seconds=record.duration_seconds,
                resource_version=str(version),
            )
            return True

    def delete(self, name: str, namespace: str, holder: str) -> None:
        with self._lock:
            current = self._leases.get((namespace, name))
            if current is not None and current.holder == holder:
                del self._leases[(namespace, name)]


class K8sLeaseBackend:
    """Kubernetes ``coordination.k8s.io/v1`` Lease store.

    Returns ``available=False`` when the ``kubernetes`` client is missing or
    no cluster config exists — callers then fall back to standalone mode
    (never crash the daemon for election infra problems).
    """

    def __init__(self) -> None:
        self._api: Any = None
        self._available = False
        try:
            from kubernetes import client, config  # type: ignore[import-not-found]

            try:
                config.load_incluster_config()
            except Exception:  # noqa: BLE001 - dev workstation fallback
                config.load_kube_config()
            self._api = client.CoordinationV1Api()
            self._client_module = client
            self._available = True
        except Exception as exc:  # noqa: BLE001 - standalone fallback
            _LOG.warning("k8s lease backend unavailable, standalone mode: %s", exc)

    @property
    def available(self) -> bool:
        """True when the K8s API client is configured and usable."""
        return self._available

    def read(self, name: str, namespace: str) -> LeaseRecord | None:
        try:
            lease = self._api.read_namespaced_lease(name, namespace)
        except Exception as exc:  # noqa: BLE001 - 404 or API error
            if getattr(exc, "status", None) not in (None, 404):
                _LOG.debug("lease read failed %s/%s: %s", namespace, name, exc)
            return None
        holder = lease.spec.holder_identity or ""
        if not holder:
            return None
        renew_time = _parse_microtime(getattr(lease.spec, "renew_time", None))
        return LeaseRecord(
            holder=holder,
            renew_time=renew_time,
            duration_seconds=float(lease.spec.lease_duration_seconds or 0),
            resource_version=lease.metadata.resource_version or "0",
        )

    def create(self, name: str, namespace: str, record: LeaseRecord) -> bool:
        client = self._client_module
        body = client.V1Lease(
            metadata=client.V1ObjectMeta(name=name, namespace=namespace),
            spec=client.V1LeaseSpec(
                holder_identity=record.holder,
                lease_duration_seconds=int(record.duration_seconds),
                renew_time=_format_microtime(record.renew_time),
                acquire_time=_format_microtime(record.renew_time),
            ),
        )
        try:
            self._api.create_namespaced_lease(namespace, body)
            return True
        except Exception as exc:  # noqa: BLE001 - 409 AlreadyExists or API error
            _LOG.debug("lease create failed %s/%s: %s", namespace, name, exc)
            return False

    def update(
        self, name: str, namespace: str, record: LeaseRecord, expected_version: str
    ) -> bool:
        client = self._client_module
        body = client.V1Lease(
            metadata=client.V1ObjectMeta(
                name=name, namespace=namespace, resource_version=expected_version
            ),
            spec=client.V1LeaseSpec(
                holder_identity=record.holder,
                lease_duration_seconds=int(record.duration_seconds),
                renew_time=_format_microtime(record.renew_time),
            ),
        )
        try:
            self._api.replace_namespaced_lease(name, namespace, body)
            return True
        except Exception as exc:  # noqa: BLE001 - 409 conflict = lost race
            _LOG.debug("lease update failed %s/%s: %s", namespace, name, exc)
            return False

    def delete(self, name: str, namespace: str, holder: str) -> None:
        try:
            current = self.read(name, namespace)
            if current is not None and current.holder == holder:
                self._api.delete_namespaced_lease(name, namespace)
        except Exception:  # noqa: BLE001 - best-effort release
            _LOG.debug("lease delete failed", exc_info=True)


def _parse_microtime(value: Any) -> float:
    """Parse a K8s MicroTime (or datetime) to epoch seconds; now() on failure."""
    if value is None:
        return time.time()
    if isinstance(value, datetime):
        return value.timestamp()
    try:
        text = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(text).timestamp()
    except (TypeError, ValueError):
        return time.time()


def _format_microtime(epoch_seconds: float) -> Any:
    """Format epoch seconds as a K8s MicroTime (datetime is accepted by client)."""
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)


# --------------------------------------------------------------------------- #
# LeaderAware interface
# --------------------------------------------------------------------------- #

class LeaderAware(Protocol):
    """Interface for daemon processors that run only on the elected leader.

    ``start()`` blocks running the processing loop; ``stop()`` unblocks it.
    CausalEngine consumers, DetectorEngine workers, FeedbackLoopProcessors
    and PatternMiners all satisfy this shape already.
    """

    def start(self, *args: Any, **kwargs: Any) -> None: ...
    def stop(self) -> None: ...


# --------------------------------------------------------------------------- #
# Elector
# --------------------------------------------------------------------------- #

def _default_identity() -> str:
    """Stable-ish replica id: POD_NAME in K8s, else hostname + short uuid."""
    pod = os.environ.get("POD_NAME") or os.environ.get("HOSTNAME")
    if pod:
        return pod
    return f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"


class LeaderElector:
    """Acquire/renew a Lease in the background; report leadership locally.

    Disabled configs (``enabled=false``) or unavailable K8s backends behave
    as standalone: ``is_leader()`` is always True and no lease traffic occurs
    (daemon runs exactly as before leader election existed).
    """

    def __init__(
        self,
        config: LeaderElectionConfig,
        identity: str | None = None,
        backend: LeaseBackend | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._config = config
        self._identity = identity or _default_identity()
        self._clock = clock or time.monotonic
        self._backend = backend
        self._stop = threading.Event()
        self._abandoned = False  # simulate crash: stop renewing, keep the lease
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._leader = False
        self._last_follower_log = 0.0

    # ------------------------------------------------------------------ #
    @property
    def identity(self) -> str:
        """This replica's holder identity string."""
        return self._identity

    @property
    def config(self) -> LeaderElectionConfig:
        """The election config this elector was built with."""
        return self._config

    def is_leader(self) -> bool:
        """True when this replica currently holds the lease (or standalone)."""
        with self._lock:
            return self._leader

    # ------------------------------------------------------------------ #
    def start(self) -> None:
        """Begin acquire/renew loop (no-op backend traffic when standalone)."""
        if not self._config.enabled:
            with self._lock:
                self._leader = True
            _LOG.info(
                "leader election disabled — running standalone as leader "
                "(identity=%s)",
                self._identity,
            )
            return
        if self._backend is None:
            k8s = K8sLeaseBackend()
            self._backend = k8s if k8s.available else None
        if self._backend is None:
            with self._lock:
                self._leader = True
            _LOG.warning(
                "leader election enabled but no lease backend — falling back "
                "to standalone as leader (identity=%s)",
                self._identity,
            )
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._elect_loop,
            name=f"leader-elector-{self._config.lease_name}",
            daemon=True,
        )
        self._thread.start()
        _LOG.info(
            "leader elector started identity=%s lease=%s/%s duration=%.0fs",
            self._identity,
            self._config.lease_namespace,
            self._config.lease_name,
            self._config.lease_duration_seconds,
        )

    def stop(self) -> None:
        """Stop the loop; release the lease when we hold it (best-effort)."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        if (
            self._config.enabled
            and self._backend is not None
            and not self._abandoned
            and self.is_leader()
        ):
            try:
                self._backend.delete(
                    self._config.lease_name,
                    self._config.lease_namespace,
                    self._identity,
                )
            except Exception:  # noqa: BLE001 - best-effort release
                _LOG.debug("lease release failed", exc_info=True)
        with self._lock:
            was_leader = self._leader
            self._leader = self._leader if not self._config.enabled else False
        if was_leader and self._config.enabled:
            _LOG.info("leader elector stopped, released leadership")

    def abandon(self) -> None:
        """Stop renewing WITHOUT releasing (test helper: simulates pod death)."""
        self._abandoned = True
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        with self._lock:
            self._leader = False

    # ------------------------------------------------------------------ #
    def _elect_loop(self) -> None:
        """Background acquire/renew cycle until stop() / abandon()."""
        assert self._backend is not None
        interval = max(0.05, self._config.renew_interval_seconds)
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception:  # noqa: BLE001 - election must never crash daemon
                _LOG.debug("election tick failed", exc_info=True)
            self._stop.wait(interval)

    def _tick(self) -> None:
        """One acquire-or-renew attempt."""
        assert self._backend is not None
        now = self._clock()
        record = self._backend.read(self._config.lease_name, self._config.lease_namespace)
        if record is None or not record.holder:
            created = self._backend.create(
                self._config.lease_name,
                self._config.lease_namespace,
                LeaseRecord(
                    holder=self._identity,
                    renew_time=now,
                    duration_seconds=self._config.lease_duration_seconds,
                ),
            )
            self._set_leader(created)
            return
        if record.holder == self._identity:
            renewed = self._backend.update(
                self._config.lease_name,
                self._config.lease_namespace,
                LeaseRecord(
                    holder=self._identity,
                    renew_time=now,
                    duration_seconds=self._config.lease_duration_seconds,
                ),
                expected_version=record.resource_version,
            )
            self._set_leader(renewed)
            return
        effective_duration = record.duration_seconds or self._config.lease_duration_seconds
        if (now - record.renew_time) >= effective_duration:
            taken = self._backend.update(
                self._config.lease_name,
                self._config.lease_namespace,
                LeaseRecord(
                    holder=self._identity,
                    renew_time=now,
                    duration_seconds=self._config.lease_duration_seconds,
                ),
                expected_version=record.resource_version,
            )
            self._set_leader(taken)
            return
        self._set_leader(False)

    def _set_leader(self, leader: bool) -> None:
        with self._lock:
            changed = leader != self._leader
            self._leader = leader
        if changed and leader:
            _LOG.info(
                "became leader identity=%s lease=%s/%s",
                self._identity,
                self._config.lease_namespace,
                self._config.lease_name,
            )
        elif changed:
            _LOG.info(
                "leadership lost identity=%s lease=%s/%s — follower waiting",
                self._identity,
                self._config.lease_namespace,
                self._config.lease_name,
            )
        elif not leader:
            now = self._clock()
            if now - self._last_follower_log >= _FOLLOWER_LOG_THROTTLE_SECONDS:
                self._last_follower_log = now
                _LOG.info(
                    "follower waiting for leadership identity=%s lease=%s/%s",
                    self._identity,
                    self._config.lease_namespace,
                    self._config.lease_name,
                )


# --------------------------------------------------------------------------- #
# Gated runner + factory
# --------------------------------------------------------------------------- #

def run_leader_gated(
    elector: LeaderElector,
    run_fn: Callable[[], None],
    stop_fn: Callable[[], None],
    stop_event: threading.Event,
    poll_interval: float = 1.0,
) -> None:
    """Run ``run_fn`` only while ``elector`` is leader; block until stopped.

    ``run_fn`` is started in a worker thread on leadership acquire and torn
    down via ``stop_fn`` on leadership loss. Followers idle and log. Returns
    when ``stop_event`` is set (also tears down the worker). Health endpoints
    and read-only APIs live OUTSIDE this gate — they keep serving regardless.
    """
    worker: threading.Thread | None = None

    def _worker_alive() -> bool:
        return worker is not None and worker.is_alive()

    try:
        while not stop_event.is_set():
            if elector.is_leader():
                if not _worker_alive():
                    assert worker is None or not worker.is_alive()
                    worker = threading.Thread(
                        target=run_fn,
                        name="leader-gated-worker",
                        daemon=True,
                    )
                    worker.start()
                    _LOG.info("leader gate open — processing started")
                stop_event.wait(poll_interval)
            else:
                if _worker_alive():
                    _LOG.info("leader gate closed — stopping processing")
                    try:
                        stop_fn()
                    except Exception:  # noqa: BLE001 - teardown must not wedge gate
                        _LOG.debug("gated stop_fn failed", exc_info=True)
                    assert worker is not None
                    worker.join(timeout=10.0)
                    worker = None
                stop_event.wait(poll_interval)
    finally:
        if _worker_alive():
            try:
                stop_fn()
            except Exception:  # noqa: BLE001 - best-effort teardown
                _LOG.debug("gated stop_fn failed on shutdown", exc_info=True)
            assert worker is not None
            worker.join(timeout=10.0)


def build_elector(
    service: str,
    default_lease_name: str | None = None,
    backend: LeaseBackend | None = None,
) -> LeaderElector:
    """Build a LeaderElector for ``service`` from the environment.

    Disabled or backend-less builds return a standalone elector whose
    ``is_leader()`` is always True — the daemon then runs exactly as today.
    """
    config = LeaderElectionConfig.from_env(
        default_lease_name=default_lease_name or f"omniwatch-{service}-leader"
    )
    elector = LeaderElector(config, backend=backend)
    _LOG.info(
        "leader election config service=%s enabled=%s lease=%s/%s duration=%.0fs",
        service,
        config.enabled,
        config.lease_namespace,
        config.lease_name,
        config.lease_duration_seconds,
    )
    return elector
