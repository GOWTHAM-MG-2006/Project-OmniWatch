"""
OmniWatch — Common Utilities
Component: OpenTelemetry SDK Setup
Phase: 1
Purpose: Shared OTel SDK initialization (metrics, traces, logs) with OTLP gRPC exporter
Inputs: Service name, endpoint configuration
Outputs: Configured MeterProvider, TracerProvider, LoggerProvider instances
"""

import atexit
import logging

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_NAMESPACE, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Module-level logger for internal use
_logger = logging.getLogger(__name__)

# Module-level state tracking
_providers_initialized: bool = False
_meter_provider: MeterProvider | None = None
_tracer_provider: TracerProvider | None = None
_logger_provider: LoggerProvider | None = None


def init_otel(
    service_name: str,
    otel_endpoint: str = "http://otelcol:4317",
    environment: str = "simulation",
) -> None:
    """
    Initialize OpenTelemetry SDK with metrics, traces, and logs providers.

    Call this once at service startup before any OTel operations.

    Args:
        service_name: Name of the service (e.g., "ingestion-api")
        otel_endpoint: OTel Collector gRPC endpoint
        environment: Deployment environment (default: "simulation")
    """
    global _providers_initialized, _meter_provider, _tracer_provider, _logger_provider

    if _providers_initialized:
        _logger.warning(
            "OTel already initialized — skipping duplicate init_otel() call"
        )
        return

    _logger.info(f"Initializing OTel SDK for service: {service_name}")

    # Build resource attributes
    resource = Resource.create(
        {
            SERVICE_NAME: service_name,
            SERVICE_NAMESPACE: "omniwatch",
            "deployment.environment": environment,
        }
    )

    try:
        # --- Traces ---
        span_exporter = OTLPSpanExporter(endpoint=otel_endpoint, insecure=True)
        tp = TracerProvider(resource=resource)
        tp.add_span_processor(BatchSpanProcessor(span_exporter))
        trace.set_tracer_provider(tp)
        _logger.info(f"TracerProvider initialized → {otel_endpoint}")

        # --- Metrics ---
        metric_exporter = OTLPMetricExporter(endpoint=otel_endpoint, insecure=True)
        metric_reader = PeriodicExportingMetricReader(
            exporter=metric_exporter,
            export_interval_millis=5000,
        )
        mp = MeterProvider(
            resource=resource,
            metric_readers=[metric_reader],
        )
        metrics.set_meter_provider(mp)
        _logger.info(f"MeterProvider initialized → {otel_endpoint} (5s interval)")

        # --- Logs ---
        log_exporter = OTLPLogExporter(endpoint=otel_endpoint, insecure=True)
        lp = LoggerProvider(resource=resource)
        lp.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
        _logger.info(f"LoggerProvider initialized → {otel_endpoint}")

        _tracer_provider = tp
        _meter_provider = mp
        _logger_provider = lp
        _setup_log_handler()
        _providers_initialized = True
        _logger.info(f"OTel SDK fully initialized for '{service_name}'")

    except Exception as exc:  # noqa: BLE001 — OTel SDK can raise any exception during init; must not crash service
        _logger.warning(
            f"OTel initialization failed (service={service_name}, "
            f"endpoint={otel_endpoint}): {exc}. "
            f"Telemetry will be disabled for this service."
        )
        # Ensure providers are None on failure
        _tracer_provider = None
        _meter_provider = None
        _logger_provider = None

    # Register cleanup on process exit
    atexit.register(shutdown_otel)


def get_meter(name: str = "omniwatch") -> metrics.Meter:
    """
    Get a named Meter instance from the global MeterProvider.

    Args:
        name: Meter name (default: "omniwatch")

    Returns:
        A Meter instance (or no-op if OTel init failed)
    """
    return metrics.get_meter(name)


def get_tracer(name: str = "omniwatch") -> trace.Tracer:
    """
    Get a named Tracer instance from the global TracerProvider.

    Args:
        name: Tracer name (default: "omniwatch")

    Returns:
        A Tracer instance (or no-op if OTel init failed)
    """
    return trace.get_tracer(name)


def get_logger(
    name: str = "omniwatch",
    version: str = "0.1.0",
) -> logging.Logger:
    """
    Get a named Python Logger instrumented with OTel log export.

    Args:
        name: Logger name (default: "omniwatch")
        version: Logger version (default: "0.1.0")

    Returns:
        A logging.Logger configured with OTel LoggingHandler
    """
    logger = logging.getLogger(name)

    # Attach OTel handler if provider is available and handler not already present
    if _logger_provider is not None:
        has_otel_handler = any(isinstance(h, LoggingHandler) for h in logger.handlers)
        if not has_otel_handler:
            otel_handler = LoggingHandler(
                level=logging.NOTSET,
                logger_provider=_logger_provider,
            )
            logger.addHandler(otel_handler)

    return logger


def shutdown_otel() -> None:
    """
    Gracefully shut down all OTel providers.

    Safe to call multiple times. Registered via atexit for automatic cleanup.
    """
    global _providers_initialized

    if not _providers_initialized:
        return

    _logger.info("Shutting down OTel providers...")

    errors: list[str] = []

    if _tracer_provider is not None:
        try:
            _tracer_provider.shutdown()
        except Exception as exc:  # noqa: BLE001 — shutdown guard must never crash the process
            errors.append(f"TracerProvider shutdown failed: {exc}")

    if _meter_provider is not None:
        try:
            _meter_provider.shutdown()
        except Exception as exc:  # noqa: BLE001 — shutdown guard must never crash the process
            errors.append(f"MeterProvider shutdown failed: {exc}")

    if _logger_provider is not None:
        try:
            _logger_provider.shutdown()
        except Exception as exc:  # noqa: BLE001 — shutdown guard must never crash the process
            errors.append(f"LoggerProvider shutdown failed: {exc}")

    if errors:
        for err in errors:
            _logger.error(err)
    else:
        _logger.info("OTel providers shut down successfully")

    _providers_initialized = False


def _setup_log_handler() -> None:
    """
    Configure root logger to route standard Python logs through OTel.

    Adds a LoggingHandler that forwards logs to the OTel Collector.
    Existing handlers are preserved.
    """
    root_logger = logging.getLogger()
    if _logger_provider is not None:
        has_otel = any(isinstance(h, LoggingHandler) for h in root_logger.handlers)
        if not has_otel:
            otel_handler = LoggingHandler(
                level=logging.NOTSET,
                logger_provider=_logger_provider,
            )
            root_logger.addHandler(otel_handler)
            _logger.info("OTel LoggingHandler attached to root logger")
