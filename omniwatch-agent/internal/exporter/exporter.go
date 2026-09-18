// OmniWatch — Agent
// Component: exporter
// Phase: 1
// Purpose: OTel SDK initialization with OTLP gRPC export for self-telemetry
// Inputs: OTLP endpoint + insecure flag (from config), resource env vars
// Outputs: Configured global trace/meter/logger providers exporting via OTLP gRPC
package exporter

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	agenttls "github.com/omniwatch/omniwatch-agent/internal/tls"
	"github.com/omniwatch/omniwatch-agent/internal/resilience"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// Service identity attached to every exported signal.
const (
	ServiceName    = "omniwatch-agent"
	ServiceVersion = "0.1.0"
)

// defaultMetricExportInterval controls how often the periodic reader flushes
// metrics. Kept short so self-telemetry is visible in otelcol within seconds.
// Override per-exporter via NewWithMetricInterval (OMNIWATCH_METRIC_EXPORT_INTERVAL_S).
const defaultMetricExportInterval = 10 * time.Second

// Exporter holds the initialized OTel SDK providers. All signals are
// exported via OTLP gRPC only — there is intentionally no Prometheus
// exporter anywhere in this package.
type Exporter struct {
	endpoint string
	tp       *sdktrace.TracerProvider
	mp       *sdkmetric.MeterProvider
	lp       *sdklog.LoggerProvider
	tlsCreds *agenttls.TLSCredentials
	breaker  *resilience.CircuitBreaker
	retry    *resilience.RetryPolicy
}

// New initializes the OTel SDK with OTLP gRPC exporters for traces, metrics
// and logs. endpoint is used verbatim (e.g. "otel-collector:4317"); when
// insecure is true the gRPC connection skips TLS (dev-only fallback with a
// loud warning), otherwise mTLS credentials are built from tlsOpts via SPIRE.
// Resource attributes (service.name, service.version, deployment.environment,
// host.name, k8s.pod.name, k8s.namespace.name) are attached to every signal.
func New(ctx context.Context, endpoint string, insecure bool, tlsOpts agenttls.Options) (*Exporter, error) {
	return NewWithMetricInterval(ctx, endpoint, insecure, tlsOpts, 0)
}

// NewWithMetricInterval is New with an explicit periodic-reader flush
// interval. A non-positive interval keeps the 10s default.
func NewWithMetricInterval(ctx context.Context, endpoint string, insecure bool, tlsOpts agenttls.Options, metricInterval time.Duration) (*Exporter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("exporter: OTLP endpoint is empty")
	}
	if metricInterval <= 0 {
		metricInterval = defaultMetricExportInterval
	}

	res, err := newResource()
	if err != nil {
		return nil, err
	}

	traceOpts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(endpoint)}
	metricOpts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(endpoint)}
	logOpts := []otlploggrpc.Option{otlploggrpc.WithEndpoint(endpoint)}
	var tlsCreds *agenttls.TLSCredentials
	if insecure {
		slog.Warn(agenttls.InsecureFallbackWarning)
		traceOpts = append(traceOpts, otlptracegrpc.WithInsecure())
		metricOpts = append(metricOpts, otlpmetricgrpc.WithInsecure())
		logOpts = append(logOpts, otlploggrpc.WithInsecure())
	} else {
		creds, tc, err := agenttls.Resolve(ctx, tlsOpts, false, slog.Default())
		if err != nil {
			return nil, fmt.Errorf("exporter: mTLS credentials: %w", err)
		}
		tlsCreds = tc
		// The caller's ctx is cancelled right after New returns (main.go
		// expCancel), so detach: rotation lives until Shutdown closes it.
		tlsCreds.StartRotation(context.WithoutCancel(ctx))
		traceOpts = append(traceOpts, otlptracegrpc.WithTLSCredentials(creds))
		metricOpts = append(metricOpts, otlpmetricgrpc.WithTLSCredentials(creds))
		logOpts = append(logOpts, otlploggrpc.WithTLSCredentials(creds))
	}

	traceExp, err := otlptracegrpc.New(ctx, traceOpts...)
	if err != nil {
		return nil, fmt.Errorf("exporter: create OTLP trace exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(traceExp)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	metricExp, err := otlpmetricgrpc.New(ctx, metricOpts...)
	if err != nil {
		return nil, fmt.Errorf("exporter: create OTLP metric exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(
			metricExp,
			sdkmetric.WithInterval(metricInterval),
		)),
	)
	otel.SetMeterProvider(mp)

	logExp, err := otlploggrpc.New(ctx, logOpts...)
	if err != nil {
		return nil, fmt.Errorf("exporter: create OTLP log exporter: %w", err)
	}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)),
	)
	global.SetLoggerProvider(lp)

	retry := resilience.DefaultRetryPolicy()
	return &Exporter{
		endpoint: endpoint,
		tp:       tp,
		mp:       mp,
		lp:       lp,
		tlsCreds: tlsCreds,
		breaker:  resilience.NewCircuitBreaker(resilience.BreakerSettings{Name: "otlp-export"}),
		retry:    &retry,
	}, nil
}

// ConfigureResilience swaps the breaker/retry guards (the collector wires
// config values here). Nil arguments are ignored. The New signature is
// unaffected: every exporter starts with production defaults.
func (e *Exporter) ConfigureResilience(b *resilience.CircuitBreaker, r *resilience.RetryPolicy) {
	if b != nil {
		e.breaker = b
	}
	if r != nil {
		e.retry = r
	}
}

// ForceFlush pushes all providers (traces, metrics, logs) to the OTLP
// endpoint; the first flush error wins. Against a dead endpoint this is the
// call that fails and feeds the breaker.
func (e *Exporter) ForceFlush(ctx context.Context) error {
	if err := e.tp.ForceFlush(ctx); err != nil {
		return fmt.Errorf("exporter: trace flush: %w", err)
	}
	if err := e.mp.ForceFlush(ctx); err != nil {
		return fmt.Errorf("exporter: metric flush: %w", err)
	}
	if err := e.lp.ForceFlush(ctx); err != nil {
		return fmt.Errorf("exporter: log flush: %w", err)
	}
	return nil
}

// ExportWithResilience emits via emit, then flushes, guarded by retry first
// and the circuit breaker around the whole retried call. When the breaker is
// open this fails fast without touching the endpoint.
func (e *Exporter) ExportWithResilience(ctx context.Context, emit func(context.Context)) error {
	return resilience.Guarded(ctx, e.breaker, e.retry, func(ctx context.Context) error {
		emit(ctx)
		return e.ForceFlush(ctx)
	})
}

// newResource builds the resource with the required attributes:
// service.name=omniwatch-agent, service.version, deployment.environment,
// host.name, k8s.pod.name, k8s.namespace.name. K8s attributes are only set
// when the corresponding environment provides a value.
func newResource() (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		semconv.ServiceName(ServiceName),
		semconv.ServiceVersion(ServiceVersion),
		semconv.DeploymentEnvironment(envOr("DEPLOYMENT_ENVIRONMENT", "development")),
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		attrs = append(attrs, semconv.HostName(host))
	}
	if pod := firstNonEmpty(
		os.Getenv("K8S_POD_NAME"),
		os.Getenv("POD_NAME"),
		os.Getenv("HOSTNAME"),
	); pod != "" {
		attrs = append(attrs, semconv.K8SPodName(pod))
	}
	if ns := firstNonEmpty(
		os.Getenv("K8S_NAMESPACE"),
		os.Getenv("POD_NAMESPACE"),
	); ns != "" {
		attrs = append(attrs, semconv.K8SNamespaceName(ns))
	}
	return resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, attrs...),
	)
}

// Shutdown flushes and shuts down all providers, then releases mTLS credentials.
func (e *Exporter) Shutdown(ctx context.Context) error {
	if e.tlsCreds != nil {
		_ = e.tlsCreds.Close()
	}
	var firstErr error
	if err := e.tp.Shutdown(ctx); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("exporter: trace provider shutdown: %w", err)
	}
	if err := e.mp.Shutdown(ctx); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("exporter: meter provider shutdown: %w", err)
	}
	if err := e.lp.Shutdown(ctx); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("exporter: logger provider shutdown: %w", err)
	}
	return firstErr
}

// Endpoint returns the OTLP endpoint this exporter was configured with.
func (e *Exporter) Endpoint() string { return e.endpoint }

// Tracer returns a tracer backed by the configured trace provider.
func (e *Exporter) Tracer(name string) oteltrace.Tracer { return e.tp.Tracer(name) }

// Meter returns a meter backed by the configured meter provider.
func (e *Exporter) Meter(name string) metric.Meter { return e.mp.Meter(name) }

// Logger returns a logger backed by the configured logger provider.
func (e *Exporter) Logger(name string) log.Logger { return e.lp.Logger(name) }

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
