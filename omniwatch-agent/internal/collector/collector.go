// OmniWatch — Agent
// Component: collector
// Phase: 1
// Purpose: In-process telemetry collection pipeline (receivers → processors → exporters)
// Inputs: Agent config, initialized OTel exporter, slog logger
// Outputs: Self-telemetry (traces/metrics/logs) exported via OTLP gRPC
package collector

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	// Receiver factories for the pipeline stages. They are registered for the
	// pipeline configured below (hostmetrics, filelog, k8scluster, k8sobjects);
	// transport to the backend is OTLP gRPC via the exporter package.
	_ "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/filelogreceiver"
	_ "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver"
	_ "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver"
	_ "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sobjectsreceiver"

	"github.com/omniwatch/omniwatch-agent/internal/config"
	"github.com/omniwatch/omniwatch-agent/internal/direct"
	"github.com/omniwatch/omniwatch-agent/internal/docker"
	"github.com/omniwatch/omniwatch-agent/internal/exporter"
	"github.com/omniwatch/omniwatch-agent/internal/resilience"
)

// instrumentationScope identifies telemetry produced by this collector.
const instrumentationScope = "github.com/omniwatch/omniwatch-agent/internal/collector"

// Pipeline describes the receivers → processors → exporters wiring of the
// collector, derived from the agent config at construction time.
type Pipeline struct {
	Receivers  []string
	Processors []string
	Exporters  []string
}

// Collector runs the agent's telemetry collection loop and emits
// self-telemetry heartbeats (one span, one metric datapoint, one log record
// per tick) through the OTLP exporter.
type Collector struct {
	cfg      *config.Config
	exp      *exporter.Exporter
	logger   *slog.Logger
	build    component.BuildInfo
	pipeline Pipeline

	// direct ships heartbeats straight to a workspace importer over HTTP.
	// Nil unless cfg.Importer.Endpoint is set; when present it replaces the
	// OTLP path for heartbeats (receivers still describe the pipeline).
	direct *direct.Client

	// apptailer/appoller ship real container telemetry (logs + stats) for
	// the configured allowlist. Nil unless docker containers are configured;
	// nil means heartbeat-only mode.
	apptailer *docker.Tailer
	appoller  *docker.Poller

	tracer     trace.Tracer
	counter    metric.Int64Counter
	otelLogger log.Logger

	// queue bounds pending heartbeat emissions: a dead OTLP endpoint cannot
	// grow memory unboundedly. Full queue drops oldest + counts
	// omniwatch.agent.queue_dropped; notifyCh wakes the drain worker.
	queue    *resilience.BoundedQueue[struct{}]
	notifyCh chan struct{}

	emitted atomic.Uint64

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// New builds a Collector from the agent config and an initialized exporter.
// It describes the pipeline from config and prepares the instruments; use
// Start to begin collection.
func New(cfg *config.Config, exp *exporter.Exporter, logger *slog.Logger) *Collector {
	c := &Collector{
		cfg:    cfg,
		exp:    exp,
		logger: logger,
		build: component.BuildInfo{
			Command: "omniwatch-agent",
			Version: exporter.ServiceVersion,
		},
		pipeline: Pipeline{
			Receivers: []string{
				"hostmetrics",
				"filelog",
				"k8s_cluster",
				"k8s_objects",
			},
			Processors: []string{"batch"},
			Exporters:  []string{"otlp"},
		},
		stopCh: make(chan struct{}),
	}
	c.tracer = exp.Tracer(instrumentationScope)
	c.otelLogger = exp.Logger(instrumentationScope)
	counter, err := exp.Meter(instrumentationScope).Int64Counter(
		"omniwatch.agent.heartbeat",
		metric.WithDescription("Agent self-telemetry heartbeat count."),
	)
	if err != nil {
		logger.Warn("heartbeat counter init failed, heartbeats will skip metrics", "error", err)
	}
	c.counter = counter

	// Wire IND-2 resilience from config: breaker + retry guard the OTLP
	// export path, the bounded queue caps pending heartbeat emissions.
	rc := cfg.Resilience
	rc.WithDefaults()
	breaker := resilience.NewCircuitBreaker(resilience.BreakerSettings{
		Name:              "otlp-export",
		FailureThreshold:  rc.CircuitBreakerThreshold,
		Interval:          rc.BreakerInterval,
		OpenTimeout:       rc.CircuitBreakerTimeout,
		MaxHalfOpenProbes: rc.BreakerMaxHalfOpenProbes,
	})
	retry := resilience.RetryPolicy{
		MaxAttempts:    rc.RetryMaxAttempts,
		BaseDelay:      resilience.DefaultRetryBaseDelay,
		MaxDelay:       rc.RetryMaxDelay,
		JitterFraction: rc.RetryJitterFraction,
	}
	if len(rc.RetryBackoffs) > 0 {
		retry.BaseDelay = rc.RetryBackoffs[0]
		retry.MaxDelay = rc.RetryBackoffs[len(rc.RetryBackoffs)-1]
	}
	if rc.RetryBaseDelay > 0 {
		retry.BaseDelay = rc.RetryBaseDelay
	}
	exp.ConfigureResilience(breaker, &retry)

	if cfg.Importer.Endpoint != "" {
		dcfg := direct.Config{
			Endpoint:       cfg.Importer.Endpoint,
			APIToken:       cfg.Importer.APIToken,
			ExporterNumber: cfg.Importer.ExporterNumber,
			ExporterName:   cfg.Importer.ExporterName,
			EntityID:       cfg.Importer.EntityID,
			Timeout:        15 * time.Second,
		}
		if dcfg.ExporterNumber <= 0 {
			dcfg.ExporterNumber = 1
		}
		if dcfg.EntityID == "" {
			if host, err := os.Hostname(); err == nil && host != "" {
				dcfg.EntityID = fmt.Sprintf("exporter-%d-%s", dcfg.ExporterNumber, host)
			} else {
				dcfg.EntityID = fmt.Sprintf("exporter-%d", dcfg.ExporterNumber)
			}
		}
		c.direct = direct.New(dcfg, logger)
		c.direct.ConfigureResilience(breaker, &retry)
		c.pipeline.Exporters = []string{"importer-direct"}
		logger.Info("direct importer export enabled", "endpoint", dcfg.Endpoint)
		if len(cfg.Importer.Docker.Containers) > 0 {
			dcli := docker.New(cfg.Importer.Docker.SocketPath)
			c.apptailer = docker.NewTailer(dcli, dcfg.EntityID, cfg.Importer.Docker.Containers, cfg.Importer.Docker.LogTail, logger)
			c.appoller = docker.NewPoller(dcli, dcfg.EntityID, cfg.Importer.Docker.Containers, logger)
			logger.Info("docker app telemetry enabled", "containers", cfg.Importer.Docker.Containers, "socket", dcli.SocketPath())
		}
	}

	dropCounter, err := exp.Meter(instrumentationScope).Int64Counter(
		resilience.QueueDroppedMetric,
		metric.WithDescription("Heartbeat emissions dropped from a full bounded queue."),
	)
	if err != nil {
		logger.Warn("queue drop counter init failed, drops will be counted locally only", "error", err)
	}
	c.queue = resilience.NewBoundedQueue[struct{}](rc.QueueSize, logger, dropCounter)
	c.notifyCh = make(chan struct{}, 1)
	return c
}

// QueueLen returns pending heartbeat emissions (for tests/observability).
func (c *Collector) QueueLen() int { return c.queue.Len() }

// QueueDropped returns the running drop-oldest total (for tests/observability).
func (c *Collector) QueueDropped() int64 { return c.queue.Dropped() }

// scheduleHeartbeat enqueues one emission without blocking; on a full queue
// the oldest pending emission is dropped (counted + logged by the queue).
func (c *Collector) scheduleHeartbeat() {
	c.queue.TryEnqueue(struct{}{})
	select {
	case c.notifyCh <- struct{}{}:
	default: // worker already awake; it drains everything queued
	}
}

// drain emits every queued heartbeat through the guarded export path
// (retry-inside-breaker + flush). It abandons the backlog on shutdown.
func (c *Collector) drain() {
	for {
		select {
		case <-c.stopCh:
			return
		default:
		}
		if _, ok := c.queue.Dequeue(); !ok {
			return
		}
		// Batch assembly (host stats, docker tail/stats, security tail) runs
		// outside the emit timeout: on small boxes the docker API calls
		// alone can exceed it, which previously expired the context before
		// the breaker ever executed. The timeout below bounds the network
		// export only.
		if c.direct != nil {
			batches := c.direct.BuildHeartbeat()
			batches = append(batches, c.direct.TailSecurity()...)
			// Real app telemetry rides the same emission (one extra batch
			// per source, still one POST per batch). Nil-safe: unconfigured
			// tailer/poller return nil and the heartbeat path is untouched.
			if c.apptailer != nil {
				batches = append(batches, c.apptailer.Collect()...)
			}
			if c.appoller != nil {
				batches = append(batches, c.appoller.Collect()...)
			}
			emitTimeout := c.cfg.Agent.HeartbeatEmitTimeout
			if emitTimeout <= 0 {
				emitTimeout = 30 * time.Second
			}
			emitCtx, cancel := context.WithTimeout(context.Background(), emitTimeout)
			if err := c.direct.ExportWithResilience(emitCtx, batches); err != nil {
				c.logger.Warn("direct heartbeat export failed (guarded)", "error", err)
			} else {
				n := c.emitted.Add(1)
				c.logger.Info("direct heartbeat export succeeded", "emitted", n, "endpoint", c.direct.Endpoint())
			}
			cancel()
			continue
		}
		emitTimeout := c.cfg.Agent.HeartbeatEmitTimeout
		if emitTimeout <= 0 {
			emitTimeout = 30 * time.Second
		}
		emitCtx, cancel := context.WithTimeout(context.Background(), emitTimeout)
		if err := c.exp.ExportWithResilience(emitCtx, c.emitHeartbeat); err != nil {
			c.logger.Warn("heartbeat export failed (guarded)", "error", err)
		} else {
			n := c.emitted.Add(1)
			c.logger.Info("heartbeat export succeeded", "emitted", n, "endpoint", c.exp.Endpoint())
		}
		cancel()
	}
}

func (c *Collector) Emitted() uint64 { return c.emitted.Load() }

// Pipeline returns the configured receivers → processors → exporters wiring.
func (c *Collector) Pipeline() Pipeline { return c.pipeline }

// Start initializes the collector: it logs the pipeline wiring, emits an
// immediate heartbeat so telemetry is visible without waiting a full
// collection interval, then collects on every tick of the configured
// collection interval until Shutdown. It is safe to call Start once.
func (c *Collector) Start(ctx context.Context) error {
	c.logger.Info("collector starting",
		"build_command", c.build.Command,
		"build_version", c.build.Version,
		"receivers", c.pipeline.Receivers,
		"processors", c.pipeline.Processors,
		"exporters", c.pipeline.Exporters,
		"otlp_endpoint", c.exp.Endpoint(),
		"hostmetrics_interval", c.cfg.Receiver.Hostmetrics.CollectionInterval.String(),
		"filelog_include", c.cfg.Receiver.Filelog.Include,
		"k8s_objects_mode", c.cfg.Receiver.K8sObjects.Mode,
		"k8s_cluster_interval", c.cfg.Receiver.K8sCluster.CollectionInterval.String(),
	)

	// Drain worker: serializes guarded emissions so a slow/dead endpoint
	// backs up into the bounded queue instead of piling up goroutines.
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.stopCh:
				return
			case <-c.notifyCh:
				c.drain()
			}
		}
	}()

	c.scheduleHeartbeat() // immediate emission so telemetry is visible at boot

	interval := c.cfg.Agent.CollectionInterval
	if interval <= 0 {
		interval = time.Minute
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-c.stopCh:
				return
			case <-ticker.C:
				c.scheduleHeartbeat()
			}
		}
	}()

	c.logger.Info("collector started", "collection_interval", interval.String())
	return nil
}

// Shutdown stops the collection loop and waits for the in-flight tick to
// finish, bounded by ctx.
func (c *Collector) Shutdown(ctx context.Context) error {
	c.stopOnce.Do(func() { close(c.stopCh) })
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.wg.Wait()
	}()
	select {
	case <-done:
		c.logger.Info("collector stopped")
		return nil
	case <-ctx.Done():
		return fmt.Errorf("collector: shutdown timed out: %w", ctx.Err())
	}
}

// emitHeartbeat produces one span, one metric datapoint and one log record
// so each signal path to the OTLP endpoint is exercised every tick.
func (c *Collector) emitHeartbeat(ctx context.Context) {
	tctx, span := c.tracer.Start(ctx, "agent.heartbeat",
		trace.WithAttributes(
			attribute.String("agent.build", c.build.Version),
			attribute.String("collection.interval", c.cfg.Agent.CollectionInterval.String()),
		),
	)
	if c.counter != nil {
		c.counter.Add(tctx, 1, metric.WithAttributes(
			attribute.String("agent.build", c.build.Version),
		))
	}
	rec := log.Record{}
	rec.SetSeverity(log.SeverityInfo)
	rec.SetBody(log.StringValue("agent heartbeat"))
	rec.AddAttributes(
		log.String("agent.build", c.build.Version),
		log.String("otlp.endpoint", c.exp.Endpoint()),
	)
	c.otelLogger.Emit(tctx, rec)
	span.End()
}
