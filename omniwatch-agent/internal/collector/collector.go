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
	"sync"
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
	"github.com/omniwatch/omniwatch-agent/internal/exporter"
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

	tracer     trace.Tracer
	counter    metric.Int64Counter
	otelLogger log.Logger

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
	return c
}

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

	emitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	c.emitHeartbeat(emitCtx)
	cancel()

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
				emitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				c.emitHeartbeat(emitCtx)
				cancel()
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
