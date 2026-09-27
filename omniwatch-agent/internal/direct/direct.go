// OmniWatch — Agent
// Component: direct (importer HTTP export)
// Phase: exporter-importer-split
// Purpose: Ship telemetry straight to a workspace importer over HTTP OTLP/JSON
// with a Bearer API token — no collector, no container, no gRPC (which wedges
// on some customer iron). Additive to the OTLP path: the collector prefers
// this client only when an importer endpoint is configured.
// Inputs: Importer endpoint + API token + exporter identity (config/env)
// Outputs: POST {endpoint} /ingest batches for the 10 telemetry types
package direct

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/omniwatch/omniwatch-agent/internal/resilience"
)

// Telemetry types accepted by the importer /ingest contract (spec §B).
const (
	TypeMetrics        = "metrics"
	TypeLogs           = "logs"
	TypeTraces         = "traces"
	TypeMetadata       = "metadata"
	TypeState          = "state"
	TypeAuditLogs      = "audit_logs"
	TypeProfiling      = "profiling"
	TypeSIEM           = "siem"
	TypeAuthLogs       = "auth_logs"
	TypeSecurityAlerts = "security_alerts"
)

// MetricPoint mirrors importer MetricPoint JSON.
type MetricPoint struct {
	MetricName string            `json:"metric_name"`
	Value      float64           `json:"value"`
	Tags       map[string]string `json:"tags,omitempty"`
	SourceType string            `json:"source_type,omitempty"`
	Timestamp  string            `json:"timestamp,omitempty"`
}

// LogPoint mirrors importer LogPoint JSON.
type LogPoint struct {
	LogLevel    string `json:"log_level,omitempty"`
	Message     string `json:"message"`
	ServiceName string `json:"service_name,omitempty"`
	TraceID     string `json:"trace_id,omitempty"`
	Timestamp   string `json:"timestamp,omitempty"`
}

// SpanPoint mirrors importer SpanPoint JSON.
type SpanPoint struct {
	TraceID      string  `json:"trace_id"`
	SpanID       string  `json:"span_id"`
	ParentSpanID string  `json:"parent_span_id,omitempty"`
	ServiceName  string  `json:"service_name,omitempty"`
	Operation    string  `json:"operation,omitempty"`
	DurationMs   float64 `json:"duration_ms,omitempty"`
	Timestamp    string  `json:"timestamp,omitempty"`
}

// IngestBody mirrors importer IngestBody JSON.
type IngestBody struct {
	TelemetryType string        `json:"telemetry_type"`
	EntityID      string        `json:"entity_id"`
	EntityType    string        `json:"entity_type,omitempty"`
	Metrics       []MetricPoint `json:"metrics,omitempty"`
	Logs          []LogPoint    `json:"logs,omitempty"`
	Traces        []SpanPoint   `json:"traces,omitempty"`
}

// Config holds the importer connection + exporter identity. The API token
// comes from the environment only and is never logged.
type Config struct {
	Endpoint       string
	APIToken       string
	ExporterNumber int
	ExporterName   string
	EntityID       string
	Timeout        time.Duration
}

// LoadFromEnv builds a Config from OMNIWATCH_IMPORTER_* variables.
// Empty Endpoint means "direct mode off" — the caller keeps the OTLP path.
func LoadFromEnv() Config {
	cfg := Config{ExporterNumber: 1, Timeout: 15 * time.Second}
	cfg.Endpoint = strings.TrimSpace(os.Getenv("OMNIWATCH_IMPORTER_ENDPOINT"))
	cfg.APIToken = os.Getenv("OMNIWATCH_IMPORTER_API_TOKEN")
	cfg.ExporterName = strings.TrimSpace(os.Getenv("OMNIWATCH_IMPORTER_EXPORTER_NAME"))
	cfg.EntityID = strings.TrimSpace(os.Getenv("OMNIWATCH_IMPORTER_ENTITY_ID"))
	if n, err := strconv.Atoi(strings.TrimSpace(os.Getenv("OMNIWATCH_IMPORTER_EXPORTER_NUMBER"))); err == nil && n > 0 {
		cfg.ExporterNumber = n
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		if cfg.EntityID == "" {
			cfg.EntityID = fmt.Sprintf("exporter-%d-%s", cfg.ExporterNumber, host)
		}
		if cfg.ExporterName == "" {
			cfg.ExporterName = host
		}
	}
	return cfg
}

// Client ships ingest batches to one workspace importer, guarded by the
// shared breaker/retry primitives.
type Client struct {
	cfg     Config
	http    *http.Client
	logger  *slog.Logger
	breaker *resilience.CircuitBreaker
	retry   *resilience.RetryPolicy
}

// New builds a Client with production-default guards.
func New(cfg Config, logger *slog.Logger) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	retry := resilience.DefaultRetryPolicy()
	return &Client{
		cfg:     cfg,
		http:    &http.Client{Timeout: cfg.Timeout},
		logger:  logger,
		breaker: resilience.NewCircuitBreaker(resilience.BreakerSettings{Name: "importer-direct"}),
		retry:   &retry,
	}
}

// ConfigureResilience swaps the breaker/retry guards. Nil arguments ignored.
func (c *Client) ConfigureResilience(b *resilience.CircuitBreaker, r *resilience.RetryPolicy) {
	if b != nil {
		c.breaker = b
	}
	if r != nil {
		c.retry = r
	}
}

// Endpoint returns the importer URL batches are shipped to.
func (c *Client) Endpoint() string { return c.cfg.Endpoint }

// Send posts one ingest batch; 200/202 is success, anything else an error.
func (c *Client) Send(ctx context.Context, body IngestBody) error {
	if c.cfg.Endpoint == "" {
		return fmt.Errorf("direct: importer endpoint is empty")
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("direct: marshal %s batch: %w", body.TelemetryType, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.Endpoint, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("direct: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.APIToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIToken)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("direct: post %s batch: %w", body.TelemetryType, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("direct: post %s batch: status %d: %s",
			body.TelemetryType, resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	return nil
}

// ExportWithResilience ships every batch, each guarded by retry-inside-breaker.
// The first failure wins; when the breaker is open this fails fast.
func (c *Client) ExportWithResilience(ctx context.Context, batches []IngestBody) error {
	for i := range batches {
		b := batches[i]
		if err := resilience.Guarded(ctx, c.breaker, c.retry, func(ctx context.Context) error {
			return c.Send(ctx, b)
		}); err != nil {
			return err
		}
	}
	c.logger.Info("direct export succeeded", "batches", len(batches), "endpoint", c.cfg.Endpoint)
	return nil
}

// newID returns 16 random hex bytes for synthetic trace/span IDs.
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// BuildHeartbeat assembles the always-on batches: host metrics, heartbeat
// log + heartbeat trace, metadata/state envelopes, and an agent self-profile
// span sourced from runtime/pprof. Security batches come from TailSecurity,
// never fabricated.
func (c *Client) BuildHeartbeat() []IngestBody {
	now := time.Now().UTC().Format(time.RFC3339)
	entity := c.cfg.EntityID
	stats := readHostStats()
	prof := selfProfile()

	metrics := []MetricPoint{
		{MetricName: "cpu_load_1m", Value: stats.Load1, SourceType: "heartbeat", Tags: map[string]string{"host": stats.Host}, Timestamp: now},
		{MetricName: "mem_used_percent", Value: stats.MemUsedPct, SourceType: "heartbeat", Tags: map[string]string{"host": stats.Host}, Timestamp: now},
		{MetricName: "disk_used_percent", Value: stats.DiskUsedPct, SourceType: "heartbeat", Tags: map[string]string{"host": stats.Host}, Timestamp: now},
		{MetricName: "go_goroutines", Value: float64(prof.Goroutines), SourceType: "heartbeat", Tags: map[string]string{"host": stats.Host}, Timestamp: now},
	}
	traceID := newID()
	hbSpanID := newID()
	batches := []IngestBody{
		{TelemetryType: TypeMetrics, EntityID: entity, EntityType: "API_NODE", Metrics: metrics},
		{TelemetryType: TypeLogs, EntityID: entity, EntityType: "API_NODE", Logs: []LogPoint{
			{LogLevel: "INFO", Message: "exporter heartbeat", ServiceName: "omniwatch-agent", Timestamp: now},
		}},
		{TelemetryType: TypeTraces, EntityID: entity, EntityType: "API_NODE", Traces: []SpanPoint{
			{TraceID: traceID, SpanID: hbSpanID, ServiceName: "omniwatch-agent", Operation: "exporter.heartbeat", Timestamp: now},
		}},
		{TelemetryType: TypeProfiling, EntityID: entity, EntityType: "API_NODE", Traces: []SpanPoint{
			{TraceID: traceID, SpanID: newID(), ParentSpanID: hbSpanID, ServiceName: "omniwatch-agent", Operation: "runtime.profile(self)", DurationMs: prof.GCPauseIntervalMs, Timestamp: now},
		}},
	}
	meta, _ := json.Marshal(map[string]string{
		"hostname": stats.Host, "goos": runtime.GOOS, "uptime_s": fmt.Sprintf("%d", stats.UptimeS),
		"exporter_name": c.cfg.ExporterName, "exporter_number": strconv.Itoa(c.cfg.ExporterNumber),
	})
	batches = append(batches,
		IngestBody{TelemetryType: TypeMetadata, EntityID: entity, EntityType: "API_NODE",
			Logs: []LogPoint{{LogLevel: "INFO", Message: string(meta), ServiceName: "omniwatch-agent", Timestamp: now}}},
		IngestBody{TelemetryType: TypeState, EntityID: entity, EntityType: "API_NODE",
			Logs: []LogPoint{{LogLevel: "INFO", Message: string(meta), ServiceName: "omniwatch-agent", Timestamp: now}}},
	)
	return batches
}
