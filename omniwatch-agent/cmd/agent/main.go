// OmniWatch — Agent
// Component: main entrypoint
// Phase: 1
// Purpose: Agent binary entrypoint with config, health server, and graceful shutdown
// Inputs: Config file path (--config flag, AGENT_CONFIG env, or configs/agent.yaml)
// Outputs: Running agent process; OTLP self-telemetry to otelcol:4317
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/omniwatch/omniwatch-agent/internal/beyla"
	"github.com/omniwatch/omniwatch-agent/internal/collector"
	"github.com/omniwatch/omniwatch-agent/internal/config"
	"github.com/omniwatch/omniwatch-agent/internal/exporter"
	"github.com/omniwatch/omniwatch-agent/internal/health"
	agenttls "github.com/omniwatch/omniwatch-agent/internal/tls"
)

const (
	healthAddr        = ":8080"
	defaultConfigPath = "configs/agent.yaml"
	shutdownTimeout   = 10 * time.Second
)

// Config loading follows the internal/config API built by T2:
// config.Load(path) returns defaults (plus OMNIWATCH_* env overrides) when the
// file is absent, so the agent starts standalone with a missing config file.
func main() {
	os.Exit(run())
}

func run() int {
	cfg, configPath := loadConfig()

	level := slog.LevelInfo
	switch strings.ToLower(cfg.Agent.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	logger.Info("agent starting",
		"config_path", configPath,
		"log_level", cfg.Agent.LogLevel,
		"collection_interval", cfg.Agent.CollectionInterval.String())

	// Beyla/eBPF preflight (IND-6): advisory kernel + eBPF availability gate.
	// Non-fatal by design: an unsupported host only degrades Beyla traces,
	// the agent still runs its receivers.
	beyla.LogCheck(logger)

	// OTel SDK initialization (T4): trace, meter and logger providers
	// exporting via OTLP gRPC to the endpoint from config.
	otlpEndpoint := cfg.Exporter.OTLP.Endpoint
	tlsOpts := agenttls.Options{
		SpiffeSocketPath:     cfg.TLS.SpiffeSocketPath,
		TrustDomain:          cfg.TLS.TrustDomain,
		CertRotationInterval: cfg.TLS.CertRotationInterval,
	}
	expCtx, expCancel := context.WithTimeout(context.Background(), 10*time.Second)
	otelExp, err := exporter.New(expCtx, otlpEndpoint, cfg.Exporter.OTLP.Insecure, tlsOpts)
	expCancel()
	if err != nil {
		logger.Error("otel SDK initialization failed", "error", err)
		return 1
	}
	logger.Info("otel SDK initialized",
		"endpoint", otlpEndpoint,
		"insecure", cfg.Exporter.OTLP.Insecure,
		"service_name", exporter.ServiceName,
		"service_version", exporter.ServiceVersion)

	healthSrv := health.NewServer()
	coll := collector.New(cfg, otelExp, logger)
	if err := coll.Start(context.Background()); err != nil {
		logger.Error("collector start failed", "error", err)
		healthSrv.SetCollectorStatus(health.CollectorDegraded)
	} else {
		healthSrv.SetCollectorStatus(health.CollectorRunning)
	}

	httpSrv := &http.Server{
		Addr:         healthAddr,
		Handler:      healthSrv.Handler(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	errCh := make(chan error, 1)
	go func() {
		logger.Info("health server listening", "addr", healthAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case sig := <-sigCh:
		logger.Info("shutdown signal received", "signal", sig.String())
	case err := <-errCh:
		if err != nil {
			logger.Error("health server failed", "error", err)
			return 1
		}
		return 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	logger.Info("shutting down health server")
	if err := httpSrv.Shutdown(ctx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
		return 1
	}

	logger.Info("stopping collector")
	if err := coll.Shutdown(ctx); err != nil {
		logger.Error("collector shutdown failed", "error", err)
	}
	logger.Info("shutting down otel SDK")
	if err := otelExp.Shutdown(ctx); err != nil {
		logger.Error("otel SDK shutdown failed", "error", err)
	}
	healthSrv.SetCollectorStatus(health.CollectorStopped)
	logger.Info("shutdown complete")
	return 0
}

// loadConfig resolves the config path (--config flag > AGENT_CONFIG env >
// configs/agent.yaml) and loads it via config.Load. A missing/unreadable file
// falls back to defaults so the agent still starts.
func loadConfig() (*config.Config, string) {
	flagPath := flag.String("config", "", "path to agent config file")
	flag.Parse()
	path := *flagPath
	if path == "" {
		path = os.Getenv("AGENT_CONFIG")
	}
	if path == "" {
		path = defaultConfigPath
	}
	cfg, err := config.Load(path)
	if err != nil {
		cfg = config.Default()
		slog.Warn("config load failed, using defaults", "config_path", path, "error", err)
	}
	return cfg, path
}
