// OmniWatch — Agent
// Component: main entrypoint
// Phase: 1
// Purpose: Agent binary entrypoint with config, health server, and graceful shutdown
// Inputs: Config file path (--config flag, AGENT_CONFIG env, /etc/omniwatch/agent.yaml, or configs/agent.yaml)
// Outputs: Running agent process; OTLP self-telemetry to the configured exporter endpoint
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
	"github.com/omniwatch/omniwatch-agent/internal/auth"
)

const (
	defaultHealthAddr = ":8080"
	// defaultConfigPath is the in-image path (Dockerfile COPYs agent.yaml
	// there and CMD passes it explicitly). Local runs fall back to the
	// repo-relative path when the image path is absent (see loadConfig).
	defaultConfigPath       = "/etc/omniwatch/agent.yaml"
	localDefaultConfigPath  = "configs/agent.yaml"
	defaultShutdownTimeout  = 10 * time.Second
	defaultExportTimeout    = 10 * time.Second
	defaultHealthIOTimeout  = 5 * time.Second
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

	prodGuard(logger, cfg)

	// Beyla/eBPF preflight (IND-6): advisory kernel + eBPF availability gate.
	// Non-fatal by design: an unsupported host only degrades Beyla traces,
	// the agent still runs its receivers. Also logs the effective Beyla OTLP
	// endpoint so boot logs prove Beyla follows the agent endpoint.
	beyla.LogEffectiveEndpoint(logger, cfg.Beyla.OTLPEndpoint, cfg.Exporter.OTLP.Endpoint)

	// OTel SDK initialization (T4): trace, meter and logger providers
	// exporting via OTLP gRPC to the endpoint from config.
	otlpEndpoint := cfg.Exporter.OTLP.Endpoint
	tlsOpts := agenttls.Options{
		SpiffeSocketPath:     cfg.TLS.SpiffeSocketPath,
		TrustDomain:          cfg.TLS.TrustDomain,
		CertRotationInterval: cfg.TLS.CertRotationInterval,
		CertFile:             cfg.TLS.CertFile,
		KeyFile:              cfg.TLS.KeyFile,
		CAFile:               cfg.TLS.CAFile,
		FetchTimeout:         cfg.TLS.FetchTimeout,
	}
	exportTimeout := cfg.Agent.ExportInitTimeout
	if exportTimeout <= 0 {
		exportTimeout = defaultExportTimeout
	}
	expCtx, expCancel := context.WithTimeout(context.Background(), exportTimeout)
	otelExp, err := exporter.NewWithMetricInterval(expCtx, otlpEndpoint, cfg.Exporter.OTLP.Insecure, tlsOpts, cfg.Exporter.OTLP.MetricExportInterval)
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

	healthAddr := cfg.Agent.HealthAddr
	if flagHealthAddr != "" {
		healthAddr = flagHealthAddr
	}
	if healthAddr == "" {
		healthAddr = defaultHealthAddr
	}
	readTimeout := cfg.Agent.HealthReadTimeout
	if readTimeout <= 0 {
		readTimeout = defaultHealthIOTimeout
	}
	writeTimeout := cfg.Agent.HealthWriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = defaultHealthIOTimeout
	}
	httpSrv := &http.Server{
		Addr:         healthAddr,
		Handler:      healthHandler(cfg, logger, healthSrv),
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
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

	shutdownTimeout := cfg.Agent.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = defaultShutdownTimeout
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

// healthHandler wraps the health mux with the IND-3 auth chain
// (mTLS -> API key -> dev bypass) built from cfg.Auth.
func healthHandler(cfg *config.Config, logger *slog.Logger, srv *health.Server) http.Handler {
	a, err := auth.NewForMode(cfg.Auth.Mode, cfg.Auth.APIKeys, cfg.Auth.TrustedCAFile)
	if err != nil {
		logger.Warn("health auth: unknown mode, falling back to dev bypass (availability-safe); set OMNIWATCH_AUTH_MODE to dev|apikey|mtls",
			"error", err, "bad_mode", cfg.Auth.Mode)
	}
	logger.Info("health auth configured", "mode", a.Mode())
	return srv.HandlerWithAuth(a)
}

// prodGuard logs ERROR-level warnings when OMNIWATCH_ENV=prod runs with
// dev/insecure defaults. Defaults are frozen (behavior freeze); this only
// makes prod misconfiguration loud.
func prodGuard(logger *slog.Logger, cfg *config.Config) {
	if !strings.EqualFold(os.Getenv("OMNIWATCH_ENV"), "prod") {
		return
	}
	if strings.EqualFold(cfg.TLS.TrustDomain, "example.org") {
		logger.Error("prod guard: tls.trust_domain is still example.org; set OMNIWATCH_TLS_TRUST_DOMAIN",
			"trust_domain", cfg.TLS.TrustDomain)
	}
	if strings.EqualFold(strings.TrimSpace(cfg.Auth.Mode), "dev") {
		logger.Error("prod guard: auth.mode is dev (no authentication); set OMNIWATCH_AUTH_MODE to apikey|mtls",
			"mode", cfg.Auth.Mode)
	}
	if cfg.Exporter.OTLP.Insecure {
		logger.Error("prod guard: exporter OTLP insecure is true (plaintext gRPC); set OMNIWATCH_EXPORTER_OTLP_INSECURE=false with SPIRE/file mTLS",
			"insecure", true)
	}
}

// flagHealthAddr holds the -health-addr flag value (flag > env > default).
var flagHealthAddr string

// loadConfig resolves the config path (--config flag > AGENT_CONFIG env >
// /etc/omniwatch/agent.yaml when present > configs/agent.yaml) and loads it
// via config.Load. A missing/unreadable file falls back to defaults so the
// agent still starts.
func loadConfig() (*config.Config, string) {
	flagPath := flag.String("config", "", "path to agent config file")
	flag.StringVar(&flagHealthAddr, "health-addr", "", "health probe listen address (default: :8080)")
	flag.Parse()
	path := *flagPath
	if path == "" {
		path = os.Getenv("AGENT_CONFIG")
	}
	if path == "" {
		if _, err := os.Stat(defaultConfigPath); err == nil {
			path = defaultConfigPath
		} else {
			path = localDefaultConfigPath
		}
	}
	cfg, err := config.Load(path)
	if err != nil {
		cfg = config.Default()
		slog.Warn("config load failed, using defaults", "config_path", path, "error", err)
	}
	return cfg, path
}
