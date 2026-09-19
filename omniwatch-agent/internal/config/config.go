// OmniWatch — Agent
// Component: config
// Phase: 1
// Purpose: YAML + env-var configuration loading for the omniwatch-agent
// Inputs: YAML config file + OMNIWATCH_* environment variables
// Outputs: Populated Config struct
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// FilelogOperatorField maps a json_parser timestamp/severity extraction rule.
type FilelogOperatorField struct {
	ParseFrom string `yaml:"parse_from"`
	Layout    string `yaml:"layout,omitempty"`
}

// FilelogOperator is a single filelog receiver operator (e.g. json_parser).
type FilelogOperator struct {
	Type      string                `yaml:"type"`
	Timestamp *FilelogOperatorField `yaml:"timestamp,omitempty"`
	Severity  *FilelogOperatorField `yaml:"severity,omitempty"`
}

// K8sObject is a single k8s_objects receiver watched object (e.g. pods).
type K8sObject struct {
	Name string `yaml:"name"`
}

// Config is the root configuration for the omniwatch-agent.
type Config struct {
	Agent struct {
		CollectionInterval time.Duration `yaml:"collection_interval"`
		LogLevel           string        `yaml:"log_level"`
		// HealthAddr is the :port (or host:port) the /health + /ready
		// server listens on. Flag -health-addr > OMNIWATCH_HEALTH_ADDR.
		HealthAddr string `yaml:"health_addr"`
		// ShutdownTimeout bounds graceful shutdown of the HTTP server,
		// collector drain, and OTel SDK providers.
		ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
		// HealthReadTimeout/HealthWriteTimeout bound the health HTTP server.
		HealthReadTimeout  time.Duration `yaml:"health_read_timeout"`
		HealthWriteTimeout time.Duration `yaml:"health_write_timeout"`
		// ExportInitTimeout bounds OTel SDK initialization at boot.
		ExportInitTimeout time.Duration `yaml:"export_init_timeout"`
		// HeartbeatEmitTimeout bounds a single guarded heartbeat emission.
		HeartbeatEmitTimeout time.Duration `yaml:"heartbeat_emit_timeout"`
	} `yaml:"agent"`
	Receiver struct {
		Hostmetrics struct {
			CollectionInterval time.Duration `yaml:"collection_interval"`
		} `yaml:"hostmetrics"`
		Filelog struct {
			Include   []string          `yaml:"include"`
			Exclude   []string          `yaml:"exclude"`
			StartAt   string            `yaml:"start_at"`
			Operators []FilelogOperator `yaml:"operators"`
		} `yaml:"filelog"`
		K8sObjects struct {
			CollectionInterval time.Duration `yaml:"collection_interval"`
			Mode               string        `yaml:"mode"` // watch or pull
			LabelSelector      string        `yaml:"label_selector"`
			FieldSelector      string        `yaml:"field_selector"`
			Objects            []K8sObject   `yaml:"objects"`
		} `yaml:"k8s_objects"`
		K8sCluster struct {
			CollectionInterval     time.Duration `yaml:"collection_interval"`
			NodeConditionsToReport []string      `yaml:"node_conditions_to_report"`
		} `yaml:"k8s_cluster"`
	} `yaml:"receiver"`
	Exporter struct {
		OTLP struct {
			Endpoint string `yaml:"endpoint"`
			Insecure bool   `yaml:"insecure"`
			// MetricExportInterval controls the periodic metric reader flush.
			MetricExportInterval time.Duration `yaml:"metric_export_interval"`
		} `yaml:"otlp"`
	} `yaml:"exporter"`
	TLS struct {
		CertRotationInterval time.Duration `yaml:"cert_rotation_interval"`
		SpiffeSocketPath     string        `yaml:"spiffe_socket_path"`
		TrustDomain          string        `yaml:"trust_domain"`
		// CertFile/KeyFile/CAFile pin a file-based identity used when the
		// SPIRE agent is unreachable (dev/test). Empty means SPIRE-only.
		CertFile string `yaml:"cert_file"`
		KeyFile  string `yaml:"key_file"`
		CAFile   string `yaml:"ca_file"`
		// FetchTimeout bounds a single Workload API attestation attempt.
		FetchTimeout time.Duration `yaml:"fetch_timeout"`
	} `yaml:"tls"`
	Auth      AuthConfig      `yaml:"auth"`
	Resilience ResilienceConfig `yaml:"resilience"`
	Alerting  AlertingConfig  `yaml:"alerting"`
	Beyla     BeylaConfig     `yaml:"beyla"`
}

// AlertingConfig tunes rule-file loading and SLO targets. Rule semantics
// (expr/for/labels) stay in configs/alerts/prometheusrules.yaml; these knobs
// only select the file, the group evaluation interval, and the numeric SLO
// targets checked by internal/alerting.
type AlertingConfig struct {
	RulesPath      string        `yaml:"rules_path"`
	ScrapeInterval time.Duration `yaml:"scrape_interval"`
	ForDefault     time.Duration `yaml:"for_default"`
	SLOHealth      float64       `yaml:"slo_health"`
	SLOExport      float64       `yaml:"slo_export"`
	SLOLatencyP99  float64       `yaml:"slo_latency_p99"`
}

// BeylaConfig tunes the Beyla eBPF sidecar. OTLPEndpoint empty means "follow
// the agent exporter endpoint" (with an http:// scheme added for Beyla v3).
type BeylaConfig struct {
	OTLPEndpoint      string `yaml:"otlp_endpoint"`
	LogLevel          string `yaml:"log_level"`
	DiscoveryPorts    string `yaml:"discovery_ports"`
	ExcludeNamespaces string `yaml:"exclude_namespaces"`
	WakeupLen         int    `yaml:"wakeup_len"`
}

// AuthConfig tunes the IND-3 health-endpoint auth chain.
// Mode is one of dev|apikey|mtls (dev disables all auth).
// TrustedCAFile pins the mTLS trust roots; API keys always come from
// OMNIWATCH_AUTH_API_KEYS (comma-list) and are never stored in YAML.
type AuthConfig struct {
	Mode          string `yaml:"mode"`
	TrustedCAFile string `yaml:"trusted_ca_file"`
	// APIKeys is populated from ENV only, never from YAML.
	APIKeys []string `yaml:"-"`
}

// ResilienceConfig tunes the IND-2 resilience library (circuit breaker,
// bounded queue, retry/backoff) guarding OTLP export.
type ResilienceConfig struct {
	// CircuitBreakerThreshold trips the breaker after N consecutive errors.
	CircuitBreakerThreshold uint32 `yaml:"circuit_breaker_threshold"`
	// CircuitBreakerTimeout is the open-state dwell before a half-open probe.
	CircuitBreakerTimeout time.Duration `yaml:"circuit_breaker_timeout"`
	// QueueSize caps pending heartbeat emissions; full queue drops oldest.
	QueueSize int `yaml:"queue_size"`
	// RetryMaxAttempts caps OTLP export attempts per emission.
	RetryMaxAttempts int `yaml:"retry_max_attempts"`
	// RetryBaseDelay is the first backoff wait (100ms → 500ms → 2s schedule).
	// Kept in sync with RetryBackoffs[0] so the singular knob and the full
	// schedule never disagree.
	RetryBaseDelay time.Duration `yaml:"retry_base_delay"`
	// RetryBackoffs is the per-wait backoff schedule (100ms, 500ms, 2s).
	RetryBackoffs []time.Duration `yaml:"retry_backoffs"`
	// BreakerInterval is the gobreaker counts window; stale failure counts
	// reset on this cadence so one old burst never trips the breaker forever.
	BreakerInterval time.Duration `yaml:"breaker_interval"`
	// BreakerMaxHalfOpenProbes caps concurrent requests in half-open state.
	BreakerMaxHalfOpenProbes uint32 `yaml:"breaker_max_half_open_probes"`
	// RetryMaxDelay caps the exponential backoff wait (growth is 5x).
	RetryMaxDelay time.Duration `yaml:"retry_max_delay"`
	// RetryJitterFraction adds uniform [0, base*fraction) spread per wait.
	RetryJitterFraction float64 `yaml:"retry_jitter_fraction"`
}

// Resilience defaults per the industry-ready plan: breaker trips after 5
// consecutive errors with a 30s reset, queue holds 1000 emissions, retry uses
// a 100ms → 500ms → 2s schedule with max 3 attempts.
func DefaultResilience() ResilienceConfig {
	return ResilienceConfig{
		CircuitBreakerThreshold:  5,
		CircuitBreakerTimeout:    30 * time.Second,
		BreakerInterval:          30 * time.Second,
		BreakerMaxHalfOpenProbes: 1,
		QueueSize:                1000,
		RetryMaxAttempts:         3,
		RetryBaseDelay:           100 * time.Millisecond,
		RetryBackoffs:            []time.Duration{100 * time.Millisecond, 500 * time.Millisecond, 2 * time.Second},
		RetryMaxDelay:            2 * time.Second,
		RetryJitterFraction:      0.5,
	}
}

// WithDefaults fills zero-valued resilience fields so a bare struct is safe.
func (r *ResilienceConfig) WithDefaults() {
	def := DefaultResilience()
	if r.CircuitBreakerThreshold == 0 {
		r.CircuitBreakerThreshold = def.CircuitBreakerThreshold
	}
	if r.CircuitBreakerTimeout <= 0 {
		r.CircuitBreakerTimeout = def.CircuitBreakerTimeout
	}
	if r.QueueSize <= 0 {
		r.QueueSize = def.QueueSize
	}
	if r.RetryMaxAttempts <= 0 {
		r.RetryMaxAttempts = def.RetryMaxAttempts
	}
	if r.RetryBaseDelay <= 0 {
		r.RetryBaseDelay = def.RetryBaseDelay
	}
	if len(r.RetryBackoffs) == 0 {
		r.RetryBackoffs = def.RetryBackoffs
	}
	if r.BreakerInterval <= 0 {
		r.BreakerInterval = def.BreakerInterval
	}
	if r.BreakerMaxHalfOpenProbes == 0 {
		r.BreakerMaxHalfOpenProbes = def.BreakerMaxHalfOpenProbes
	}
	if r.RetryMaxDelay <= 0 {
		r.RetryMaxDelay = def.RetryMaxDelay
	}
	if r.RetryJitterFraction <= 0 {
		r.RetryJitterFraction = def.RetryJitterFraction
	}
}

// Default returns a Config populated with default values.
func Default() *Config {
	c := &Config{}
	c.Agent.CollectionInterval = 60 * time.Second
	c.Agent.LogLevel = "info"
	c.Agent.HealthAddr = ":8080"
	c.Agent.ShutdownTimeout = 10 * time.Second
	c.Agent.HealthReadTimeout = 5 * time.Second
	c.Agent.HealthWriteTimeout = 5 * time.Second
	c.Agent.ExportInitTimeout = 10 * time.Second
	c.Agent.HeartbeatEmitTimeout = 10 * time.Second
	c.Receiver.Hostmetrics.CollectionInterval = 60 * time.Second
	c.Receiver.Filelog.Include = []string{"/var/log/pods/*/*/*.log", "/var/log/containers/*/*.log", "/var/log/kubernetes/audit/*.log"}
	c.Receiver.Filelog.Exclude = []string{"/var/log/pods/*/*/**.gz"}
	c.Receiver.Filelog.StartAt = "beginning"
	c.Receiver.Filelog.Operators = []FilelogOperator{
		{
			Type:      "json_parser",
			Timestamp: &FilelogOperatorField{ParseFrom: "attributes.time", Layout: "RFC3339"},
			Severity:  &FilelogOperatorField{ParseFrom: "attributes.level"},
		},
	}
	c.Receiver.K8sObjects.CollectionInterval = 3 * time.Minute
	c.Receiver.K8sObjects.Mode = "pull"
	c.Receiver.K8sObjects.LabelSelector = ""
	c.Receiver.K8sObjects.FieldSelector = ""
	c.Receiver.K8sCluster.CollectionInterval = 3 * time.Minute
	c.Receiver.K8sCluster.NodeConditionsToReport = []string{
		"Ready",
		"MemoryPressure",
		"DiskPressure",
		"PIDPressure",
		"NetworkUnavailable",
	}
	c.Exporter.OTLP.Endpoint = "otel-collector:4317"
	c.Exporter.OTLP.Insecure = true
	c.Exporter.OTLP.MetricExportInterval = 10 * time.Second
	c.TLS.CertRotationInterval = 24 * time.Hour
	c.TLS.SpiffeSocketPath = "unix:///tmp/spire-agent/public/api.sock"
	c.TLS.TrustDomain = "example.org"
	c.TLS.CertFile = ""
	c.TLS.KeyFile = ""
	c.TLS.CAFile = ""
	c.TLS.FetchTimeout = 5 * time.Second
	c.Auth.Mode = "dev"
	c.Auth.TrustedCAFile = ""
	c.Resilience = DefaultResilience()
	c.Alerting.RulesPath = "configs/alerts/prometheusrules.yaml"
	c.Alerting.ScrapeInterval = 30 * time.Second
	c.Alerting.ForDefault = 0
	c.Alerting.SLOHealth = 99.9
	c.Alerting.SLOExport = 99.0
	c.Alerting.SLOLatencyP99 = 5
	c.Beyla.OTLPEndpoint = ""
	c.Beyla.LogLevel = "INFO"
	c.Beyla.DiscoveryPorts = "80,443,8000-8999"
	c.Beyla.ExcludeNamespaces = "kube-system"
	c.Beyla.WakeupLen = 100
	return c
}

// EffectiveBeylaEndpoint resolves the OTLP endpoint the Beyla sidecar should
// use: an explicit Beyla override wins, otherwise the agent exporter endpoint
// is followed with an http:// scheme added (Beyla v3 rejects bare host:port).
func (c *Config) EffectiveBeylaEndpoint() string {
	if c.Beyla.OTLPEndpoint != "" {
		return c.Beyla.OTLPEndpoint
	}
	ep := c.Exporter.OTLP.Endpoint
	if strings.HasPrefix(ep, "http://") || strings.HasPrefix(ep, "https://") {
		return ep
	}
	return "http://" + ep
}

// Load reads the YAML file at path, then applies OMNIWATCH_* env overrides.
// Missing file is not an error: defaults are returned with env overrides applied.
func Load(path string) (*Config, error) {
	cfg := Default()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				applyEnvOverrides(cfg)
				return cfg, nil
			}
			return nil, fmt.Errorf("read config file: %w", err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config file: %w", err)
		}
	}
	applyEnvOverrides(cfg)
	return cfg, nil
}

func applyEnvOverrides(c *Config) {
	if v, ok := lookupEnv("OMNIWATCH_AGENT_COLLECTION_INTERVAL"); ok {
		if d, err := time.ParseDuration(v); err == nil {
			c.Agent.CollectionInterval = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_AGENT_LOG_LEVEL"); ok {
		c.Agent.LogLevel = v
	}
	if v, ok := lookupEnv("OMNIWATCH_HEALTH_ADDR"); ok {
		c.Agent.HealthAddr = v
	}
	if v, ok := lookupEnv("OMNIWATCH_AGENT_SHUTDOWN_TIMEOUT_S"); ok {
		if d, err := parseSeconds(v); err == nil && d > 0 {
			c.Agent.ShutdownTimeout = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_EXPORTER_INIT_TIMEOUT_S"); ok {
		if d, err := parseSeconds(v); err == nil && d > 0 {
			c.Agent.ExportInitTimeout = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_HEALTH_READ_TIMEOUT_S"); ok {
		if d, err := parseSeconds(v); err == nil && d > 0 {
			c.Agent.HealthReadTimeout = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_HEALTH_WRITE_TIMEOUT_S"); ok {
		if d, err := parseSeconds(v); err == nil && d > 0 {
			c.Agent.HealthWriteTimeout = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_HEARTBEAT_EMIT_TIMEOUT_S"); ok {
		if d, err := parseSeconds(v); err == nil && d > 0 {
			c.Agent.HeartbeatEmitTimeout = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_RECEIVER_HOSTMETRICS_COLLECTION_INTERVAL"); ok {
		if d, err := time.ParseDuration(v); err == nil {
			c.Receiver.Hostmetrics.CollectionInterval = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_RECEIVER_FILELOG_INCLUDE"); ok {
		c.Receiver.Filelog.Include = splitList(v)
	}
	if v, ok := lookupEnv("OMNIWATCH_RECEIVER_FILELOG_EXCLUDE"); ok {
		c.Receiver.Filelog.Exclude = splitList(v)
	}
	if v, ok := lookupEnv("OMNIWATCH_RECEIVER_FILELOG_START_AT"); ok {
		c.Receiver.Filelog.StartAt = v
	}
	if v, ok := lookupEnv("OMNIWATCH_RECEIVER_K8S_OBJECTS_COLLECTION_INTERVAL"); ok {
		if d, err := time.ParseDuration(v); err == nil {
			c.Receiver.K8sObjects.CollectionInterval = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_RECEIVER_K8S_OBJECTS_MODE"); ok {
		c.Receiver.K8sObjects.Mode = v
	}
	if v, ok := lookupEnv("OMNIWATCH_RECEIVER_K8S_OBJECTS_LABEL_SELECTOR"); ok {
		c.Receiver.K8sObjects.LabelSelector = v
	}
	if v, ok := lookupEnv("OMNIWATCH_RECEIVER_K8S_OBJECTS_FIELD_SELECTOR"); ok {
		c.Receiver.K8sObjects.FieldSelector = v
	}
	if v, ok := lookupEnv("OMNIWATCH_RECEIVER_K8S_OBJECTS"); ok {
		names := splitList(v)
		objs := make([]K8sObject, 0, len(names))
		for _, n := range names {
			objs = append(objs, K8sObject{Name: n})
		}
		if len(objs) > 0 {
			c.Receiver.K8sObjects.Objects = objs
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_RECEIVER_K8S_CLUSTER_COLLECTION_INTERVAL"); ok {
		if d, err := time.ParseDuration(v); err == nil {
			c.Receiver.K8sCluster.CollectionInterval = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_RECEIVER_K8S_CLUSTER_NODE_CONDITIONS_TO_REPORT"); ok {
		c.Receiver.K8sCluster.NodeConditionsToReport = splitList(v)
	}
	if v, ok := lookupEnv("OMNIWATCH_EXPORTER_OTLP_ENDPOINT"); ok {
		c.Exporter.OTLP.Endpoint = v
	}
	if v, ok := lookupEnv("OMNIWATCH_EXPORTER_OTLP_INSECURE"); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			c.Exporter.OTLP.Insecure = b
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_METRIC_EXPORT_INTERVAL_S"); ok {
		if d, err := parseSeconds(v); err == nil && d > 0 {
			c.Exporter.OTLP.MetricExportInterval = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_BEYLA_OTLP_ENDPOINT"); ok {
		c.Beyla.OTLPEndpoint = v
	}
	if v, ok := lookupEnv("OMNIWATCH_BEYLA_LOG_LEVEL"); ok {
		c.Beyla.LogLevel = v
	}
	if v, ok := lookupEnv("OMNIWATCH_BEYLA_DISCOVERY_PORTS"); ok {
		c.Beyla.DiscoveryPorts = v
	}
	if v, ok := lookupEnv("OMNIWATCH_BEYLA_EXCLUDE_NAMESPACES"); ok {
		c.Beyla.ExcludeNamespaces = v
	}
	if v, ok := lookupEnv("OMNIWATCH_BEYLA_WAKEUP_LEN"); ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.Beyla.WakeupLen = n
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_TLS_CERT_ROTATION_INTERVAL"); ok {
		if d, err := time.ParseDuration(v); err == nil {
			c.TLS.CertRotationInterval = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_TLS_SPIFFE_SOCKET_PATH"); ok {
		c.TLS.SpiffeSocketPath = v
	}
	if v, ok := lookupEnv("OMNIWATCH_TLS_TRUST_DOMAIN"); ok {
		c.TLS.TrustDomain = v
	}
	if v, ok := lookupEnv("OMNIWATCH_TLS_CERT_FILE"); ok {
		c.TLS.CertFile = v
	}
	if v, ok := lookupEnv("OMNIWATCH_TLS_KEY_FILE"); ok {
		c.TLS.KeyFile = v
	}
	if v, ok := lookupEnv("OMNIWATCH_TLS_CA_FILE"); ok {
		c.TLS.CAFile = v
	}
	if v, ok := lookupEnv("OMNIWATCH_TLS_FETCH_TIMEOUT_S"); ok {
		if d, err := parseSeconds(v); err == nil && d > 0 {
			c.TLS.FetchTimeout = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_ALERTING_RULES_PATH"); ok {
		c.Alerting.RulesPath = v
	}
	if v, ok := lookupEnv("OMNIWATCH_ALERTING_SCRAPE_INTERVAL"); ok {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			c.Alerting.ScrapeInterval = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_ALERTING_FOR_DEFAULT"); ok {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			c.Alerting.ForDefault = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_SLO_HEALTH"); ok {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			c.Alerting.SLOHealth = f
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_SLO_EXPORT"); ok {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			c.Alerting.SLOExport = f
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_SLO_LATENCY_P99"); ok {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			c.Alerting.SLOLatencyP99 = f
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_AUTH_MODE"); ok {
		c.Auth.Mode = v
	}
	if v, ok := lookupEnv("OMNIWATCH_AUTH_TRUSTED_CA_FILE"); ok {
		c.Auth.TrustedCAFile = v
	}
	if v, ok := lookupEnv("OMNIWATCH_AUTH_API_KEYS"); ok {
		c.Auth.APIKeys = splitList(v)
	}
	if v, ok := lookupEnv("OMNIWATCH_RESILIENCE_QUEUE_SIZE"); ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.Resilience.QueueSize = n
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_RESILIENCE_CIRCUIT_BREAKER_THRESHOLD"); ok {
		if n, err := strconv.ParseUint(v, 10, 32); err == nil && n > 0 {
			c.Resilience.CircuitBreakerThreshold = uint32(n)
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_RESILIENCE_CIRCUIT_BREAKER_TIMEOUT"); ok {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			c.Resilience.CircuitBreakerTimeout = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_RESILIENCE_RETRY_MAX_ATTEMPTS"); ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.Resilience.RetryMaxAttempts = n
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_RESILIENCE_RETRY_BACKOFFS"); ok {
		if ds, err := splitDurations(v); err == nil && len(ds) > 0 {
			c.Resilience.RetryBackoffs = ds
			c.Resilience.RetryBaseDelay = ds[0]
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_RESILIENCE_BREAKER_INTERVAL_S"); ok {
		if d, err := parseSeconds(v); err == nil && d > 0 {
			c.Resilience.BreakerInterval = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_RESILIENCE_BREAKER_MAX_HALF_OPEN_PROBES"); ok {
		if n, err := strconv.ParseUint(v, 10, 32); err == nil && n > 0 {
			c.Resilience.BreakerMaxHalfOpenProbes = uint32(n)
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_RESILIENCE_RETRY_MAX_DELAY"); ok {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			c.Resilience.RetryMaxDelay = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_RESILIENCE_RETRY_JITTER_FRACTION"); ok {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
			c.Resilience.RetryJitterFraction = f
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_RESILIENCE_RETRY_BASE_DELAY"); ok {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			c.Resilience.RetryBaseDelay = d
			if len(c.Resilience.RetryBackoffs) > 0 {
				c.Resilience.RetryBackoffs[0] = d
			} else {
				c.Resilience.RetryBackoffs = []time.Duration{d}
			}
		}
	}
	c.Resilience.WithDefaults()
}

func lookupEnv(key string) (string, bool) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return "", false
	}
	return v, true
}

// parseSeconds parses a bare-seconds value ("10", "2.5") into a duration.
// The _S-suffixed timeout knobs use plain seconds so K8s env values stay
// numeric; a Go duration string ("10s") is also accepted.
func parseSeconds(v string) (time.Duration, error) {
	if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
		return time.Duration(f * float64(time.Second)), nil
	}
	return time.ParseDuration(strings.TrimSpace(v))
}

func splitDurations(v string) ([]time.Duration, error) {
	parts := strings.Split(v, ",")
	out := make([]time.Duration, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		d, err := time.ParseDuration(p)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

func splitList(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
