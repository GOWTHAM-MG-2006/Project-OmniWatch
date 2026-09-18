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
		} `yaml:"otlp"`
	} `yaml:"exporter"`
	TLS struct {
		CertRotationInterval time.Duration `yaml:"cert_rotation_interval"`
		SpiffeSocketPath     string        `yaml:"spiffe_socket_path"`
		TrustDomain          string        `yaml:"trust_domain"`
	} `yaml:"tls"`
	Auth AuthConfig `yaml:"auth"`
	Resilience ResilienceConfig `yaml:"resilience"`
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
}

// Resilience defaults per the industry-ready plan: breaker trips after 5
// consecutive errors with a 30s reset, queue holds 1000 emissions, retry uses
// a 100ms → 500ms → 2s schedule with max 3 attempts.
func DefaultResilience() ResilienceConfig {
	return ResilienceConfig{
		CircuitBreakerThreshold: 5,
		CircuitBreakerTimeout:   30 * time.Second,
		QueueSize:               1000,
		RetryMaxAttempts:        3,
		RetryBaseDelay:          100 * time.Millisecond,
		RetryBackoffs:           []time.Duration{100 * time.Millisecond, 500 * time.Millisecond, 2 * time.Second},
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
}

// Default returns a Config populated with default values.
func Default() *Config {
	c := &Config{}
	c.Agent.CollectionInterval = 60 * time.Second
	c.Agent.LogLevel = "info"
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
	c.Receiver.K8sObjects.CollectionInterval = 15 * time.Minute
	c.Receiver.K8sObjects.Mode = "pull"
	c.Receiver.K8sObjects.LabelSelector = ""
	c.Receiver.K8sObjects.FieldSelector = ""
	c.Receiver.K8sCluster.CollectionInterval = 10 * time.Minute
	c.Receiver.K8sCluster.NodeConditionsToReport = []string{
		"Ready",
		"MemoryPressure",
		"DiskPressure",
		"PIDPressure",
		"NetworkUnavailable",
	}
	c.Exporter.OTLP.Endpoint = "otel-collector:4317"
	c.Exporter.OTLP.Insecure = true
	c.TLS.CertRotationInterval = 24 * time.Hour
	c.TLS.SpiffeSocketPath = "unix:///tmp/spire-agent/public/api.sock"
	c.TLS.TrustDomain = "example.org"
	c.Auth.Mode = "dev"
	c.Auth.TrustedCAFile = ""
	c.Resilience = DefaultResilience()
	return c
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
	if v, ok := lookupEnv("OMNIWATCH_RECEIVER_HOSTMETRICS_COLLECTION_INTERVAL"); ok {
		if d, err := time.ParseDuration(v); err == nil {
			c.Receiver.Hostmetrics.CollectionInterval = d
		}
	}
	if v, ok := lookupEnv("OMNIWATCH_RECEIVER_FILELOG_INCLUDE"); ok {
		c.Receiver.Filelog.Include = splitList(v)
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
