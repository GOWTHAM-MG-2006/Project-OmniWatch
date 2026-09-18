// OmniWatch — Agent
// Component: config (tests)
// Phase: 1
// Purpose: Unit tests for YAML loading and env-var overrides
// Inputs: Temp YAML files + OMNIWATCH_* env vars
// Outputs: go test results
package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

const testYAML = `agent:
  collection_interval: 30s
  log_level: debug
receiver:
  hostmetrics:
    collection_interval: 30s
  filelog:
    include:
      - /var/log/app.log
      - /var/log/sys.log
  k8s_objects:
    collection_interval: 5m
    mode: watch
    label_selector: "app=test"
    field_selector: "metadata.namespace=default"
  k8s_cluster:
    collection_interval: 5m
    node_conditions_to_report:
      - Ready
      - MemoryPressure
exporter:
  otlp:
    endpoint: test-collector:4317
    insecure: false
`

func writeTempYAML(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "agent.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp yaml: %v", err)
	}
	return path
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"OMNIWATCH_AGENT_COLLECTION_INTERVAL",
		"OMNIWATCH_AGENT_LOG_LEVEL",
		"OMNIWATCH_RECEIVER_HOSTMETRICS_COLLECTION_INTERVAL",
		"OMNIWATCH_RECEIVER_FILELOG_INCLUDE",
		"OMNIWATCH_RECEIVER_K8S_OBJECTS_COLLECTION_INTERVAL",
		"OMNIWATCH_RECEIVER_K8S_OBJECTS_MODE",
		"OMNIWATCH_RECEIVER_K8S_OBJECTS_LABEL_SELECTOR",
		"OMNIWATCH_RECEIVER_K8S_OBJECTS_FIELD_SELECTOR",
		"OMNIWATCH_RECEIVER_K8S_CLUSTER_COLLECTION_INTERVAL",
		"OMNIWATCH_RECEIVER_K8S_CLUSTER_NODE_CONDITIONS_TO_REPORT",
		"OMNIWATCH_EXPORTER_OTLP_ENDPOINT",
		"OMNIWATCH_EXPORTER_OTLP_INSECURE",
		"OMNIWATCH_HEALTH_ADDR",
		"OMNIWATCH_AGENT_SHUTDOWN_TIMEOUT_S",
		"OMNIWATCH_EXPORTER_INIT_TIMEOUT_S",
		"OMNIWATCH_HEALTH_READ_TIMEOUT_S",
		"OMNIWATCH_HEALTH_WRITE_TIMEOUT_S",
		"OMNIWATCH_HEARTBEAT_EMIT_TIMEOUT_S",
		"OMNIWATCH_METRIC_EXPORT_INTERVAL_S",
		"OMNIWATCH_TLS_CERT_FILE",
		"OMNIWATCH_TLS_KEY_FILE",
		"OMNIWATCH_TLS_CA_FILE",
		"OMNIWATCH_TLS_FETCH_TIMEOUT_S",
		"OMNIWATCH_BEYLA_OTLP_ENDPOINT",
		"OMNIWATCH_BEYLA_LOG_LEVEL",
		"OMNIWATCH_BEYLA_DISCOVERY_PORTS",
		"OMNIWATCH_BEYLA_EXCLUDE_NAMESPACES",
		"OMNIWATCH_BEYLA_WAKEUP_LEN",
		"OMNIWATCH_ALERTING_RULES_PATH",
		"OMNIWATCH_ALERTING_SCRAPE_INTERVAL",
		"OMNIWATCH_ALERTING_FOR_DEFAULT",
		"OMNIWATCH_SLO_HEALTH",
		"OMNIWATCH_SLO_EXPORT",
		"OMNIWATCH_SLO_LATENCY_P99",
		"OMNIWATCH_RESILIENCE_BREAKER_INTERVAL_S",
		"OMNIWATCH_RESILIENCE_BREAKER_MAX_HALF_OPEN_PROBES",
		"OMNIWATCH_RESILIENCE_RETRY_MAX_DELAY",
		"OMNIWATCH_RESILIENCE_RETRY_JITTER_FRACTION",
		"OMNIWATCH_RECEIVER_FILELOG_EXCLUDE",
		"OMNIWATCH_RECEIVER_FILELOG_START_AT",
		"OMNIWATCH_RECEIVER_K8S_OBJECTS",
	} {
		t.Setenv(k, "")
		if err := os.Unsetenv(k); err != nil {
			t.Fatalf("unset %s: %v", k, err)
		}
	}
}

func TestLoadFromYAML(t *testing.T) {
	clearEnv(t)
	path := writeTempYAML(t, testYAML)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Agent.CollectionInterval != 30*time.Second {
		t.Errorf("agent interval = %v, want 30s", cfg.Agent.CollectionInterval)
	}
	if cfg.Agent.LogLevel != "debug" {
		t.Errorf("log level = %q, want debug", cfg.Agent.LogLevel)
	}
	if cfg.Receiver.Hostmetrics.CollectionInterval != 30*time.Second {
		t.Errorf("hostmetrics interval = %v, want 30s", cfg.Receiver.Hostmetrics.CollectionInterval)
	}
	if len(cfg.Receiver.Filelog.Include) != 2 || cfg.Receiver.Filelog.Include[0] != "/var/log/app.log" {
		t.Errorf("filelog include = %v", cfg.Receiver.Filelog.Include)
	}
	if cfg.Receiver.K8sObjects.Mode != "watch" {
		t.Errorf("k8s mode = %q, want watch", cfg.Receiver.K8sObjects.Mode)
	}
	if cfg.Receiver.K8sObjects.LabelSelector != "app=test" {
		t.Errorf("label selector = %q", cfg.Receiver.K8sObjects.LabelSelector)
	}
	if cfg.Receiver.K8sObjects.FieldSelector != "metadata.namespace=default" {
		t.Errorf("field selector = %q", cfg.Receiver.K8sObjects.FieldSelector)
	}
	if len(cfg.Receiver.K8sCluster.NodeConditionsToReport) != 2 {
		t.Errorf("node conditions = %v", cfg.Receiver.K8sCluster.NodeConditionsToReport)
	}
	if cfg.Exporter.OTLP.Endpoint != "test-collector:4317" {
		t.Errorf("endpoint = %q", cfg.Exporter.OTLP.Endpoint)
	}
	if cfg.Exporter.OTLP.Insecure {
		t.Errorf("insecure = true, want false")
	}
}

func TestEnvOverridesYAML(t *testing.T) {
	clearEnv(t)
	path := writeTempYAML(t, testYAML)

	t.Setenv("OMNIWATCH_AGENT_LOG_LEVEL", "warn")
	t.Setenv("OMNIWATCH_AGENT_COLLECTION_INTERVAL", "90s")
	t.Setenv("OMNIWATCH_EXPORTER_OTLP_ENDPOINT", "localhost:5000")
	t.Setenv("OMNIWATCH_EXPORTER_OTLP_INSECURE", "true")
	t.Setenv("OMNIWATCH_RECEIVER_K8S_OBJECTS_MODE", "pull")
	t.Setenv("OMNIWATCH_RECEIVER_FILELOG_INCLUDE", "/tmp/a.log,/tmp/b.log")
	t.Setenv("OMNIWATCH_RECEIVER_K8S_CLUSTER_NODE_CONDITIONS_TO_REPORT", "Ready")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Agent.LogLevel != "warn" {
		t.Errorf("log level = %q, want warn (env override)", cfg.Agent.LogLevel)
	}
	if cfg.Agent.CollectionInterval != 90*time.Second {
		t.Errorf("agent interval = %v, want 90s (env override)", cfg.Agent.CollectionInterval)
	}
	if cfg.Exporter.OTLP.Endpoint != "localhost:5000" {
		t.Errorf("endpoint = %q, want localhost:5000 (env override)", cfg.Exporter.OTLP.Endpoint)
	}
	if !cfg.Exporter.OTLP.Insecure {
		t.Errorf("insecure = false, want true (env override)")
	}
	if cfg.Receiver.K8sObjects.Mode != "pull" {
		t.Errorf("k8s mode = %q, want pull (env override)", cfg.Receiver.K8sObjects.Mode)
	}
	if len(cfg.Receiver.Filelog.Include) != 2 || cfg.Receiver.Filelog.Include[1] != "/tmp/b.log" {
		t.Errorf("filelog include = %v, want env override", cfg.Receiver.Filelog.Include)
	}
	if len(cfg.Receiver.K8sCluster.NodeConditionsToReport) != 1 || cfg.Receiver.K8sCluster.NodeConditionsToReport[0] != "Ready" {
		t.Errorf("node conditions = %v, want [Ready] (env override)", cfg.Receiver.K8sCluster.NodeConditionsToReport)
	}
	// Non-overridden values must survive from YAML.
	if cfg.Receiver.K8sObjects.LabelSelector != "app=test" {
		t.Errorf("label selector = %q, want YAML value preserved", cfg.Receiver.K8sObjects.LabelSelector)
	}
}

func TestLoadFilelogReceiver(t *testing.T) {
	clearEnv(t)
	path := filepath.Join("..", "..", "configs", "agent.yaml")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		path = writeTempYAML(t, testFilelogYAML)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	wantInclude := []string{
		"/var/log/pods/*/*/*.log",
		"/var/log/containers/*/*.log",
		"/var/log/kubernetes/audit/*.log",
	}
	if len(cfg.Receiver.Filelog.Include) != len(wantInclude) {
		t.Fatalf("filelog include = %v, want %v", cfg.Receiver.Filelog.Include, wantInclude)
	}
	for i, want := range wantInclude {
		if cfg.Receiver.Filelog.Include[i] != want {
			t.Errorf("filelog include[%d] = %q, want %q", i, cfg.Receiver.Filelog.Include[i], want)
		}
	}
	if len(cfg.Receiver.Filelog.Exclude) != 1 || cfg.Receiver.Filelog.Exclude[0] != "/var/log/pods/*/*/**.gz" {
		t.Errorf("filelog exclude = %v, want [/var/log/pods/*/*/**.gz]", cfg.Receiver.Filelog.Exclude)
	}
	if cfg.Receiver.Filelog.StartAt != "beginning" {
		t.Errorf("filelog start_at = %q, want beginning", cfg.Receiver.Filelog.StartAt)
	}
	if len(cfg.Receiver.Filelog.Operators) != 1 {
		t.Fatalf("filelog operators = %v, want 1 json_parser operator", cfg.Receiver.Filelog.Operators)
	}
	op := cfg.Receiver.Filelog.Operators[0]
	if op.Type != "json_parser" {
		t.Errorf("operator type = %q, want json_parser", op.Type)
	}
	if op.Timestamp == nil || op.Timestamp.ParseFrom != "attributes.time" || op.Timestamp.Layout != "RFC3339" {
		t.Errorf("operator timestamp = %+v, want {attributes.time RFC3339}", op.Timestamp)
	}
	if op.Severity == nil || op.Severity.ParseFrom != "attributes.level" {
		t.Errorf("operator severity = %+v, want {attributes.level}", op.Severity)
	}
}

func TestLoadK8sReceivers(t *testing.T) {
	clearEnv(t)
	path := filepath.Join("..", "..", "configs", "agent.yaml")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// DYN-goagent: reconciled to the compiled Go defaults (yaml == Default()).
	if cfg.Receiver.K8sObjects.CollectionInterval != 15*time.Minute {
		t.Errorf("k8s_objects interval = %v, want 15m", cfg.Receiver.K8sObjects.CollectionInterval)
	}
	if cfg.Receiver.K8sObjects.Mode != "pull" {
		t.Errorf("k8s_objects mode = %q, want pull", cfg.Receiver.K8sObjects.Mode)
	}
	if cfg.Receiver.K8sObjects.LabelSelector != "" {
		t.Errorf("k8s_objects label selector = %q, want empty", cfg.Receiver.K8sObjects.LabelSelector)
	}
	if cfg.Receiver.K8sObjects.FieldSelector != "" {
		t.Errorf("k8s_objects field selector = %q, want empty", cfg.Receiver.K8sObjects.FieldSelector)
	}
	wantObjects := []string{
		"pods",
		"nodes",
		"services",
		"endpoints",
		"namespaces",
		"deployments",
		"replicasets",
		"daemonsets",
		"statefulsets",
	}
	if len(cfg.Receiver.K8sObjects.Objects) != len(wantObjects) {
		t.Fatalf("k8s_objects objects = %v, want %v", cfg.Receiver.K8sObjects.Objects, wantObjects)
	}
	for i, want := range wantObjects {
		if cfg.Receiver.K8sObjects.Objects[i].Name != want {
			t.Errorf("k8s_objects objects[%d] = %q, want %q", i, cfg.Receiver.K8sObjects.Objects[i].Name, want)
		}
	}
	// DYN-goagent: reconciled to the compiled Go defaults (yaml == Default()).
	if cfg.Receiver.K8sCluster.CollectionInterval != 10*time.Minute {
		t.Errorf("k8s_cluster interval = %v, want 10m", cfg.Receiver.K8sCluster.CollectionInterval)
	}
	wantConditions := []string{"Ready", "MemoryPressure", "DiskPressure", "PIDPressure", "NetworkUnavailable"}
	if len(cfg.Receiver.K8sCluster.NodeConditionsToReport) != len(wantConditions) {
		t.Fatalf("k8s_cluster conditions = %v, want %v", cfg.Receiver.K8sCluster.NodeConditionsToReport, wantConditions)
	}
	for i, want := range wantConditions {
		if cfg.Receiver.K8sCluster.NodeConditionsToReport[i] != want {
			t.Errorf("k8s_cluster conditions[%d] = %q, want %q", i, cfg.Receiver.K8sCluster.NodeConditionsToReport[i], want)
		}
	}
}

const testFilelogYAML = `agent:
  collection_interval: 60s
  log_level: info
receiver:
  filelog:
    include:
      - /var/log/pods/*/*/*.log
      - /var/log/containers/*/*.log
      - /var/log/kubernetes/audit/*.log
    exclude:
      - /var/log/pods/*/*/**.gz
    start_at: beginning
    operators:
      - type: json_parser
        timestamp:
          parse_from: attributes.time
          layout: RFC3339
        severity:
          parse_from: attributes.level
exporter:
  otlp:
    endpoint: test-collector:4317
    insecure: false
`
func TestLoadDefaultsWhenMissingFile(t *testing.T) {
	clearEnv(t)
	cfg, err := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err != nil {
		t.Fatalf("Load missing file: %v", err)
	}
	def := Default()
	if cfg.Agent.CollectionInterval != def.Agent.CollectionInterval {
		t.Errorf("default agent interval = %v, want %v", cfg.Agent.CollectionInterval, def.Agent.CollectionInterval)
	}
	if cfg.Exporter.OTLP.Endpoint != def.Exporter.OTLP.Endpoint {
		t.Errorf("default endpoint = %q, want %q", cfg.Exporter.OTLP.Endpoint, def.Exporter.OTLP.Endpoint)
	}
}
