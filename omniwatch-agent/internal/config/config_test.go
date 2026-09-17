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
