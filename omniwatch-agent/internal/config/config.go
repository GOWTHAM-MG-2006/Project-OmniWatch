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
			Include []string `yaml:"include"`
		} `yaml:"filelog"`
		K8sObjects struct {
			CollectionInterval time.Duration `yaml:"collection_interval"`
			Mode               string        `yaml:"mode"` // watch or pull
			LabelSelector      string        `yaml:"label_selector"`
			FieldSelector      string        `yaml:"field_selector"`
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
}

// Default returns a Config populated with default values.
func Default() *Config {
	c := &Config{}
	c.Agent.CollectionInterval = 60 * time.Second
	c.Agent.LogLevel = "info"
	c.Receiver.Hostmetrics.CollectionInterval = 60 * time.Second
	c.Receiver.Filelog.Include = []string{"/var/log/*.log"}
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
}

func lookupEnv(key string) (string, bool) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return "", false
	}
	return v, true
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
