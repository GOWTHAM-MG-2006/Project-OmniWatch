// OmniWatch — Agent
// Component: config (tests: resilience section)
// Phase: industry-ready (IND-2)
// Purpose: Defaults, YAML loading, and OMNIWATCH_RESILIENCE_* overrides
// Inputs: Default(), temp YAML, env vars
// Outputs: go test results
package config

import (
	"testing"
	"time"
)

func TestResilienceDefaults(t *testing.T) {
	clearEnv(t)
	cfg := Default()
	rc := cfg.Resilience
	if rc.QueueSize != 1000 {
		t.Errorf("QueueSize = %d, want 1000", rc.QueueSize)
	}
	if rc.CircuitBreakerThreshold != 5 {
		t.Errorf("CircuitBreakerThreshold = %d, want 5", rc.CircuitBreakerThreshold)
	}
	if rc.CircuitBreakerTimeout != 30*time.Second {
		t.Errorf("CircuitBreakerTimeout = %v, want 30s", rc.CircuitBreakerTimeout)
	}
	if rc.RetryMaxAttempts != 3 {
		t.Errorf("RetryMaxAttempts = %d, want 3", rc.RetryMaxAttempts)
	}
	want := []time.Duration{100 * time.Millisecond, 500 * time.Millisecond, 2 * time.Second}
	if len(rc.RetryBackoffs) != len(want) {
		t.Fatalf("RetryBackoffs = %v, want %v", rc.RetryBackoffs, want)
	}
	for i := range want {
		if rc.RetryBackoffs[i] != want[i] {
			t.Errorf("RetryBackoffs[%d] = %v, want %v", i, rc.RetryBackoffs[i], want[i])
		}
	}
}

func TestResilienceEnvOverrides(t *testing.T) {
	clearEnv(t)
	t.Setenv("OMNIWATCH_RESILIENCE_QUEUE_SIZE", "10")
	t.Setenv("OMNIWATCH_RESILIENCE_CIRCUIT_BREAKER_THRESHOLD", "2")
	t.Setenv("OMNIWATCH_RESILIENCE_CIRCUIT_BREAKER_TIMEOUT", "5s")
	t.Setenv("OMNIWATCH_RESILIENCE_RETRY_MAX_ATTEMPTS", "2")
	t.Setenv("OMNIWATCH_RESILIENCE_RETRY_BACKOFFS", "50ms,250ms")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	rc := cfg.Resilience
	if rc.QueueSize != 10 {
		t.Errorf("QueueSize = %d, want 10", rc.QueueSize)
	}
	if rc.CircuitBreakerThreshold != 2 {
		t.Errorf("CircuitBreakerThreshold = %d, want 2", rc.CircuitBreakerThreshold)
	}
	if rc.CircuitBreakerTimeout != 5*time.Second {
		t.Errorf("CircuitBreakerTimeout = %v, want 5s", rc.CircuitBreakerTimeout)
	}
	if rc.RetryMaxAttempts != 2 {
		t.Errorf("RetryMaxAttempts = %d, want 2", rc.RetryMaxAttempts)
	}
	if len(rc.RetryBackoffs) != 2 || rc.RetryBackoffs[0] != 50*time.Millisecond || rc.RetryBackoffs[1] != 250*time.Millisecond {
		t.Errorf("RetryBackoffs = %v, want [50ms 250ms]", rc.RetryBackoffs)
	}
}

func TestResilienceYAMLSection(t *testing.T) {
	clearEnv(t)
	path := writeTempYAML(t, testYAML+`resilience:
  circuit_breaker_threshold: 7
  circuit_breaker_timeout: 45s
  queue_size: 42
  retry_max_attempts: 4
  retry_backoffs:
    - 100ms
    - 500ms
    - 2s
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	rc := cfg.Resilience
	if rc.QueueSize != 42 || rc.CircuitBreakerThreshold != 7 || rc.CircuitBreakerTimeout != 45*time.Second || rc.RetryMaxAttempts != 4 {
		t.Errorf("resilience YAML not applied: %+v", rc)
	}
	if len(rc.RetryBackoffs) != 3 || rc.RetryBackoffs[2] != 2*time.Second {
		t.Errorf("RetryBackoffs = %v, want [100ms 500ms 2s]", rc.RetryBackoffs)
	}
}

func TestResilienceAgentYAMLHasSection(t *testing.T) {
	clearEnv(t)
	cfg, err := Load("../../configs/agent.yaml")
	if err != nil {
		t.Fatalf("Load agent.yaml: %v", err)
	}
	rc := cfg.Resilience
	if rc.QueueSize != 1000 || rc.CircuitBreakerThreshold != 5 || rc.CircuitBreakerTimeout != 30*time.Second || rc.RetryMaxAttempts != 3 {
		t.Errorf("agent.yaml resilience section wrong: %+v", rc)
	}
}
