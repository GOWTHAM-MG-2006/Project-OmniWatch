# OmniWatch — Orchestration + Policy
# Component: OPA Policy Unit Tests
# Phase: 9
# Purpose: Unit tests for the OPA Rego policy governing auto-remediation decisions.
#          Validates severity×action matrix, confidence gating, and human-approval rules.
# Inputs: Mock input fixtures (severity, confidence, action_type) and data.config overrides
# Outputs: Pass/fail per rule — run via `opa test /policies/ -v`

package omniwatch_test

# Shared test config — matches decision D5 safe_actions and D2 threshold
config := {
	"confidence_threshold": 95,
	"safe_actions": ["restart_service", "scale_deployment", "clear_cache", "kill_pod", "rollback"],
}

# Test 1: P1 + high confidence + safe action → auto-allow
test_allow_p1_high_confidence_safe_action if {
	data.omniwatch.allow with input as {"severity": "P1", "confidence": 97, "action_type": "restart_service"} with data.config as config
	not data.omniwatch.needs_approval with input as {"severity": "P1", "confidence": 97, "action_type": "restart_service"} with data.config as config
}

# Test 2: P2 + high confidence + safe action → auto-allow
test_allow_p2_high_confidence_safe_action if {
	data.omniwatch.allow with input as {"severity": "P2", "confidence": 96, "action_type": "scale_deployment"} with data.config as config
	not data.omniwatch.needs_approval with input as {"severity": "P2", "confidence": 96, "action_type": "scale_deployment"} with data.config as config
}

# Test 3: block_ip → always needs approval, never auto-allowed
test_block_ip_always_approval if {
	not data.omniwatch.allow with input as {"severity": "P1", "confidence": 99, "action_type": "block_ip"} with data.config as config
	data.omniwatch.needs_approval with input as {"severity": "P1", "confidence": 99, "action_type": "block_ip"} with data.config as config
}

# Test 4: rotate_credentials → always needs approval, never auto-allowed
test_rotate_credentials_always_approval if {
	not data.omniwatch.allow with input as {"severity": "P2", "confidence": 98, "action_type": "rotate_credentials"} with data.config as config
	data.omniwatch.needs_approval with input as {"severity": "P2", "confidence": 98, "action_type": "rotate_credentials"} with data.config as config
}

# Test 5: Low confidence (below threshold) → needs approval even for safe action on P1
test_low_confidence_needs_approval if {
	not data.omniwatch.allow with input as {"severity": "P1", "confidence": 90, "action_type": "restart_service"} with data.config as config
	data.omniwatch.needs_approval with input as {"severity": "P1", "confidence": 90, "action_type": "restart_service"} with data.config as config
}

# Test 6: P3 incident → needs approval even for safe action with high confidence
test_p3_incident_needs_approval if {
	not data.omniwatch.allow with input as {"severity": "P3", "confidence": 97, "action_type": "restart_service"} with data.config as config
	data.omniwatch.needs_approval with input as {"severity": "P3", "confidence": 97, "action_type": "restart_service"} with data.config as config
}

# Test 7: Unsafe action (not in safe_actions, not block_ip/rotate_credentials) → denied
test_unsafe_action_denied if {
	not data.omniwatch.allow with input as {"severity": "P1", "confidence": 97, "action_type": "delete_everything"} with data.config as config
	not data.omniwatch.needs_approval with input as {"severity": "P1", "confidence": 97, "action_type": "delete_everything"} with data.config as config
}
