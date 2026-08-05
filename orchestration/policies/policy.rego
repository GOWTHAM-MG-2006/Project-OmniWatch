# OmniWatch — Orchestration + Policy
# Component: OPA Rego Policy
# Phase: 9
# Purpose: Policy rules governing auto-remediation decisions — severity×action matrix,
#          confidence gating, and human-approval requirements for sensitive actions.
#          Configurable via data.config (confidence_threshold, safe_actions) passed by
#          the decision client at query time (decisions D2, D5, D8).
# Inputs: input.severity (P1–P4), input.confidence (0..100), input.action_type (str),
#         data.config.confidence_threshold (float), data.config.safe_actions (list[str])
# Outputs: {"allow": bool, "needs_approval": bool, "reason": str}
#          OPA query path: POST /v1/data/omniwatch/allow

package omniwatch

default allow := false
default needs_approval := false
default reason := ""

# ── Sensitive actions (D4): block_ip, rotate_credentials → ALWAYS human approval ──
needs_approval if {
	input.action_type in {"block_ip", "rotate_credentials"}
}

# ── Auto-remediation (D2/D5): safe actions on P1/P2 with confidence above threshold ──
allow if {
	not input.action_type in {"block_ip", "rotate_credentials"}
	input.severity in {"P1", "P2"}
	input.confidence > data.config.confidence_threshold
	input.action_type in data.config.safe_actions
}

# ── Safe action but not auto-eligible: P3/P4 or below confidence → approval needed ──
needs_approval if {
	not allow
	input.action_type in data.config.safe_actions
}

# ── Reason strings (human-readable decision explanation) ──
reason := "auto-remediation allowed for P1/P2 incident with confidence above threshold" if {
	allow
}

reason := "human approval required: sensitive action" if {
	needs_approval
	input.action_type in {"block_ip", "rotate_credentials"}
}

reason := "human approval required: incident below auto-remediation threshold" if {
	needs_approval
	not allow
	input.action_type in data.config.safe_actions
}

reason := "action denied" if {
	not allow
	not needs_approval
}
