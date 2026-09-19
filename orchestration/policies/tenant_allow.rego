# OmniWatch — Orchestration + Policy
# Component: OPA tenant isolation rule (ENTRY-2 addition)
# Phase: entry-point (Wave 2, todo 2)
# Purpose: tenant_allow gate — JWT sub/ws vs resource workspace, evaluated
#          BEFORE the existing allow/needs_approval rules in policy.rego.
#          policy.rego bytes are preserved verbatim; this file is purely
#          additive. Decision clients query /v1/data/omniwatch/tenant_allow
#          first and short-circuit deny when it is false.
# Inputs: input.jwt_sub (JWT `sub`), input.jwt_ws (JWT `ws`, absent means
#         the legacy/default workspace), input.resource_ws (workspace slug
#         owning the resource), input.resource_owner (workspace `user_id`)
# Outputs: {"tenant_allow": bool}
#          OPA query path: POST /v1/data/omniwatch/tenant_allow

package omniwatch

import rego.v1

# ── tenant_allow (ENTRY-2): workspace isolation gate ──────────────────────
# Checked before the existing allow/needs_approval rules. Deny (false)
# short-circuits: the request never reaches the remediation policy.

default tenant_allow := false

# Legacy/unscoped resources read as the default workspace; a default-scoped
# caller (or a legacy token with no ws claim) is allowed through.
tenant_allow if {
	input.resource_ws == "default"
	object.get(input, "jwt_ws", "default") == "default"
}

# Exact workspace match owned by the caller.
tenant_allow if {
	input.resource_ws == input.jwt_ws
	input.jwt_sub == input.resource_owner
}
