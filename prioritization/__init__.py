"""
OmniWatch — Incident Prioritization Engine
Component: prioritization package
Phase: 8
Purpose: Incident severity classification, business impact scoring, alert deduplication (GAP3), and orchestration routing.
Inputs: RootCauseObject dicts from Kafka omniwatch.incidents.causal (Phase 7 output)
Outputs: IncidentRecord dicts to Kafka omniwatch.incidents.created (Phase 9 input)
"""
