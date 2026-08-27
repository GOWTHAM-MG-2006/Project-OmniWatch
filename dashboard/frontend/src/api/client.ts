/**
 * OmniWatch — Dashboard Frontend
 * Component: API Client
 * Phase: 11
 * Purpose: Typed axios wrapper for dashboard-api (port 8011)
 * Inputs: Dashboard API REST endpoints
 * Outputs: Typed response objects for React components
 */

import axios from 'axios'

const api = axios.create({
  baseURL: '/api',
  timeout: 15_000,
})

// ── Response types ──────────────────────────────────────────────────

export interface SummaryResponse {
  total_incidents: number
  active_anomalies: number
  knowledge_base_entries: number
  timestamp: string
}

export interface SeverityDistribution {
  severity: string
  cnt: number
}

export interface SeverityDistributionResponse {
  distribution: SeverityDistribution[]
  timestamp: string
}

export interface TimelinePoint {
  hour: string
  incident_count: number
  severity: string
}

export interface TimelineResponse {
  timeline: TimelinePoint[]
  count: number
  timestamp: string
}

export interface Incident {
  incident_id: string
  created_at: string
  severity: string
  business_impact_score: number
  root_cause_entity: string
  entity_type: string
  fault_path: string
  impacted_services: string
  deduplicated_count: number
  sla_breach_risk: string
  assigned_to: string
  status: string
  related_anomalies: string
}

export interface IncidentsResponse {
  incidents: Incident[]
  count: number
  timestamp: string
}

export interface TopologyNode {
  id: string
  data: {
    label: string
    entity_type: string
    criticality: string
    status: string
    anomaly_score: number
  }
  position: { x: number; y: number }
  type: string
}

export interface TopologyEdge {
  source: string
  target: string
  label: string
  data: { latency_p50: number; error_rate: number }
}

export interface TopologyResponse {
  nodes: TopologyNode[]
  edges: TopologyEdge[]
  node_count: number
  edge_count: number
}

export interface EntityHealth {
  id: string
  name: string
  anomaly_score: number
  status: string
}

export interface EntityHealthResponse {
  entities: EntityHealth[]
  count: number
  timestamp: string
}

export interface KnowledgeEntry {
  root_cause_entity: string
  root_cause_type: string
  resolution: string
  resolution_steps: string
  avg_resolution_minutes: number
  success_count: number
  failure_count: number
}

export interface KnowledgeResponse {
  entries: KnowledgeEntry[]
  count: number
  timestamp: string
}

export interface CopilotResponse {
  answer: string
  sources: string[]
  timestamp: string
}

// ── API calls ───────────────────────────────────────────────────────

export async function fetchSummary(): Promise<SummaryResponse> {
  const { data } = await api.get<SummaryResponse>('/summary')
  return data
}

export async function fetchSeverityDistribution(): Promise<SeverityDistributionResponse> {
  const { data } = await api.get<SeverityDistributionResponse>('/dashboard/severity-distribution')
  return data
}

export async function fetchIncidentsTimeline(hours = 24): Promise<TimelineResponse> {
  const { data } = await api.get<TimelineResponse>('/dashboard/incidents-timeline', { params: { hours } })
  return data
}

export async function fetchIncidents(params?: {
  severity?: string
  status?: string
  limit?: number
}): Promise<IncidentsResponse> {
  const { data } = await api.get<IncidentsResponse>('/incidents', { params })
  return data
}

export async function fetchTopology(): Promise<TopologyResponse> {
  const { data } = await api.get<TopologyResponse>('/topology')
  return data
}

export async function fetchEntityHealth(): Promise<EntityHealthResponse> {
  const { data } = await api.get<EntityHealthResponse>('/dashboard/entity-health')
  return data
}

export async function fetchKnowledgeBase(): Promise<KnowledgeResponse> {
  const { data } = await api.get<KnowledgeResponse>('/knowledge-base')
  return data
}

export async function fetchCopilot(query: string): Promise<CopilotResponse> {
  const { data } = await api.get<CopilotResponse>('/copilot', { params: { query } })
  return data
}

export default api
