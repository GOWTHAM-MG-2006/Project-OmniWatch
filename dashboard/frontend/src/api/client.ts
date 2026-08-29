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
  // Backend may return `items` (paginated) or `total_count`; accept both for resilience
  items?: Incident[]
  count: number
  total_count?: number
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

// ── API calls ───────────────────────────────────────────────────────

export async function fetchSummary(params?: { timeRange?: string; hours?: number }): Promise<SummaryResponse> {
  const { data } = await api.get<SummaryResponse>('/summary', { params })
  return data
}

export async function fetchSeverityDistribution(params?: { timeRange?: string; hours?: number }): Promise<SeverityDistributionResponse> {
  const { data } = await api.get<SeverityDistributionResponse>('/dashboard/severity-distribution', { params })
  return data
}

export async function fetchIncidentsTimeline(params?: { timeRange?: string; hours?: number }): Promise<TimelineResponse> {
  // keep backward-compat: if caller passes a plain number, treat as hours
  const p: Record<string, unknown> = {}
  if (typeof params === 'number') {
    p.hours = params
  } else if (params) {
    Object.assign(p, params)
    // send both for compatibility when using timeRange alias
    if (params.timeRange && !params.hours) {
      const map: Record<string, number> = { '1h': 1, '6h': 6, '24h': 24, '7d': 168 }
      const h = map[params.timeRange] ?? 24
      p.hours = h
    }
  } else {
    p.hours = 24
  }
  const { data } = await api.get<TimelineResponse>('/dashboard/incidents-timeline', { params: p })
  return data
}

export async function fetchIncidents(params?: {
  severity?: string
  status?: string
  limit?: number
  timeRange?: string
  hours?: number
}): Promise<IncidentsResponse> {
  const p: Record<string, unknown> = { ...(params as Record<string, unknown>) }
  if (p.timeRange && !p.hours) {
    const map: Record<string, number> = { '1h': 1, '6h': 6, '24h': 24, '7d': 168 }
    p.hours = map[p.timeRange as string] ?? 24
  }
  const { data } = await api.get<IncidentsResponse>('/incidents', { params: p })
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

export async function fetchKnowledgeBase(params?: { timeRange?: string; hours?: number }): Promise<KnowledgeResponse> {
  const p: Record<string, unknown> = { ...(params as Record<string, unknown> | undefined) }
  if (p.timeRange && !p.hours) {
    const map: Record<string, number> = { '1h': 1, '6h': 6, '24h': 24, '7d': 168 }
    p.hours = map[p.timeRange as string] ?? 24
  }
  const { data } = await api.get<KnowledgeResponse>('/knowledge-base', { params: p })
  return data
}

export interface MinioBucket {
  name: string
  creation_date: string | null
}

export interface MinioBucketsResponse {
  buckets: MinioBucket[]
  count: number
  timestamp: string
  error?: string
}

export interface MinioObject {
  name: string
  size: number
  last_modified: string | null
  etag: string | null
}

export interface MinioObjectsResponse {
  bucket: string
  prefix: string
  objects: MinioObject[]
  count: number
  total: number
  limit: number
  offset: number
  timestamp: string
  error?: string
  has_more?: boolean
  truncated?: boolean
}

export async function fetchMinioBuckets(): Promise<MinioBucketsResponse> {
  const { data } = await api.get<MinioBucketsResponse>('/minio/buckets')
  return data
}

export async function fetchMinioObjects(params: {
  bucket: string
  prefix?: string
  limit?: number
  offset?: number
}): Promise<MinioObjectsResponse> {
  const { data } = await api.get<MinioObjectsResponse>('/minio/objects', { params, timeout: 8000 })
  return data
}

export interface SecurityGeoBucket {
  ip: string
  count: number
  cnt: number
  attack_type: string
  severity: string
  last_seen: string
}

export interface SecurityGeoResponse {
  buckets: SecurityGeoBucket[]
  count: number
  timestamp: string
  note?: string
}

export async function fetchSecurityGeo(params?: { limit?: number; hours?: number; timeRange?: string }): Promise<SecurityGeoResponse> {
  const p: Record<string, unknown> = { ...(params as Record<string, unknown> | undefined) }
  if (p.timeRange && !p.hours) {
    const map: Record<string, number> = { '1h': 1, '6h': 6, '24h': 24, '7d': 168 }
    p.hours = map[p.timeRange as string] ?? 24
  }
  const { data } = await api.get<SecurityGeoResponse>('/security/geo', { params: p })
  return data
}

export interface SecurityAnomaliesResponse2 {
  anomalies: Array<{
    anomaly_id: string
    entity_id: string
    entity_type: string
    metric_name: string
    anomaly_score: number
    confidence: number
    timestamp: string
    deviation_from_baseline: number
    source_type: string
    status: string
    attack_type: string
    severity: string
    source_ip: string | null
    evidence_logs: string
    recommended_action: string | null
  }>
  count: number
  timestamp: string
}

export async function fetchSecurityAnomalies(params?: { limit?: number; hours?: number; timeRange?: string }): Promise<SecurityAnomaliesResponse2> {
  const p: Record<string, unknown> = { ...(params as Record<string, unknown> | undefined) }
  if (p.timeRange && !p.hours) {
    const map: Record<string, number> = { '1h': 1, '6h': 6, '24h': 24, '7d': 168 }
    p.hours = map[p.timeRange as string] ?? 24
  }
  const { data } = await api.get<SecurityAnomaliesResponse2>('/security/anomalies', { params: p })
  return data
}

export async function fetchActions(params?: { limit?: number; status?: string }) {
  const { data } = await api.get('/actions', { params })
  return data
}

export async function fetchRemediationHistory(params?: { limit?: number }) {
  const { data } = await api.get('/remediation/history', { params })
  return data
}

export async function fetchLearningStats() {
  const { data } = await api.get('/learning/stats')
  return data
}

export default api
