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

// Cached DB-console auth flags. Set ONLY after a verified 200 (see
// AuthGate); cleared here on ANY 401 so a bad/expired credential can never
// leave a page showing a false "authenticated" state.
const AUTH_FLAG_BY_PREFIX: Array<{ prefix: string; flag: string; service: string }> = [
  { prefix: '/clickhouse/', flag: 'clickhouse_authenticated', service: 'clickhouse' },
  { prefix: '/neo4j/', flag: 'neo4j_authenticated', service: 'neo4j' },
  { prefix: '/minio/', flag: 'minio_authenticated', service: 'minio' },
]

export function clearDbAuthFlags(): void {
  for (const { flag } of AUTH_FLAG_BY_PREFIX) sessionStorage.removeItem(flag)
  window.dispatchEvent(new Event('omniwatch:auth-invalid'))
}

function clearDbAuthFlagForUrl(url: string | undefined): void {
  const match = AUTH_FLAG_BY_PREFIX.find(({ prefix }) => url?.includes(prefix))
  if (match) {
    sessionStorage.removeItem(match.flag)
    window.dispatchEvent(new CustomEvent('omniwatch:auth-invalid', { detail: { service: match.service } }))
  } else {
    clearDbAuthFlags()
  }
}

api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error?.response?.status === 401) clearDbAuthFlagForUrl(error?.config?.url)
    return Promise.reject(error)
  },
)

// Surface the backend's explicit error body (e.g. "ClickHouse
// authentication failed") instead of axios's generic status text.
export function apiError(e: unknown, fallback: string): string {
  const err = e as { response?: { data?: { error?: string } }; message?: string }
  return err?.response?.data?.error || err?.message || fallback
}

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
  const { data } = await api.get<{
    distribution: Array<SeverityDistribution & { s?: string; c?: number }>;
    timestamp: string
  }>('/dashboard/severity-distribution', { params })
  // Backend may return abbreviated keys ({s, c}) instead of ({severity, cnt})
  // depending on the running image — normalize both shapes here.
  const distribution = (data.distribution ?? []).map((d) => ({
    severity: d.severity ?? d.s ?? 'unknown',
    cnt: Number(d.cnt ?? d.c ?? 0),
  }))
  return { distribution, timestamp: data.timestamp }
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
  const { data } = await api.get<{
    timeline: Array<TimelinePoint & { h?: string; i?: number; s?: string }>;
    count: number;
    timestamp: string
  }>('/dashboard/incidents-timeline', { params: p })
  // Normalize abbreviated keys ({h, i, s}) to ({hour, incident_count, severity}).
  const timeline = (data.timeline ?? []).map((t) => ({
    hour: String(t.hour ?? t.h ?? ''),
    incident_count: Number(t.incident_count ?? t.i ?? 0),
    severity: t.severity ?? t.s ?? 'unknown',
  }))
  return { timeline, count: timeline.length, timestamp: data.timestamp }
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
  const { data } = await api.get<MinioBucketsResponse>('/minio/buckets', { headers: minioHeaders() })
  return data
}

export async function fetchMinioObjects(params: {
  bucket: string
  prefix?: string
  limit?: number
  offset?: number
}): Promise<MinioObjectsResponse> {
  const { data } = await api.get<MinioObjectsResponse>('/minio/objects', { params, headers: minioHeaders(), timeout: 8000 })
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

// ── ClickHouse console ─────────────────────────────────────────────

export interface ClickHouseQueryResult {
  columns: string[]
  rows: unknown[][]
  row_count: number
  truncated: boolean
  timestamp: string
  error?: string
}

export interface ClickHouseTableInfo {
  name: string
  database?: string
  engine?: string
  row_count?: number
  total_bytes?: number
}

export interface ClickHouseSchemaColumn {
  name: string
  type: string
  default_kind: string
  default_expression: string
  comment: string
}

function chHeaders(): Record<string, string> {
  const user = sessionStorage.getItem('ch_user') || ''
  const pass = sessionStorage.getItem('ch_password') || ''
  return { 'X-ClickHouse-User': user, 'X-ClickHouse-Password': pass }
}

export async function clickhouseQuery(query: string, limit = 100): Promise<ClickHouseQueryResult> {
  const { data } = await api.post<ClickHouseQueryResult>('/clickhouse/query', { query, limit }, { headers: chHeaders(), timeout: 30_000 })
  return data
}

export async function clickhouseTables(): Promise<{ tables: ClickHouseTableInfo[]; count: number }> {
  const { data } = await api.get('/clickhouse/tables', { headers: chHeaders(), timeout: 10_000 })
  // Live backend abbreviates table keys as {d,n,e} (contract drift, same as
  // timeline {h,i,s}); normalize both shapes so the table list renders names.
  const rawTables = (data?.tables ?? []) as Array<ClickHouseTableInfo & { d?: string; n?: string; e?: string }>
  const tables: ClickHouseTableInfo[] = rawTables.map((t) => ({
    name: t.name ?? t.n ?? '',
    database: t.database ?? t.d ?? '',
    engine: t.engine ?? t.e ?? '',
    row_count: t.row_count,
    total_bytes: t.total_bytes,
  }))
  return { tables, count: data?.count ?? tables.length }
}

export async function clickhouseSchema(tableName: string): Promise<{ table: string; columns: ClickHouseSchemaColumn[] }> {
  const { data } = await api.get(`/clickhouse/schema/${encodeURIComponent(tableName)}`, { headers: chHeaders(), timeout: 10_000 })
  // Live backend abbreviates column keys as {n,t,d,c}; normalize both shapes.
  const rawCols = (data?.columns ?? []) as Array<ClickHouseSchemaColumn & { n?: string; t?: string; d?: string; c?: string }>
  const columns: ClickHouseSchemaColumn[] = rawCols.map((c) => ({
    name: c.name ?? c.n ?? '',
    type: c.type ?? c.t ?? '',
    default_kind: c.default_kind ?? '',
    default_expression: c.default_expression ?? c.d ?? '',
    comment: c.comment ?? c.c ?? '',
  }))
  return { table: data?.table ?? tableName, columns }
}

// ── Neo4j console ──────────────────────────────────────────────────

export interface Neo4jQueryResult {
  columns: string[]
  rows: Record<string, unknown>[]
  row_count: number
  truncated: boolean
  timestamp: string
  error?: string
}

export interface Neo4jSchemaInfo {
  labels: string[]
  relationshipTypes: string[]
  propertyKeys: string[]
}

function neo4jHeaders(): Record<string, string> {
  const user = sessionStorage.getItem('neo4j_user') || ''
  const pass = sessionStorage.getItem('neo4j_password') || ''
  return { 'X-Neo4j-User': user, 'X-Neo4j-Password': pass }
}

export async function neo4jQuery(query: string, limit = 100): Promise<Neo4jQueryResult> {
  const { data } = await api.post<Neo4jQueryResult>('/neo4j/query', { query, limit }, { headers: neo4jHeaders(), timeout: 30_000 })
  return data
}

export async function neo4jSchema(): Promise<Neo4jSchemaInfo> {
  const { data } = await api.get('/neo4j/schema', { headers: neo4jHeaders(), timeout: 10_000 })
  return data
}

// ── MinIO console (auth via headers) ───────────────────────────────

function minioHeaders(): Record<string, string> {
  const ak = sessionStorage.getItem('minio_access_key') || ''
  const sk = sessionStorage.getItem('minio_secret_key') || ''
  return { 'X-MinIO-AccessKey': ak, 'X-MinIO-SecretKey': sk }
}

export async function minioUpload(bucket: string, key: string, file: File): Promise<{ message: string; bucket: string; key: string }> {
  const form = new FormData()
  form.append('bucket', bucket)
  form.append('key', key)
  form.append('file', file)
  const { data } = await api.post('/minio/upload', form, { headers: { ...minioHeaders(), 'Content-Type': 'multipart/form-data' }, timeout: 60_000 })
  return data
}

export async function minioDownload(bucket: string, key: string): Promise<Blob> {
  const { data } = await api.get(`/minio/download/${encodeURIComponent(bucket)}/${key}`, { headers: minioHeaders(), responseType: 'blob', timeout: 60_000 })
  return data
}

export async function minioDeleteObject(bucket: string, key: string): Promise<{ message: string }> {
  const { data } = await api.delete('/minio/object', { headers: minioHeaders(), data: { bucket, key } })
  return data
}

export async function minioDeleteBucket(name: string): Promise<{ message: string }> {
  const { data } = await api.delete(`/minio/bucket/${encodeURIComponent(name)}`, { headers: minioHeaders() })
  return data
}

export async function minioCreateBucket(name: string): Promise<{ message: string; bucket: string }> {
  const { data } = await api.post('/minio/buckets', { name }, { headers: minioHeaders() })
  return data
}

export async function minioMetadata(bucket: string, key: string): Promise<{
  bucket: string; key: string; size: number; content_type: string; last_modified: string; etag: string
}> {
  const { data } = await api.get(`/minio/metadata/${encodeURIComponent(bucket)}/${key}`, { headers: minioHeaders() })
  return data
}

export default api
