/**
 * OmniWatch — Dashboard Frontend
 * Component: Security Page
 * Phase: 11
 * Purpose: Security events overview — anomalies with source_type=security — Stitch design polished
 * Inputs: Dashboard API — /api/anomalies?source_type=security, /api/security/geo (ip aggregation)
 * Outputs: Security event list with attack type badges, ip/geo viz (live, zero dummy)
 */

import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip, BarChart, Bar, XAxis, YAxis, CartesianGrid } from 'recharts'
import { useFetch } from '../hooks/useFetch'
import { useTimeRange } from '../hooks/useTimeRange'
import api from '../api/client'

interface SecurityAnomaly {
  entity_id: string
  entity_type: string
  metric_name: string
  anomaly_score: number
  confidence: number
  timestamp: string
  deviation_from_baseline: number
  source_type: string
  status: string
  attack_type?: string
  severity?: string
  source_ip?: string | null
  evidence_logs?: string
}

interface SecurityAnomaliesResponse {
  anomalies: SecurityAnomaly[]
  count: number
  timestamp: string
}

interface GeoBucket {
  ip: string
  count: number
  cnt: number
  attack_type: string
  severity: string
  last_seen: string
}

interface GeoResponse {
  buckets: GeoBucket[]
  count: number
  timestamp: string
}

const ATTACK_COLORS: Record<string, string> = {
  BRUTE_FORCE_ATTEMPT: '#ef4444',
  PRIVILEGE_ESCALATION_ATTEMPT: '#f59e0b',
  UNAUTHORIZED_CONFIG_CHANGE: '#00d4ff',
  POTENTIAL_DATA_EXFILTRATION: '#7c3aed',
  UNKNOWN: '#6b7280',
}

const ATTACK_LABELS: Record<string, string> = {
  BRUTE_FORCE_ATTEMPT: 'Brute Force',
  PRIVILEGE_ESCALATION_ATTEMPT: 'Privilege Escalation',
  UNAUTHORIZED_CONFIG_CHANGE: 'Config Change',
  POTENTIAL_DATA_EXFILTRATION: 'Data Exfiltration',
  UNKNOWN: 'Unknown',
}

function AttackDistribution({ data }: { data: SecurityAnomaly[] }) {
  const counts = new Map<string, number>()
  for (const a of data) {
    const key = (a.attack_type || a.metric_name || 'UNKNOWN').trim() || 'UNKNOWN'
    counts.set(key, (counts.get(key) ?? 0) + 1)
  }
  const chartData = Array.from(counts.entries()).map(([name, value]) => ({ name, value }))

  if (!chartData.length) {
    return (
      <div className="h-full flex items-center justify-center text-[#a1a1aa] text-sm font-mono">
        No attacks detected
      </div>
    )
  }

  return (
    <div className="h-full flex items-center gap-4">
      <ResponsiveContainer width="60%" height="100%">
        <PieChart>
          <Pie
            data={chartData}
            dataKey="value"
            nameKey="name"
            cx="50%"
            cy="50%"
            innerRadius="45%"
            outerRadius="75%"
            strokeWidth={0}
          >
            {chartData.map((entry) => (
              <Cell key={entry.name} fill={ATTACK_COLORS[entry.name] ?? ATTACK_COLORS.UNKNOWN} />
            ))}
          </Pie>
          <Tooltip
            contentStyle={{
              background: '#1a1a1a',
              border: '1px solid #2a2a2a',
              borderRadius: 8,
              fontSize: 12,
              fontFamily: "'JetBrains Mono', monospace",
            }}
            labelStyle={{ color: '#a1a1aa' }}
          />
        </PieChart>
      </ResponsiveContainer>
      <div className="flex flex-col gap-2 text-xs">
        {chartData.map((d) => (
          <div key={d.name} className="flex items-center gap-2">
            <span
              className="w-2.5 h-2.5 rounded-full"
              style={{
                background: ATTACK_COLORS[d.name] ?? ATTACK_COLORS.UNKNOWN,
                boxShadow: `0 0 6px ${ATTACK_COLORS[d.name] ?? ATTACK_COLORS.UNKNOWN}44`,
              }}
            />
            <span className="text-[#a1a1aa] truncate max-w-[140px]">{ATTACK_LABELS[d.name] ?? d.name}</span>
            <span className="text-[#e4e4e7] font-mono font-bold">{d.value}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

function AttackTypeBadge({ metricName }: { metricName: string }) {
  const color = ATTACK_COLORS[metricName] ?? ATTACK_COLORS.UNKNOWN
  const label = ATTACK_LABELS[metricName] ?? metricName
  return (
    <span
      className="px-2 py-0.5 rounded text-[10px] font-mono font-bold"
      style={{
        background: `${color}22`,
        color: color,
        boxShadow: `0 0 8px ${color}22`,
      }}
    >
      {label}
    </span>
  )
}

function IpGeoViz({ timeRange, hours }: { timeRange: string; hours: number }) {
  const { data, loading, error } = useFetch<GeoResponse>(
    async () => {
      const { data } = await api.get<GeoResponse>('/security/geo', { params: { limit: 20, timeRange, hours } })
      return data
    },
    [timeRange],
  )

  const buckets = data?.buckets ?? []

  if (loading) {
    return <div className="p-8 text-center text-[#a1a1aa] animate-pulse font-mono text-xs">Loading IP distribution…</div>
  }
  if (error) {
    return <div className="p-6 text-center text-[#ef4444] font-mono text-xs">Failed to load IP distribution: {String(error)}</div>
  }
  if (buckets.length === 0) {
    return (
      <div className="p-8 text-center font-mono text-xs">
        <div className="text-[#a1a1aa]">No source IP data yet</div>
        <div className="text-[#71717a] mt-1">Security anomalies with source_ip will appear here. Generate with <span className="text-[#e4e4e7]">--scenario security_attack</span></div>
        <div className="mt-4 mx-auto max-w-md h-16 rounded border border-dashed border-[#2a2a2a] flex items-center justify-center text-[#52525b] text-[10px] uppercase tracking-widest">
          World map — awaiting geo-enriched source_ip data
        </div>
      </div>
    )
  }

  const chartData = buckets.slice(0, 12).map((b) => ({ ip: b.ip, count: b.count }))
  const maxCount = Math.max(...chartData.map((d) => d.count), 1)

  return (
    <div className="flex flex-col gap-4">
      <div className="h-56">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData} layout="vertical" margin={{ left: 8, right: 16, top: 4, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2a2a2a" horizontal={false} />
            <XAxis type="number" stroke="#71717a" tick={{ fontSize: 10, fontFamily: 'JetBrains Mono' }} allowDecimals={false} domain={[0, maxCount]} />
            <YAxis type="category" dataKey="ip" stroke="#a1a1aa" tick={{ fontSize: 10, fontFamily: 'JetBrains Mono' }} width={140} />
            <Tooltip
              contentStyle={{ background: '#1a1a1a', border: '1px solid #2a2a2a', borderRadius: 8, fontSize: 12, fontFamily: "'JetBrains Mono', monospace" }}
              formatter={(value: number) => [value, 'events']}
            />
            <Bar dataKey="count" fill="#00d4ff" radius={[0, 6, 6, 0]} barSize={14} />
          </BarChart>
        </ResponsiveContainer>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-[#2a2a2a] text-[#a1a1aa] uppercase tracking-widest">
              <th className="text-left p-2 font-mono font-medium">Source IP</th>
              <th className="text-left p-2 font-mono font-medium">Events</th>
              <th className="text-left p-2 font-mono font-medium">Attack</th>
              <th className="text-left p-2 font-mono font-medium">Severity</th>
              <th className="text-left p-2 font-mono font-medium">Last Seen</th>
            </tr>
          </thead>
          <tbody>
            {buckets.map((b) => (
              <tr key={b.ip} className="border-b border-[#2a2a2a]/60 hover:bg-[#141618] transition-colors">
                <td className="p-2 font-mono text-[#00d4ff]">{b.ip}</td>
                <td className="p-2 font-mono text-[#e4e4e7] font-bold">{b.count}</td>
                <td className="p-2"><span className="text-[#a1a1aa] font-mono text-[11px]">{b.attack_type || '—'}</span></td>
                <td className="p-2 font-mono text-[11px]" style={{ color: b.severity === 'CRITICAL' ? '#ef4444' : b.severity === 'HIGH' ? '#f59e0b' : '#a1a1aa' }}>{b.severity || '—'}</td>
                <td className="p-2 font-mono text-[#71717a] text-[11px]">{b.last_seen ? new Date(b.last_seen).toLocaleString() : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="text-[10px] font-mono text-[#52525b]">Live from ClickHouse <span className="text-[#71717a]">omniwatch.anomalies.source_ip</span> grouping — no synthetic GeoIP. World map shown only when geo enrichment is present.</div>
    </div>
  )
}

export function Security() {
  const { timeRange, hours } = useTimeRange()
  const { data, loading } = useFetch<SecurityAnomaliesResponse>(
    async () => {
      const { data } = await api.get<SecurityAnomaliesResponse>('/anomalies', { params: { source_type: 'security', limit: 100, timeRange, hours } })
      return data
    },
    [timeRange],
  )

  const anomalies = data?.anomalies ?? []
  const total = data?.count ?? 0
  const activeCount = anomalies.filter((a) => a.status === 'active').length

  return (
    <div className="p-4 flex flex-col gap-3">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="font-heading text-lg text-[#e4e4e7]" style={{ fontFamily: "'Space Grotesk', sans-serif" }}>
            Security Events
          </h1>
          <p className="text-[#a1a1aa] text-xs font-mono">{total} security anomalies detected <span className="text-[#71717a]">· {timeRange}</span></p>
        </div>
        {activeCount > 0 && (
          <span
            className="px-3 py-1 rounded-lg text-xs font-mono font-bold"
            style={{ background: 'rgba(239,68,68,0.15)', color: '#ef4444', boxShadow: '0 0 10px rgba(239,68,68,0.2)' }}
          >
            {activeCount} active
          </span>
        )}
      </div>

      <div className="grid grid-cols-4 gap-3">
        {[
          { label: 'Total Events', value: total, color: '#00d4ff' },
          { label: 'Active Threats', value: activeCount, color: '#ef4444' },
          { label: 'Avg Score', value: anomalies.length > 0 ? (anomalies.reduce((s, a) => s + a.anomaly_score, 0) / anomalies.length).toFixed(3) : '0.000', color: '#f59e0b' },
          { label: 'Avg Confidence', value: anomalies.length > 0 ? `${(anomalies.reduce((s, a) => s + a.confidence, 0) / anomalies.length).toFixed(1)}%` : '0.0%', color: '#7c3aed' },
        ].map((stat) => (
          <div key={stat.label} className="card p-3 rounded-lg border border-[#2a2a2a]" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
            <div className="text-[#a1a1aa] text-[10px] uppercase tracking-widest font-mono">{stat.label}</div>
            <div className="font-heading text-xl mt-1" style={{ color: stat.color, fontFamily: "'Space Grotesk', sans-serif" }}>
              {stat.value}
            </div>
          </div>
        ))}
      </div>

      <div className="card p-4 h-56 rounded-lg border border-[#2a2a2a]" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
        <div className="text-[#a1a1aa] text-[10px] uppercase tracking-widest mb-2 font-mono">Attack Distribution</div>
        <div className="h-40">
          <AttackDistribution data={anomalies} />
        </div>
      </div>

      <div className="card p-4 rounded-lg border border-[#2a2a2a]" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
        <div className="text-[#a1a1aa] text-[10px] uppercase tracking-widest mb-3 font-mono">Source IP Distribution — live from ClickHouse</div>
        <IpGeoViz timeRange={timeRange} hours={hours} />
      </div>

      <div className="card rounded-lg border border-[#2a2a2a] overflow-hidden" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
        <div className="text-[#a1a1aa] text-[10px] uppercase tracking-widest p-3 border-b border-[#2a2a2a] font-mono">
          Evidence Log
        </div>
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-[#2a2a2a] text-[#a1a1aa] uppercase tracking-widest">
              <th className="text-left p-3 font-mono font-medium">Entity</th>
              <th className="text-left p-3 font-mono font-medium">Attack Type</th>
              <th className="text-left p-3 font-mono font-medium">Source IP</th>
              <th className="text-left p-3 font-mono font-medium">Score</th>
              <th className="text-left p-3 font-mono font-medium">Confidence</th>
              <th className="text-left p-3 font-mono font-medium">Status</th>
              <th className="text-left p-3 font-mono font-medium">Timestamp</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={7} className="p-8 text-center text-[#a1a1aa] animate-pulse font-mono">Loading...</td>
              </tr>
            ) : anomalies.length === 0 ? (
              <tr>
                <td colSpan={7} className="p-8 text-center text-[#a1a1aa] font-mono">No security events detected</td>
              </tr>
            ) : (
              anomalies.map((a, i) => (
                <tr key={i} className="border-b border-[#2a2a2a] hover:bg-[#141618] transition-colors">
                  <td className="p-3 text-[#e4e4e7] font-mono">{a.entity_id}</td>
                  <td className="p-3">
                    <AttackTypeBadge metricName={a.attack_type || a.metric_name} />
                  </td>
                  <td className="p-3 font-mono text-[#00d4ff] text-[11px]">{a.source_ip || '—'}</td>
                  <td className="p-3 text-[#e4e4e7] font-mono">{a.anomaly_score.toFixed(3)}</td>
                  <td className="p-3 text-[#e4e4e7] font-mono">{a.confidence.toFixed(1)}%</td>
                  <td className="p-3">
                    <span
                      className="px-2 py-0.5 rounded text-[10px] font-mono font-bold"
                      style={a.status === 'active'
                        ? { background: 'rgba(239,68,68,0.15)', color: '#ef4444' }
                        : { background: 'rgba(34,197,94,0.15)', color: '#22c55e' }
                      }
                    >
                      {a.status}
                    </span>
                  </td>
                  <td className="p-3 text-[#a1a1aa] font-mono">
                    {a.timestamp ? new Date(a.timestamp).toLocaleString() : '—'}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
