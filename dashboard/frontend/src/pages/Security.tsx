/**
 * OmniWatch — Dashboard Frontend
 * Component: Security Page
 * Phase: 11
 * Purpose: Security events overview — anomalies with source_type=security — Stitch design polished
 * Inputs: Dashboard API — /api/anomalies?source_type=security
 * Outputs: Security event list with attack type badges and evidence logs
 */

import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from 'recharts'
import { useFetch } from '../hooks/useFetch'
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
}

interface SecurityAnomaliesResponse {
  anomalies: SecurityAnomaly[]
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
    counts.set(a.metric_name, (counts.get(a.metric_name) ?? 0) + 1)
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

export function Security() {
  const { data, loading } = useFetch<SecurityAnomaliesResponse>(
    async () => {
      const { data } = await api.get<SecurityAnomaliesResponse>('/anomalies', { params: { source_type: 'security', limit: 100 } })
      return data
    },
  )

  const anomalies = data?.anomalies ?? []
  const total = data?.count ?? 0
  const activeCount = anomalies.filter((a) => a.status === 'active').length

  return (
    <div className="p-4 flex flex-col gap-3">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="font-heading text-lg text-[#e4e4e7]" style={{ fontFamily: "'Space Grotesk', sans-serif" }}>
            Security Events
          </h1>
          <p className="text-[#a1a1aa] text-xs font-mono">{total} security anomalies detected</p>
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

      {/* Stats row */}
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

      {/* Attack Distribution */}
      <div className="card p-4 h-56 rounded-lg border border-[#2a2a2a]" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
        <div className="text-[#a1a1aa] text-[10px] uppercase tracking-widest mb-2 font-mono">Attack Distribution</div>
        <div className="h-40">
          <AttackDistribution data={anomalies} />
        </div>
      </div>

      {/* Evidence Table */}
      <div className="card rounded-lg border border-[#2a2a2a] overflow-hidden" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
        <div className="text-[#a1a1aa] text-[10px] uppercase tracking-widest p-3 border-b border-[#2a2a2a] font-mono">
          Evidence Log
        </div>
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-[#2a2a2a] text-[#a1a1aa] uppercase tracking-widest">
              <th className="text-left p-3 font-mono font-medium">Entity</th>
              <th className="text-left p-3 font-mono font-medium">Attack Type</th>
              <th className="text-left p-3 font-mono font-medium">Score</th>
              <th className="text-left p-3 font-mono font-medium">Confidence</th>
              <th className="text-left p-3 font-mono font-medium">Status</th>
              <th className="text-left p-3 font-mono font-medium">Timestamp</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={6} className="p-8 text-center text-[#a1a1aa] animate-pulse font-mono">Loading...</td>
              </tr>
            ) : anomalies.length === 0 ? (
              <tr>
                <td colSpan={6} className="p-8 text-center text-[#a1a1aa] font-mono">No security events detected</td>
              </tr>
            ) : (
              anomalies.map((a, i) => (
                <tr key={i} className="border-b border-[#2a2a2a] hover:bg-[#141618] transition-colors">
                  <td className="p-3 text-[#e4e4e7] font-mono">{a.entity_id}</td>
                  <td className="p-3">
                    <AttackTypeBadge metricName={a.metric_name} />
                  </td>
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
