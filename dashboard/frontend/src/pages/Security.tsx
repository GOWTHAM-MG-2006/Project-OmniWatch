/**
 * OmniWatch — Dashboard Frontend
 * Component: Security Page
 * Phase: 11
 * Purpose: Security events overview — anomalies with source_type=security
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

function AttackDistribution({ data }: { data: SecurityAnomaly[] }) {
  const counts = new Map<string, number>()
  for (const a of data) {
    counts.set(a.metric_name, (counts.get(a.metric_name) ?? 0) + 1)
  }
  const chartData = Array.from(counts.entries()).map(([name, value]) => ({ name, value }))

  if (!chartData.length) {
    return (
      <div className="h-full flex items-center justify-center text-text-muted text-sm">No attacks detected</div>
    )
  }

  return (
    <div className="h-full flex items-center gap-4">
      <ResponsiveContainer width="60%" height="100%">
        <PieChart>
          <Pie data={chartData} dataKey="value" nameKey="name" cx="50%" cy="50%" innerRadius="50%" outerRadius="80%" strokeWidth={0}>
            {chartData.map((entry) => (
              <Cell key={entry.name} fill={ATTACK_COLORS[entry.name] ?? ATTACK_COLORS.UNKNOWN} />
            ))}
          </Pie>
          <Tooltip
            contentStyle={{ background: '#1a1a1a', border: '1px solid #2a2a2a', borderRadius: 8, fontSize: 12 }}
            labelStyle={{ color: '#a1a1aa' }}
          />
        </PieChart>
      </ResponsiveContainer>
      <div className="flex flex-col gap-1 text-xs">
        {chartData.map((d) => (
          <div key={d.name} className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full" style={{ background: ATTACK_COLORS[d.name] ?? ATTACK_COLORS.UNKNOWN }} />
            <span className="text-text-muted truncate max-w-[140px]">{d.name}</span>
            <span className="text-text-primary font-mono">{d.value}</span>
          </div>
        ))}
      </div>
    </div>
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

  return (
    <div className="p-4 flex flex-col gap-3">
      <div>
        <h1 className="font-heading text-lg text-text-primary">Security Events</h1>
        <p className="text-text-muted text-xs">{total} security anomalies detected</p>
      </div>

      {/* Attack Distribution */}
      <div className="card p-4 h-56">
        <div className="text-text-muted text-[10px] uppercase tracking-widest mb-2">Attack Distribution</div>
        <div className="h-40">
          <AttackDistribution data={anomalies} />
        </div>
      </div>

      {/* Evidence Table */}
      <div className="card overflow-hidden">
        <div className="text-text-muted text-[10px] uppercase tracking-widest p-3 border-b border-border-default">Evidence Log</div>
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-border-default text-text-muted uppercase tracking-widest">
              <th className="text-left p-3">Entity</th>
              <th className="text-left p-3">Attack Type</th>
              <th className="text-left p-3">Score</th>
              <th className="text-left p-3">Confidence</th>
              <th className="text-left p-3">Status</th>
              <th className="text-left p-3">Timestamp</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={6} className="p-8 text-center text-text-muted">Loading...</td>
              </tr>
            ) : anomalies.length === 0 ? (
              <tr>
                <td colSpan={6} className="p-8 text-center text-text-muted">No security events detected</td>
              </tr>
            ) : (
              anomalies.map((a, i) => (
                <tr key={i} className="border-b border-border-default hover:bg-bg-deep transition-colors">
                  <td className="p-3 text-text-primary font-mono">{a.entity_id}</td>
                  <td className="p-3">
                    <span className="px-2 py-0.5 rounded text-[10px] font-mono" style={{ background: `${ATTACK_COLORS[a.metric_name] ?? ATTACK_COLORS.UNKNOWN}22`, color: ATTACK_COLORS[a.metric_name] ?? ATTACK_COLORS.UNKNOWN }}>
                      {a.metric_name}
                    </span>
                  </td>
                  <td className="p-3 text-text-primary font-mono">{a.anomaly_score.toFixed(3)}</td>
                  <td className="p-3 text-text-primary font-mono">{a.confidence.toFixed(1)}%</td>
                  <td className="p-3">
                    <span className={`px-2 py-0.5 rounded text-[10px] font-mono ${
                      a.status === 'active' ? 'bg-status-critical/20 text-status-critical' : 'bg-status-healthy/20 text-status-healthy'
                    }`}>
                      {a.status}
                    </span>
                  </td>
                  <td className="p-3 text-text-muted font-mono">
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
