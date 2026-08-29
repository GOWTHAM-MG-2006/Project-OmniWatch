/**
 * OmniWatch — Dashboard Frontend
 * Component: IncidentExplorer Page
 * Phase: 11
 * Purpose: Filterable incident table with severity badges and status indicators — Stitch design polished
 * Inputs: Dashboard API — /api/incidents, /api/dashboard/severity-distribution
 * Outputs: Sortable incident list with severity color coding
 */

import { useState } from 'react'
import { useFetch } from '../hooks/useFetch'
import { fetchIncidents, fetchSeverityDistribution } from '../api/client'

const SEVERITY_STYLES: Record<string, { bg: string; text: string; shadow: string }> = {
  P1: { bg: 'rgba(239, 68, 68, 0.2)', text: '#ef4444', shadow: '0 0 8px rgba(239,68,68,0.3)' },
  P2: { bg: 'rgba(245, 158, 11, 0.2)', text: '#f59e0b', shadow: '0 0 8px rgba(245,158,11,0.3)' },
  P3: { bg: 'rgba(0, 212, 255, 0.15)', text: '#00d4ff', shadow: '0 0 8px rgba(0,212,255,0.2)' },
  P4: { bg: 'rgba(161, 161, 170, 0.15)', text: '#a1a1aa', shadow: 'none' },
}

const STATUS_STYLES: Record<string, { border: string; text: string; shadow: string }> = {
  OPEN: { border: 'rgba(239,68,68,0.4)', text: '#ef4444', shadow: '0 0 6px rgba(239,68,68,0.2)' },
  RESOLVING: { border: 'rgba(245,158,11,0.4)', text: '#f59e0b', shadow: '0 0 6px rgba(245,158,11,0.2)' },
  RESOLVED: { border: 'rgba(34,197,94,0.4)', text: '#22c55e', shadow: '0 0 6px rgba(34,197,94,0.2)' },
  ESCALATED: { border: 'rgba(124,58,237,0.4)', text: '#7c3aed', shadow: '0 0 6px rgba(124,58,237,0.2)' },
}

function SeverityBadge({ severity }: { severity: string }) {
  const style = SEVERITY_STYLES[severity] ?? SEVERITY_STYLES.P4
  return (
    <span
      className="px-2.5 py-1 rounded-md text-[10px] font-mono font-bold"
      style={{ background: style.bg, color: style.text, boxShadow: style.shadow }}
    >
      {severity}
    </span>
  )
}

function StatusDot({ status }: { status: string }) {
  const style = STATUS_STYLES[status]
  const color = style?.text ?? '#6b7280'
  const isLive = status === 'OPEN' || status === 'RESOLVING'
  return (
    <span className="flex items-center gap-1.5">
      <span
        className={`w-2 h-2 rounded-full ${isLive ? 'animate-pulse' : ''}`}
        style={{ background: color, boxShadow: `0 0 6px ${color}44` }}
      />
      <span className="text-[10px] font-mono" style={{ color: style?.text ?? '#a1a1aa' }}>{status}</span>
    </span>
  )
}

export function IncidentExplorer() {
  const [severityFilter, setSeverityFilter] = useState<string>('')
  const [statusFilter, setStatusFilter] = useState<string>('')

  const { data, loading } = useFetch(
    () => fetchIncidents({ severity: severityFilter || undefined, status: statusFilter || undefined, limit: 100 }),
    [severityFilter, statusFilter],
  )

  const { data: sevDist } = useFetch(fetchSeverityDistribution)

  const incidents = data?.incidents ?? data?.items ?? []
  const total = data?.count ?? data?.total_count ?? incidents.length

  return (
    <div className="p-4 flex flex-col gap-3">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="font-heading text-lg text-[#e4e4e7]" style={{ fontFamily: "'Space Grotesk', sans-serif" }}>
            Incident Explorer
          </h1>
          <p className="text-[#a1a1aa] text-xs font-mono">
            {incidents.length > 0
              ? `${incidents.length}${total > incidents.length ? ` of ${total}` : ''} incidents found`
              : total > 0
                ? `${total} incidents found (no matching data loaded)`
                : 'No incidents found'
            }
          </p>
        </div>

        <div className="flex gap-2">
          <select
            value={severityFilter}
            onChange={(e) => setSeverityFilter(e.target.value)}
            className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg px-3 py-1.5 text-xs text-[#e4e4e7] font-mono focus:outline-none focus:border-[#00d4ff] transition-colors"
          >
            <option value="">All Severities</option>
            <option value="P1">P1 — Critical</option>
            <option value="P2">P2 — High</option>
            <option value="P3">P3 — Medium</option>
            <option value="P4">P4 — Low</option>
          </select>

          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg px-3 py-1.5 text-xs text-[#e4e4e7] font-mono focus:outline-none focus:border-[#00d4ff] transition-colors"
          >
            <option value="">All Statuses</option>
            <option value="OPEN">Open</option>
            <option value="RESOLVING">Resolving</option>
            <option value="RESOLVED">Resolved</option>
            <option value="ESCALATED">Escalated</option>
          </select>
        </div>
      </div>

      {/* Filter chips */}
      {sevDist?.distribution && (
        <div className="flex gap-2">
          {sevDist.distribution.map((d) => {
            const isActive = severityFilter === d.severity
            const style = SEVERITY_STYLES[d.severity] ?? SEVERITY_STYLES.P4
            return (
              <button
                key={d.severity}
                onClick={() => setSeverityFilter(isActive ? '' : d.severity)}
                className="px-3 py-1.5 rounded-lg text-xs font-mono transition-all duration-200"
                style={isActive
                  ? { background: style.bg, color: style.text, boxShadow: style.shadow }
                  : { background: '#1a1a1a', color: '#a1a1aa', border: '1px solid #2a2a2a' }
                }
              >
                {d.severity}: {d.cnt}
              </button>
            )
          })}
        </div>
      )}

      {/* Table */}
      <div className="card rounded-lg border border-[#2a2a2a] overflow-hidden" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-[#2a2a2a] text-[#a1a1aa] uppercase tracking-widest">
              <th className="text-left p-3 font-mono font-medium">Severity</th>
              <th className="text-left p-3 font-mono font-medium">Status</th>
              <th className="text-left p-3 font-mono font-medium">Root Cause</th>
              <th className="text-left p-3 font-mono font-medium">Entity Type</th>
              <th className="text-left p-3 font-mono font-medium">Impacted</th>
              <th className="text-left p-3 font-mono font-medium">SLA Risk</th>
              <th className="text-left p-3 font-mono font-medium">Assigned</th>
              <th className="text-left p-3 font-mono font-medium">Created</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={8} className="p-8 text-center text-[#a1a1aa] animate-pulse">Loading...</td>
              </tr>
            ) : incidents.length === 0 ? (
              <tr>
                <td colSpan={8} className="p-8 text-center text-[#a1a1aa]">No incidents found</td>
              </tr>
            ) : (
              incidents.map((inc: any) => (
                <tr key={inc.incident_id} className="border-b border-[#2a2a2a] hover:bg-[#141618] transition-colors">
                  <td className="p-3"><SeverityBadge severity={inc.severity ?? 'P4'} /></td>
                  <td className="p-3"><StatusDot status={inc.status ?? 'OPEN'} /></td>
                  <td className="p-3 text-[#e4e4e7] font-mono truncate max-w-[200px]">{inc.root_cause_entity ?? '—'}</td>
                  <td className="p-3 text-[#a1a1aa]">{inc.entity_type ?? '—'}</td>
                  <td className="p-3 text-[#e4e4e7] font-mono">{inc.impacted_services ?? '—'}</td>
                  <td className="p-3">
                    <span className={`font-mono text-[10px] font-bold ${
                      inc.sla_breach_risk === 'HIGH' ? 'text-[#ef4444]' :
                      inc.sla_breach_risk === 'MEDIUM' ? 'text-[#f59e0b]' :
                      'text-[#22c55e]'
                    }`}>
                      {inc.sla_breach_risk ?? '—'}
                    </span>
                  </td>
                  <td className="p-3 text-[#a1a1aa]">{inc.assigned_to ?? '—'}</td>
                  <td className="p-3 text-[#a1a1aa] font-mono">
                    {inc.created_at ? new Date(inc.created_at).toLocaleString() : '—'}
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
