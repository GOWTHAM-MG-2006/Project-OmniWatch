/**
 * OmniWatch — Dashboard Frontend
 * Component: IncidentExplorer Page
 * Phase: 11
 * Purpose: Filterable incident table with severity badges and status indicators
 * Inputs: Dashboard API — /api/incidents, /api/dashboard/severity-distribution
 * Outputs: Sortable incident list with severity color coding
 */

import { useState } from 'react'
import { useFetch } from '../hooks/useFetch'
import { fetchIncidents, fetchSeverityDistribution } from '../api/client'

const SEVERITY_BADGES: Record<string, string> = {
  P1: 'bg-status-critical text-white',
  P2: 'bg-status-warning text-black',
  P3: 'bg-accent-cyan text-black',
  P4: 'bg-text-muted text-black',
}

const STATUS_BADGES: Record<string, string> = {
  OPEN: 'border border-status-critical text-status-critical',
  RESOLVING: 'border border-status-warning text-status-warning',
  RESOLVED: 'border border-status-healthy text-status-healthy',
  ESCALATED: 'border border-accent-violet text-accent-violet',
}

function SeverityBadge({ severity }: { severity: string }) {
  return (
    <span className={`px-2 py-0.5 rounded text-[10px] font-mono font-bold ${SEVERITY_BADGES[severity] ?? 'bg-bg-deep text-text-muted'}`}>
      {severity}
    </span>
  )
}

function StatusBadge({ status }: { status: string }) {
  return (
    <span className={`px-2 py-0.5 rounded text-[10px] font-mono ${STATUS_BADGES[status] ?? 'text-text-muted'}`}>
      {status}
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

  const incidents = data?.incidents ?? []
  const total = data?.count ?? 0

  return (
    <div className="p-4 flex flex-col gap-3">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="font-heading text-lg text-text-primary">Incident Explorer</h1>
          <p className="text-text-muted text-xs">{total} incidents found</p>
        </div>

        <div className="flex gap-2">
          <select
            value={severityFilter}
            onChange={(e) => setSeverityFilter(e.target.value)}
            className="bg-bg-card border border-border-default rounded px-3 py-1.5 text-xs text-text-primary"
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
            className="bg-bg-card border border-border-default rounded px-3 py-1.5 text-xs text-text-primary"
          >
            <option value="">All Statuses</option>
            <option value="OPEN">Open</option>
            <option value="RESOLVING">Resolving</option>
            <option value="RESOLVED">Resolved</option>
            <option value="ESCALATED">Escalated</option>
          </select>
        </div>
      </div>

      {sevDist?.distribution && (
        <div className="flex gap-2">
          {sevDist.distribution.map((d) => (
            <button
              key={d.severity}
              onClick={() => setSeverityFilter(severityFilter === d.severity ? '' : d.severity)}
              className={`px-3 py-1 rounded text-xs font-mono transition-colors ${
                severityFilter === d.severity
                  ? 'bg-accent-cyan text-black'
                  : 'bg-bg-card border border-border-default text-text-muted hover:border-accent-cyan'
              }`}
            >
              {d.severity}: {d.cnt}
            </button>
          ))}
        </div>
      )}

      <div className="card overflow-hidden">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-border-default text-text-muted uppercase tracking-widest">
              <th className="text-left p-3">Severity</th>
              <th className="text-left p-3">Status</th>
              <th className="text-left p-3">Root Cause</th>
              <th className="text-left p-3">Entity Type</th>
              <th className="text-left p-3">Impacted</th>
              <th className="text-left p-3">SLA Risk</th>
              <th className="text-left p-3">Assigned</th>
              <th className="text-left p-3">Created</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={8} className="p-8 text-center text-text-muted">Loading...</td>
              </tr>
            ) : incidents.length === 0 ? (
              <tr>
                <td colSpan={8} className="p-8 text-center text-text-muted">No incidents found</td>
              </tr>
            ) : (
              incidents.map((inc) => (
                <tr key={inc.incident_id} className="border-b border-border-default hover:bg-bg-deep transition-colors">
                  <td className="p-3"><SeverityBadge severity={inc.severity} /></td>
                  <td className="p-3"><StatusBadge status={inc.status} /></td>
                  <td className="p-3 text-text-primary font-mono truncate max-w-[200px]">{inc.root_cause_entity}</td>
                  <td className="p-3 text-text-muted">{inc.entity_type}</td>
                  <td className="p-3 text-text-primary font-mono">{inc.impacted_services}</td>
                  <td className="p-3">
                    <span className={`font-mono ${
                      inc.sla_breach_risk === 'HIGH' ? 'text-status-critical' :
                      inc.sla_breach_risk === 'MEDIUM' ? 'text-status-warning' :
                      'text-status-healthy'
                    }`}>
                      {inc.sla_breach_risk}
                    </span>
                  </td>
                  <td className="p-3 text-text-muted">{inc.assigned_to}</td>
                  <td className="p-3 text-text-muted font-mono">
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
