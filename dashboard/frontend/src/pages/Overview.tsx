/**
 * OmniWatch — Dashboard Frontend
 * Component: Overview Page
 * Phase: 11
 * Purpose: Main overview dashboard with KPIs, severity donut, timeline, and topology — Stitch design polished
 * Inputs: Dashboard API (port 8011) — /api/summary, /api/dashboard/*, /api/topology
 * Outputs: 24-column grid layout with live data
 */

import { useFetch } from '../hooks/useFetch'
import { fetchSummary, fetchSeverityDistribution, fetchIncidentsTimeline, fetchTopology } from '../api/client'
import { KpiCard } from '../components/KpiCard'
import { SeverityDonut } from '../components/SeverityDonut'
import { IncidentsTimeline } from '../components/IncidentsTimeline'
import { TopologyMini } from '../components/TopologyMini'

function SkeletonCard() {
  return <div className="col-span-6 card p-4 animate-pulse h-24 rounded-lg border border-[#2a2a2a]" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }} />
}

function SkeletonChart({ className }: { className?: string }) {
  return <div className={`card animate-pulse rounded-lg border border-[#2a2a2a] ${className ?? ''}`} style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }} />
}

export function Overview() {
  const { data: summary, loading: summaryLoading, error: summaryErr } = useFetch(fetchSummary)
  const { data: sevDist, loading: sevLoading } = useFetch(fetchSeverityDistribution)
  const { data: timeline, loading: tlLoading } = useFetch(() => fetchIncidentsTimeline(24))
  const { data: topology, loading: topoLoading } = useFetch(fetchTopology)

  const statusColor = summaryErr ? 'bg-[#ef4444]' : 'bg-[#22c55e]'
  const statusShadow = summaryErr
    ? '0 0 6px rgba(239, 68, 68, 0.4)'
    : '0 0 6px rgba(34, 197, 94, 0.4)'
  const statusLabel = summaryErr ? 'Offline' : summaryLoading ? 'Connecting...' : 'Live'

  return (
    <div className="p-4 flex flex-col gap-3">
      {/* Status indicator */}
      <div className="flex items-center gap-2 mb-1">
        <span
          className={`w-2 h-2 rounded-full ${statusColor} ${!summaryErr && !summaryLoading ? 'animate-pulse' : ''}`}
          style={{ boxShadow: statusShadow }}
        />
        <span className="text-[10px] text-[#a1a1aa] uppercase tracking-widest font-mono">{statusLabel}</span>
        {summary?.timestamp && (
          <span className="text-[10px] text-[#a1a1aa] ml-auto font-mono">
            {new Date(summary.timestamp).toLocaleTimeString()}
          </span>
        )}
      </div>

      {/* KPI Row — 4 x col-span-6 */}
      {summaryLoading ? (
        <>
          <SkeletonCard />
          <SkeletonCard />
          <SkeletonCard />
          <SkeletonCard />
        </>
      ) : (
        <>
          <KpiCard label="Total Incidents" value={summary?.total_incidents ?? 0} color="cyan" />
          <KpiCard label="Active Anomalies" value={summary?.active_anomalies ?? 0} color="red" />
          <KpiCard label="Knowledge Base" value={summary?.knowledge_base_entries ?? 0} color="violet" />
          <KpiCard label="Active Anomalies" value={summary?.active_anomalies ?? 0} color="green" />
        </>
      )}

      {/* Severity Donut + Timeline row */}
      <div className="grid grid-cols-8 gap-3">
        {sevLoading ? (
          <SkeletonChart className="col-span-3 h-56" />
        ) : (
          <div className="col-span-3 card p-4 rounded-lg border border-[#2a2a2a]" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
            <div className="text-[#a1a1aa] text-[10px] uppercase tracking-widest mb-2 font-mono">Severity Distribution</div>
            <div className="h-44">
              <SeverityDonut data={sevDist?.distribution ?? []} />
            </div>
          </div>
        )}

        {tlLoading ? (
          <SkeletonChart className="col-span-5 h-56" />
        ) : (
          <div className="col-span-5 card p-4 rounded-lg border border-[#2a2a2a]" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
            <div className="text-[#a1a1aa] text-[10px] uppercase tracking-widest mb-2 font-mono">Incident Timeline (24h)</div>
            <div className="h-44">
              <IncidentsTimeline data={timeline?.timeline ?? []} />
            </div>
          </div>
        )}
      </div>

      {/* Topology Mini — full width */}
      {topoLoading ? (
        <SkeletonChart className="h-72" />
      ) : (
        <div className="card p-4 rounded-lg border border-[#2a2a2a]" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
          <div className="flex items-center justify-between mb-2">
            <div className="text-[#a1a1aa] text-[10px] uppercase tracking-widest font-mono">Service Topology</div>
            <div className="flex gap-3 text-[10px] text-[#a1a1aa]">
              <span className="flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-full" style={{ background: '#22c55e', boxShadow: '0 0 6px rgba(34,197,94,0.4)' }} />Healthy</span>
              <span className="flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-full" style={{ background: '#f59e0b', boxShadow: '0 0 6px rgba(245,158,11,0.4)' }} />Warning</span>
              <span className="flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-full" style={{ background: '#ef4444', boxShadow: '0 0 6px rgba(239,68,68,0.4)' }} />Critical</span>
            </div>
          </div>
          <div className="h-64">
            <TopologyMini nodes={topology?.nodes ?? []} edges={topology?.edges ?? []} />
          </div>
        </div>
      )}
    </div>
  )
}
