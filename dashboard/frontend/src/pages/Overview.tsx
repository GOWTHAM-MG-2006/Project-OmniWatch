/**
 * OmniWatch — Dashboard Frontend
 * Component: Overview Page
 * Phase: 11
 * Purpose: Main overview dashboard with KPIs, severity donut, timeline, and topology
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
  return <div className="card p-4 animate-pulse h-24 bg-bg-deep rounded-lg" />
}

function SkeletonChart({ className }: { className?: string }) {
  return <div className={`card animate-pulse bg-bg-deep rounded-lg ${className ?? ''}`} />
}

export function Overview() {
  const { data: summary, loading: summaryLoading, error: summaryErr } = useFetch(fetchSummary)
  const { data: sevDist, loading: sevLoading } = useFetch(fetchSeverityDistribution)
  const { data: timeline, loading: tlLoading } = useFetch(() => fetchIncidentsTimeline(24))
  const { data: topology, loading: topoLoading } = useFetch(fetchTopology)

  const statusColor = summaryErr ? 'text-status-critical' : 'text-status-healthy'
  const statusLabel = summaryErr ? 'Offline' : summaryLoading ? 'Connecting...' : 'Live'

  return (
    <div className="grid-24 gap-2 p-4">
      {/* Status indicator */}
      <div className="col-span-24 flex items-center gap-2 mb-1">
        <span className={`w-2 h-2 rounded-full ${statusColor} ${!summaryErr && !summaryLoading ? 'animate-pulse' : ''}`} />
        <span className="text-[10px] text-text-muted uppercase tracking-widest">{statusLabel}</span>
        {summary?.timestamp && (
          <span className="text-[10px] text-text-muted ml-auto">
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
          <KpiCard label="Total Incidents" value={summary?.total_incidents ?? 0} />
          <KpiCard label="Active Anomalies" value={summary?.active_anomalies ?? 0} />
          <KpiCard label="Knowledge Base" value={summary?.knowledge_base_entries ?? 0} />
          <KpiCard label="Active Anomalies" value={summary?.active_anomalies ?? 0} />
        </>
      )}

      {/* Severity Donut — col-span-8 */}
      {sevLoading ? (
        <SkeletonChart className="col-span-8 h-56" />
      ) : (
        <div className="col-span-8 card p-4">
          <div className="text-text-muted text-[10px] uppercase tracking-widest mb-2">Severity Distribution</div>
          <div className="h-44">
            <SeverityDonut data={sevDist?.distribution ?? []} />
          </div>
        </div>
      )}

      {/* Timeline — col-span-16 */}
      {tlLoading ? (
        <SkeletonChart className="col-span-16 h-56" />
      ) : (
        <div className="col-span-16 card p-4">
          <div className="text-text-muted text-[10px] uppercase tracking-widest mb-2">Incident Timeline (24h)</div>
          <div className="h-44">
            <IncidentsTimeline data={timeline?.timeline ?? []} />
          </div>
        </div>
      )}

      {/* Topology Mini — col-span-24 */}
      {topoLoading ? (
        <SkeletonChart className="col-span-24 h-72" />
      ) : (
        <div className="col-span-24 card p-4">
          <div className="flex items-center justify-between mb-2">
            <div className="text-text-muted text-[10px] uppercase tracking-widest">Service Topology</div>
            <div className="flex gap-3 text-[10px] text-text-muted">
              <span><span className="inline-block w-2 h-2 rounded-full bg-status-healthy mr-1" />Healthy</span>
              <span><span className="inline-block w-2 h-2 rounded-full bg-status-warning mr-1" />Warning</span>
              <span><span className="inline-block w-2 h-2 rounded-full bg-status-critical mr-1" />Critical</span>
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
