/**
 * OmniWatch — Dashboard Frontend
 * Component: KnowledgeBase Page
 * Phase: 11
 * Purpose: Knowledge base entries from resolved incidents with resolution summaries — Stitch design polished
 * Inputs: Dashboard API — /api/knowledge-base
 * Outputs: Searchable KB table with success/failure counts
 */

import { useFetch } from '../hooks/useFetch'
import { fetchKnowledgeBase } from '../api/client'

function SuccessBar({ success, failure }: { success: number; failure: number }) {
  const total = success + failure
  const pct = total > 0 ? (success / total) * 100 : 0
  return (
    <div className="flex items-center gap-2">
      <div className="w-20 h-2 bg-[#2a2a2a] rounded-full overflow-hidden">
        <div
          className="h-full rounded-full"
          style={{
            width: `${pct}%`,
            background: 'linear-gradient(90deg, #22c55e88, #22c55e)',
            boxShadow: '0 0 6px rgba(34,197,94,0.3)',
          }}
        />
      </div>
      <span className="text-[10px] font-mono text-[#a1a1aa]">{success}/{total}</span>
    </div>
  )
}

function TypeTag({ type }: { type: string }) {
  return (
    <span
      className="px-2 py-0.5 rounded text-[10px] font-mono"
      style={{
        background: 'rgba(0, 212, 255, 0.1)',
        color: '#00d4ff',
      }}
    >
      {type}
    </span>
  )
}

export function KnowledgeBase() {
  const { data, loading } = useFetch(fetchKnowledgeBase)
  const entries = data?.entries ?? []
  const total = data?.count ?? 0

  return (
    <div className="p-4 flex flex-col gap-3">
      {/* Header */}
      <div>
        <h1 className="font-heading text-lg text-[#e4e4e7]" style={{ fontFamily: "'Space Grotesk', sans-serif" }}>
          Knowledge Base
        </h1>
        <p className="text-[#a1a1aa] text-xs font-mono">{total} resolved incident patterns</p>
      </div>

      {/* Stats row */}
      <div className="grid grid-cols-3 gap-3">
        <div className="card p-3 rounded-lg border border-[#2a2a2a]" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
          <div className="text-[#a1a1aa] text-[10px] uppercase tracking-widest font-mono">Total Patterns</div>
          <div className="font-heading text-2xl mt-1 text-[#00d4ff]" style={{ fontFamily: "'Space Grotesk', sans-serif" }}>{total}</div>
        </div>
        <div className="card p-3 rounded-lg border border-[#2a2a2a]" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
          <div className="text-[#a1a1aa] text-[10px] uppercase tracking-widest font-mono">Avg Resolution</div>
          <div className="font-heading text-2xl mt-1 text-[#f59e0b]" style={{ fontFamily: "'Space Grotesk', sans-serif" }}>
            {entries.length > 0
              ? (entries.reduce((s, e) => s + (e.avg_resolution_minutes ?? 0), 0) / entries.length).toFixed(1)
              : '0'} min
          </div>
        </div>
        <div className="card p-3 rounded-lg border border-[#2a2a2a]" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
          <div className="text-[#a1a1aa] text-[10px] uppercase tracking-widest font-mono">Overall Success</div>
          <div className="font-heading text-2xl mt-1 text-[#22c55e]" style={{ fontFamily: "'Space Grotesk', sans-serif" }}>
            {entries.length > 0
              ? ((entries.reduce((s, e) => s + e.success_count, 0) /
                  Math.max(entries.reduce((s, e) => s + e.success_count + e.failure_count, 0), 1)) * 100).toFixed(0)
              : '0'}%
          </div>
        </div>
      </div>

      {/* Table */}
      <div className="card rounded-lg border border-[#2a2a2a] overflow-hidden" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-[#2a2a2a] text-[#a1a1aa] uppercase tracking-widest">
              <th className="text-left p-3 font-mono font-medium">Root Cause Entity</th>
              <th className="text-left p-3 font-mono font-medium">Type</th>
              <th className="text-left p-3 font-mono font-medium">Resolution</th>
              <th className="text-left p-3 font-mono font-medium">Avg Time (min)</th>
              <th className="text-left p-3 font-mono font-medium">Success Rate</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={5} className="p-8 text-center text-[#a1a1aa] animate-pulse font-mono">Loading...</td>
              </tr>
            ) : entries.length === 0 ? (
              <tr>
                <td colSpan={5} className="p-8 text-center text-[#a1a1aa] font-mono">
                  No knowledge base entries yet. Resolved incidents will appear here.
                </td>
              </tr>
            ) : (
              entries.map((entry, i) => (
                <tr key={i} className="border-b border-[#2a2a2a] hover:bg-[#141618] transition-colors">
                  <td className="p-3 text-[#e4e4e7] font-mono">{entry.root_cause_entity}</td>
                  <td className="p-3"><TypeTag type={entry.root_cause_type} /></td>
                  <td className="p-3 text-[#a1a1aa] max-w-[300px] truncate">{entry.resolution}</td>
                  <td className="p-3 text-[#e4e4e7] font-mono">{entry.avg_resolution_minutes}</td>
                  <td className="p-3">
                    <SuccessBar success={entry.success_count} failure={entry.failure_count} />
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
