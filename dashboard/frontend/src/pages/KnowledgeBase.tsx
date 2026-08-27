/**
 * OmniWatch — Dashboard Frontend
 * Component: KnowledgeBase Page
 * Phase: 11
 * Purpose: Knowledge base entries from resolved incidents with resolution summaries
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
      <div className="w-16 h-1.5 bg-bg-deep rounded-full overflow-hidden">
        <div className="h-full rounded-full bg-status-healthy" style={{ width: `${pct}%` }} />
      </div>
      <span className="text-[10px] font-mono text-text-muted">{success}/{total}</span>
    </div>
  )
}

export function KnowledgeBase() {
  const { data, loading } = useFetch(fetchKnowledgeBase)
  const entries = data?.entries ?? []
  const total = data?.count ?? 0

  return (
    <div className="p-4 flex flex-col gap-3">
      <div>
        <h1 className="font-heading text-lg text-text-primary">Knowledge Base</h1>
        <p className="text-text-muted text-xs">{total} resolved incident patterns</p>
      </div>

      <div className="card overflow-hidden">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-border-default text-text-muted uppercase tracking-widest">
              <th className="text-left p-3">Root Cause Entity</th>
              <th className="text-left p-3">Type</th>
              <th className="text-left p-3">Resolution</th>
              <th className="text-left p-3">Avg Time (min)</th>
              <th className="text-left p-3">Success Rate</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={5} className="p-8 text-center text-text-muted">Loading...</td>
              </tr>
            ) : entries.length === 0 ? (
              <tr>
                <td colSpan={5} className="p-8 text-center text-text-muted">
                  No knowledge base entries yet. Resolved incidents will appear here.
                </td>
              </tr>
            ) : (
              entries.map((entry, i) => (
                <tr key={i} className="border-b border-border-default hover:bg-bg-deep transition-colors">
                  <td className="p-3 text-text-primary font-mono">{entry.root_cause_entity}</td>
                  <td className="p-3 text-text-muted">{entry.root_cause_type}</td>
                  <td className="p-3 text-text-primary max-w-[300px] truncate">{entry.resolution}</td>
                  <td className="p-3 text-text-primary font-mono">{entry.avg_resolution_minutes}</td>
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
