/**
 * OmniWatch — Dashboard Frontend
 * Component: GenAIReports Page
 * Phase: 11
 * Purpose: GenAI-generated reports — summary, executive, runbook, postmortem
 * Inputs: Dashboard API — /api/genai/summary, /api/genai/executive, /api/genai/runbook, /api/genai/postmortem
 * Outputs: Markdown-rendered report cards with loading states
 */

import { useFetch } from '../hooks/useFetch'
import api from '../api/client'

interface GenAIReport {
  content: string
  source: string
  timestamp: string
}

function ReportCard({ title, endpoint }: { title: string; endpoint: string }) {
  const { data, loading, error } = useFetch<GenAIReport>(
    async () => {
      const { data } = await api.get<GenAIReport>(endpoint)
      return data
    },
    [endpoint],
  )

  return (
    <div className="col-span-12 card p-4 flex flex-col">
      <div className="flex items-center justify-between mb-2">
        <div className="text-text-muted text-[10px] uppercase tracking-widest">{title}</div>
        {data?.source && (
          <span className="text-[10px] text-text-muted font-mono">via {data.source}</span>
        )}
      </div>
      <div className="flex-1 min-h-[180px]">
        {loading ? (
          <div className="h-full flex items-center justify-center text-text-muted text-sm animate-pulse">
            Generating...
          </div>
        ) : error ? (
          <div className="h-full flex items-center justify-center text-status-critical text-sm">{error}</div>
        ) : (
          <pre className="text-xs text-text-primary whitespace-pre-wrap font-mono leading-relaxed overflow-auto max-h-[200px]">
            {data?.content ?? 'No content available'}
          </pre>
        )}
      </div>
    </div>
  )
}

export function GenAIReports() {
  return (
    <div className="p-4 flex flex-col gap-3">
      <div>
        <h1 className="font-heading text-lg text-text-primary">GenAI Reports</h1>
        <p className="text-text-muted text-xs">AI-generated analysis powered by Ollama + qwen3:8b</p>
      </div>
      <div className="grid-24 gap-2">
        <ReportCard title="System Summary" endpoint="/genai/summary" />
        <ReportCard title="Executive Report" endpoint="/genai/executive" />
        <ReportCard title="Runbook Generation" endpoint="/genai/runbook" />
        <ReportCard title="Post-Incident Analysis" endpoint="/genai/postmortem" />
      </div>
    </div>
  )
}
