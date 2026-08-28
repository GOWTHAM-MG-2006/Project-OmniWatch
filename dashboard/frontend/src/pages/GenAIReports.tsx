/**
 * OmniWatch — Dashboard Frontend
 * Component: GenAIReports Page
 * Phase: 11
 * Purpose: GenAI-generated reports — summary, executive, runbook, postmortem — Stitch design polished
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

const REPORT_ICONS: Record<string, string> = {
  'System Summary': '📊',
  'Executive Report': '📋',
  'Runbook Generation': '🔧',
  'Post-Incident Analysis': '🔍',
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
    <div
      className="col-span-6 card p-4 flex flex-col rounded-lg border border-[#2a2a2a] transition-all duration-200 hover:scale-[1.01]"
      style={{
        background: 'linear-gradient(135deg, #1a1a1a, #141618)',
        boxShadow: '0 0 15px rgba(0,0,0,0.3)',
      }}
    >
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <span className="text-sm">{REPORT_ICONS[title] ?? '📄'}</span>
          <div className="text-[#e4e4e7] text-[10px] uppercase tracking-widest font-mono">{title}</div>
        </div>
        {data?.source && (
          <span className="text-[10px] text-[#a1a1aa] font-mono px-2 py-0.5 rounded" style={{ background: 'rgba(0,212,255,0.1)', color: '#00d4ff' }}>
            {data.source}
          </span>
        )}
      </div>
      <div className="flex-1 min-h-[180px]">
        {loading ? (
          <div className="h-full flex flex-col items-center justify-center gap-2 text-[#a1a1aa] text-sm">
            <div className="w-8 h-8 rounded-full border-2 border-[#00d4ff] border-t-transparent animate-spin" />
            <span className="font-mono text-xs">Generating...</span>
          </div>
        ) : error ? (
          <div className="h-full flex items-center justify-center text-[#ef4444] text-sm font-mono">{error}</div>
        ) : (
          <pre className="text-xs text-[#a1a1aa] whitespace-pre-wrap font-mono leading-relaxed overflow-auto max-h-[200px]">
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
      <div className="flex items-center justify-between">
        <div>
          <h1 className="font-heading text-lg text-[#e4e4e7]" style={{ fontFamily: "'Space Grotesk', sans-serif" }}>
            GenAI Reports
          </h1>
          <p className="text-[#a1a1aa] text-xs font-mono">AI-generated analysis powered by Ollama + qwen3:8b</p>
        </div>
      </div>

      <div className="grid grid-cols-12 gap-3">
        <ReportCard title="System Summary" endpoint="/genai/summary" />
        <ReportCard title="Executive Report" endpoint="/genai/executive" />
        <ReportCard title="Runbook Generation" endpoint="/genai/runbook" />
        <ReportCard title="Post-Incident Analysis" endpoint="/genai/postmortem" />
      </div>
    </div>
  )
}
