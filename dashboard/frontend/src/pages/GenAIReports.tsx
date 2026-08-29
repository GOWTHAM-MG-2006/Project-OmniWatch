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
  const { data, loading, error, refetch } = useFetch<GenAIReport>(
    async () => {
      const { data } = await api.get<GenAIReport>(endpoint)
      return data
    },
    [endpoint],
  )

  const isEmpty = data?.source === 'empty'

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
          <span className="text-[10px] font-mono px-2 py-0.5 rounded" style={{ background: isEmpty ? 'rgba(234,179,8,0.15)' : 'rgba(0,212,255,0.1)', color: isEmpty ? '#eab308' : '#00d4ff' }}>
            {data.source} · {data.timestamp ? new Date(data.timestamp).toLocaleTimeString() : 'live'}
          </span>
        )}
      </div>
      <div className="flex-1 min-h-[180px]">
        {loading ? (
          <div className="h-full flex flex-col gap-2 p-2">
            <div className="h-3 w-3/4 rounded bg-[#2a2a2a] animate-pulse" />
            <div className="h-3 w-full rounded bg-[#2a2a2a] animate-pulse" />
            <div className="h-3 w-5/6 rounded bg-[#2a2a2a] animate-pulse" />
            <div className="h-3 w-2/3 rounded bg-[#2a2a2a] animate-pulse" />
            <div className="flex items-center justify-center gap-2 text-[#a1a1aa] text-sm mt-4">
              <div className="w-5 h-5 rounded-full border-2 border-[#00d4ff] border-t-transparent animate-spin" />
              <span className="font-mono text-xs">Loading live report...</span>
            </div>
          </div>
        ) : error ? (
          <div className="h-full flex flex-col items-center justify-center gap-3 text-sm font-mono">
            <span className="text-[#ef4444]">{error}</span>
            <button onClick={refetch} className="px-3 py-1 rounded bg-[#00d4ff] text-black text-xs font-semibold hover:bg-[#00b8db] transition-colors">Retry</button>
            <span className="text-[10px] text-[#a1a1aa]">GET {endpoint} → live ClickHouse/MinIO/Ollama</span>
          </div>
        ) : isEmpty ? (
          <div className="h-full flex flex-col items-center justify-center gap-2 text-center p-2">
            <span className="text-[#eab308] text-xs font-mono">No reports yet — generate one</span>
            <pre className="text-xs text-[#a1a1aa] whitespace-pre-wrap font-mono leading-relaxed overflow-auto max-h-[200px] w-full text-left">
              {data?.content ?? 'No incidents recorded yet — run `python simulation/anomaly_injector.py --scenario database_cascade` to seed ClickHouse.'}
            </pre>
            <button onClick={refetch} className="mt-1 px-3 py-1 rounded border border-[#eab308] text-[#eab308] text-xs font-mono hover:bg-[rgba(234,179,8,0.1)] transition-colors">Retry</button>
          </div>
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
