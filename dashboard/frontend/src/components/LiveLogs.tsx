import { useEffect, useRef, useState } from 'react'
import { getRecentLogs } from '../api/client'

interface LogRow {
  timestamp: string
  level: string
  entity: string
  message: string
}

function levelColor(level: string): string {
  const upper = (level || '').toUpperCase()
  if (upper === 'ERROR' || upper === 'FATAL' || upper === 'CRITICAL') return '#ef4444'
  if (upper === 'WARN' || upper === 'WARNING') return '#f59e0b'
  if (upper === 'DEBUG') return '#71717a'
  return '#22c55e'
}

export function LiveLogs({ database, workspaceId }: { database: string; workspaceId: string }) {
  const [rows, setRows] = useState<LogRow[]>([])
  const [live, setLive] = useState(true)
  const boxRef = useRef<HTMLDivElement>(null)
  const failuresRef = useRef(0)

  useEffect(() => {
    // Streams straight from the workspace importer (in-memory newest-150
    // ring) — no ClickHouse involved. `database` stays as the scope label.
    if (!workspaceId) return
    let cancelled = false
    let timer: ReturnType<typeof setInterval> | undefined
    const poll = async () => {
      try {
        const logs = await getRecentLogs(workspaceId)
        if (cancelled) return
        failuresRef.current = 0
        setLive(true)
        const mapped: LogRow[] = logs.map((row) => ({
          timestamp: String(row.timestamp ?? ''),
          level: String(row.level ?? 'INFO'),
          entity: String(row.entity ?? ''),
          message: String(row.message ?? ''),
        }))
        setRows(mapped.slice(-100))
      } catch {
        if (cancelled) return
        failuresRef.current += 1
        if (failuresRef.current > 2) {
          setLive(false)
          if (timer) {
            clearInterval(timer)
            timer = setInterval(poll, 30000)
          }
        }
      }
    }
    poll()
    timer = setInterval(poll, 5000)
    return () => {
      cancelled = true
      if (timer) clearInterval(timer)
    }
  }, [workspaceId, database])

  useEffect(() => {
    const el = boxRef.current
    if (el) el.scrollTop = el.scrollHeight
  }, [rows])

  return (
    <div className="card p-4 rounded-lg border border-[#2a2a2a]" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
      <div className="flex items-center justify-between mb-2">
        <div className="text-[#a1a1aa] text-[10px] uppercase tracking-widest font-mono">Live Logs</div>
        <div className="flex items-center gap-1.5">
          <span
            className={`w-2 h-2 rounded-full ${live ? 'animate-pulse' : ''}`}
            style={{ background: live ? '#22c55e' : '#6b7280', boxShadow: '0 0 6px rgba(34,197,94,0.4)' }}
          />
          <span className="text-[10px] text-[#a1a1aa] font-mono">{live ? 'Streaming' : 'Paused'}</span>
        </div>
      </div>
      <div ref={boxRef} className="h-[420px] overflow-y-auto font-mono text-[11px] leading-5 pr-2">
        {rows.length === 0 ? (
          <div className="text-[#71717a]">Waiting for log records…</div>
        ) : (
          rows.map((row, i) => (
            <div key={i} className="flex gap-2 whitespace-nowrap">
              <span className="text-[#71717a] shrink-0">{row.timestamp.replace('T', ' ').slice(0, 19)}</span>
              <span className="shrink-0 w-14" style={{ color: levelColor(row.level) }}>{row.level}</span>
              <span className="text-[#00c8c8] shrink-0">{row.entity}</span>
              <span className="text-[#e4e4e7] truncate">{row.message}</span>
            </div>
          ))
        )}
      </div>
    </div>
  )
}
