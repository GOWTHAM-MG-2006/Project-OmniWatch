/**
 * OmniWatch — Dashboard Frontend
 * Component: Omni-Agent page
 * Phase: exporter-importer-split
 * Purpose: Show the active workspace's auto-generated importer identity
 *          (endpoint URL + API token, read-only) and manage its exporter
 *          agents (user-given name, auto number, bundle download each)
 * Inputs: session workspace id, identityApi importer/exporter endpoints
 * Outputs: Importer card bound to the active workspace, exporter table
 */

import { useCallback, useEffect, useState } from 'react'
import { getSession } from '../auth/session'
import {
  apiError,
  deleteExporter,
  downloadExporterBundle,
  getImporter,
  getImporterActivity,
  listExporters,
  registerExporter,
  rotateImporter,
  type ActivityPoint,
  type ExporterInfo,
  type ImporterInfo,
} from '../api/client'

// Fresh telemetry newer than this counts as connected (heartbeats ship
// every collection interval, default 60s — 5min tolerates a few misses).
const CONNECTED_WITHIN_MS = 5 * 60 * 1000

function StatusDot({ status }: { status: 'connected' | 'stale' | 'never' }) {
  const color = status === 'connected' ? '#22c55e' : status === 'stale' ? '#eab308' : '#6b7280'
  return (
    <span className="flex items-center gap-1.5">
      <span
        className={`w-2 h-2 rounded-full ${status === 'connected' ? 'animate-pulse' : ''}`}
        style={{ background: color, boxShadow: `0 0 6px ${color}44` }}
      />
      <span className="text-[10px] font-mono" style={{ color }}>{status}</span>
    </span>
  )
}

function agoText(iso: string | null): string {
  if (!iso) return '—'
  const ms = Date.now() - new Date(iso).getTime()
  if (Number.isNaN(ms) || ms < 0) return '—'
  const s = Math.floor(ms / 1000)
  if (s < 60) return `${s}s ago`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m ago`
  const h = Math.floor(m / 60)
  if (h < 48) return `${h}h ago`
  return `${Math.floor(h / 24)}d ago`
}

export function OmniAgent() {
  const session = getSession()
  // The exporter belongs to the workspace it was created in — the active
  // session workspace. No picker: there is nothing to choose.
  const workspaceId = session?.workspaceId ?? ''

  const [importer, setImporter] = useState<ImporterInfo | null>(null)
  const [impLoading, setImpLoading] = useState(false)
  const [impError, setImpError] = useState<string | null>(null)
  const [revealedToken, setRevealedToken] = useState<string | null>(null)
  const [rotating, setRotating] = useState(false)
  const [copied, setCopied] = useState<string | null>(null)
  const [exporters, setExporters] = useState<ExporterInfo[]>([])
  const [activity, setActivity] = useState<ActivityPoint[]>([])
  const [expName, setExpName] = useState('')
  const [expBusy, setExpBusy] = useState(false)
  const [expError, setExpError] = useState<string | null>(null)
  const [downloading, setDownloading] = useState<number | null>(null)
  const [confirmDelete, setConfirmDelete] = useState<number | null>(null)
  const [deleting, setDeleting] = useState<number | null>(null)

  const refresh = useCallback(async (wsId: string) => {
    if (!wsId) {
      setImporter(null)
      setExporters([])
      setActivity([])
      return
    }
    setImpLoading(true)
    setImpError(null)
    setRevealedToken(null)
    try {
      const info = await getImporter(wsId)
      setImporter(info)
      // Auto-generated token, revealed exactly once: on the first read
      // after the workspace was created, or right after a rotation.
      if (info.api_token) setRevealedToken(info.api_token)
      const [list, act] = await Promise.all([
        listExporters(wsId).catch(() => [] as ExporterInfo[]),
        getImporterActivity(wsId).catch(() => null),
      ])
      setExporters(list)
      setActivity(act?.activity ?? [])
    } catch (err) {
      // A 403 here almost always means the session points at a workspace
      // that no longer exists (deleted in another tab / before a rebuild):
      // say so explicitly instead of a generic failure.
      const status = (err as { response?: { status?: number } })?.response?.status
      setImpError(
        status === 403
          ? 'This workspace no longer exists — switch to a live one on the Workspaces page.'
          : 'Failed to load importer for this workspace.',
      )
      setImporter(null)
      setExporters([])
      setActivity([])
    } finally {
      setImpLoading(false)
    }
  }, [])

  useEffect(() => {
    refresh(workspaceId)
  }, [workspaceId, refresh])

  function copyText(kind: string, text: string) {
    void navigator.clipboard?.writeText(text).then(
      () => {
        setCopied(kind)
        window.setTimeout(() => setCopied((c) => (c === kind ? null : c)), 2000)
      },
      () => undefined,
    ).catch(() => undefined)
  }

  async function onRotate() {
    if (!workspaceId) return
    setRotating(true)
    try {
      const issued = await rotateImporter(workspaceId)
      setImporter(issued)
      setRevealedToken(issued.api_token)
    } catch {
      setImpError('Rotation failed.')
    } finally {
      setRotating(false)
    }
  }

  async function onCreate() {
    if (!workspaceId || !expName.trim()) {
      setExpError('Exporter name is required.')
      return
    }
    setExpBusy(true)
    setExpError(null)
    try {
      // Endpoint URL + API token come from this workspace's importer
      // automatically — only the name is user-given.
      await registerExporter({ workspace_id: workspaceId, name: expName.trim() })
      setExpName('')
      const list = await listExporters(workspaceId)
      setExporters(list)
    } catch (err) {
      const status = (err as { response?: { status?: number } })?.response?.status
      setExpError(
        status === 403
          ? 'Workspace not found or access denied — it may have been deleted. Switch workspaces and retry.'
          : 'Creation failed.',
      )
    } finally {
      setExpBusy(false)
    }
  }

  async function onDownload(number: number) {
    if (!workspaceId) return
    setDownloading(number)
    setExpError(null)
    try {
      const blob = await downloadExporterBundle(workspaceId, number)
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `omniwatch-exporter-${importer?.workspace_slug ?? 'ws'}-${number}.zip`
      document.body.appendChild(a)
      a.click()
      a.remove()
      URL.revokeObjectURL(url)
    } catch (err) {
      // Never fail silently: a stuck "Preparing…" with no file was the
      // original complaint. Surface the backend's reason instead.
      setExpError(apiError(err, 'Download failed.'))
    } finally {
      setDownloading(null)
    }
  }

  async function onDelete(number: number) {
    if (!workspaceId) return
    setDeleting(number)
    setExpError(null)
    try {
      await deleteExporter(workspaceId, number)
      setConfirmDelete(null)
      const list = await listExporters(workspaceId)
      setExporters(list)
    } catch (err) {
      const status = (err as { response?: { status?: number } })?.response?.status
      setExpError(
        status === 403
          ? 'Workspace not found or access denied — it may have been deleted. Switch workspaces and retry.'
          : apiError(err, 'Delete failed.'),
      )
    } finally {
      setDeleting(null)
    }
  }

  return (
    <div className="p-4 flex flex-col gap-4">
      <div>
        <h1 className="font-heading text-lg text-[#e4e4e7]" style={{ fontFamily: "'Space Grotesk', sans-serif" }}>
          Omni-Agent
        </h1>
        <p className="text-[#a1a1aa] text-xs font-mono">
          This workspace&apos;s importer identity and its exporter agents — telemetry flows only here.
        </p>
      </div>

      {!workspaceId ? (
        <div className="card rounded-lg border border-[#2a2a2a] p-8 text-center" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
          <p className="text-sm text-[#e4e4e7]">No active workspace.</p>
          <p className="text-[11px] font-mono text-[#71717a] mt-1">Switch to a workspace (Workspaces page) to see its importer and exporters.</p>
        </div>
      ) : (
        <>
          <div className="card rounded-lg border border-[#2a2a2a] p-4 flex flex-col gap-3" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
            <h2 className="text-sm font-semibold text-[#e4e4e7]">
              Workspace importer{importer ? <span className="ml-2 text-[11px] font-mono text-[#a1a1aa]">{importer.workspace_slug}</span> : null}
            </h2>
            {impLoading ? (
              <p className="text-xs font-mono text-[#a1a1aa] animate-pulse">Loading importer…</p>
            ) : impError ? (
              <p className="text-xs font-mono text-[#ef4444]">{impError}</p>
            ) : importer ? (
              <div className="flex flex-col gap-2 text-xs font-mono">
                <div className="flex flex-col gap-1">
                  <span className="text-[#a1a1aa]">Agent endpoint URL (auto-generated, read-only)</span>
                  <div className="flex gap-2 items-center">
                    <span className="flex-1 bg-[#0c0c0e] border border-[#2a2a2a] rounded px-3 py-2 text-[#e4e4e7] break-all">{importer.endpoint_url}</span>
                    <button
                      className="px-3 py-2 rounded border border-accent-cyan/40 text-accent-cyan text-[11px] shrink-0"
                      onClick={() => copyText('endpoint', importer.endpoint_url)}
                    >
                      {copied === 'endpoint' ? 'Copied' : 'Copy'}
                    </button>
                  </div>
                </div>
                <div className="flex flex-col gap-1">
                  <span className="text-[#a1a1aa]">API token (auto-generated, read-only)</span>
                  {revealedToken ? (
                    <div className="flex gap-2 items-center">
                      <span className="flex-1 bg-[#0c0c0e] border border-accent-cyan/40 rounded px-3 py-2 text-accent-cyan break-all">{revealedToken}</span>
                      <button
                        className="px-3 py-2 rounded border border-accent-cyan/40 text-accent-cyan text-[11px] shrink-0"
                        onClick={() => copyText('token', revealedToken)}
                      >
                        {copied === 'token' ? 'Copied' : 'Copy'}
                      </button>
                    </div>
                  ) : (
                    <p className="text-[#71717a]">Shown once when issued (workspace creation or rotation) — stored hashed, never displayed again.</p>
                  )}
                </div>
                <div>
                  <button
                    className="px-4 py-2 rounded bg-accent-cyan/15 text-accent-cyan border border-accent-cyan/40 text-sm disabled:opacity-50"
                    disabled={rotating}
                    onClick={onRotate}
                  >
                    {rotating ? 'Rotating…' : 'Rotate API token'}
                  </button>
                </div>
              </div>
            ) : null}
          </div>

          <div className="card rounded-lg border border-[#2a2a2a] p-4 flex flex-col gap-3" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
            <h2 className="text-sm font-semibold text-[#e4e4e7]">Exporters</h2>
            <div className="flex gap-2 items-end max-w-md">
              <label className="flex flex-col gap-1 text-xs text-[#a1a1aa] flex-1">
                New exporter name
                <input
                  className="bg-[#0c0c0e] border border-[#2a2a2a] rounded px-3 py-2 text-[#e4e4e7] font-mono"
                  placeholder="web-01"
                  autoComplete="off"
                  value={expName}
                  onChange={(e) => setExpName(e.target.value)}
                />
              </label>
              <button
                className="px-4 py-2 rounded bg-accent-cyan/15 text-accent-cyan border border-accent-cyan/40 text-sm disabled:opacity-50"
                disabled={expBusy}
                onClick={onCreate}
              >
                {expBusy ? 'Creating…' : 'Create'}
              </button>
            </div>
            {expError && <p className="text-xs font-mono text-[#ef4444]">{expError}</p>}
            <p className="text-[11px] font-mono text-[#71717a]">
              Created with this workspace&apos;s endpoint URL and API token automatically — only the name is yours.
            </p>
            {exporters.length === 0 ? (
              <p className="text-[11px] font-mono text-[#71717a]">No exporters created yet.</p>
            ) : (
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-[#2a2a2a] text-[#a1a1aa] uppercase tracking-widest">
                    <th className="text-left p-2 font-mono font-medium">#</th>
                    <th className="text-left p-2 font-mono font-medium">Name</th>
                    <th className="text-left p-2 font-mono font-medium">Created</th>
                    <th className="text-left p-2 font-mono font-medium"></th>
                  </tr>
                </thead>
                <tbody>
                  {exporters.map((e) => (
                    <tr key={e.number} className="border-b border-[#2a2a2a]">
                      <td className="p-2 text-[#e4e4e7] font-mono">{e.number}</td>
                      <td className="p-2 text-[#e4e4e7] font-mono">{e.name}</td>
                      <td className="p-2 text-[#a1a1aa] font-mono">
                        {e.created_at ? new Date(e.created_at).toLocaleString() : '—'}
                      </td>
                      <td className="p-2 text-right whitespace-nowrap">
                        <button
                          className="px-3 py-1 rounded border border-accent-cyan/40 text-accent-cyan text-[11px] disabled:opacity-50 mr-2"
                          disabled={downloading === e.number}
                          onClick={() => onDownload(e.number)}
                        >
                          {downloading === e.number ? 'Preparing…' : 'Download bundle'}
                        </button>
                        {confirmDelete === e.number ? (
                          <button
                            className="px-3 py-1 rounded border border-[#ef4444] text-[#ef4444] text-[11px] disabled:opacity-50"
                            disabled={deleting === e.number}
                            onClick={() => onDelete(e.number)}
                          >
                            {deleting === e.number ? 'Deleting…' : 'Confirm delete'}
                          </button>
                        ) : (
                          <button
                            className="px-3 py-1 rounded border border-[#ef4444]/50 text-[#ef4444] text-[11px]"
                            onClick={() => setConfirmDelete(e.number)}
                          >
                            Delete
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          <div className="card rounded-lg border border-[#2a2a2a] p-4 flex flex-col gap-3" style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}>
            <h2 className="text-sm font-semibold text-[#e4e4e7]">Exporter agent status</h2>
            <p className="text-[11px] font-mono text-[#71717a]">
              Live — derived from telemetry actually received by this workspace&apos;s importer.
            </p>
            {(() => {
              const latestByEntity = new Map<string, string>()
              for (const p of activity) {
                const prev = latestByEntity.get(p.entity_id)
                if (!prev || p.last_seen > prev) latestByEntity.set(p.entity_id, p.last_seen)
              }
              const matched = new Set<string>()
              const rows: Array<{
                key: string
                label: string
                status: 'connected' | 'stale' | 'never'
                seen: string | null
              }> = exporters.map((e) => {
                const prefix = `exporter-${e.number}-`
                let best: string | null = null
                let entity = ''
                for (const [eid, seen] of latestByEntity) {
                  if (eid.startsWith(prefix) && (best === null || seen > best)) {
                    best = seen
                    entity = eid
                  }
                }
                if (entity) matched.add(entity)
                const age = best === null ? null : Date.now() - new Date(best).getTime()
                const status = best === null || age === null || Number.isNaN(age)
                  ? 'never'
                  : age <= CONNECTED_WITHIN_MS ? 'connected' : 'stale'
                return { key: `exp-${e.number}`, label: `#${e.number} ${e.name}`, status, seen: best }
              })
              for (const [eid, seen] of latestByEntity) {
                if (!matched.has(eid)) {
                  const age = Date.now() - new Date(seen).getTime()
                  rows.push({
                    key: `unknown-${eid}`,
                    label: eid,
                    status: Number.isNaN(age) || age > CONNECTED_WITHIN_MS ? 'stale' : 'connected',
                    seen,
                  })
                }
              }
              if (rows.length === 0) {
                return <p className="text-[11px] font-mono text-[#71717a]">No exporters reporting yet — create one above, download its bundle, and run it.</p>
              }
              return (
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-[#2a2a2a] text-[#a1a1aa] uppercase tracking-widest">
                      <th className="text-left p-2 font-mono font-medium">Status</th>
                      <th className="text-left p-2 font-mono font-medium">Exporter</th>
                      <th className="text-left p-2 font-mono font-medium">Last seen</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((r) => (
                      <tr key={r.key} className="border-b border-[#2a2a2a]">
                        <td className="p-2"><StatusDot status={r.status} /></td>
                        <td className="p-2 text-[#e4e4e7] font-mono truncate max-w-[260px]">{r.label}</td>
                        <td className="p-2 text-[#a1a1aa] font-mono">{agoText(r.seen)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )
            })()}
          </div>
        </>
      )}
    </div>
  )
}
