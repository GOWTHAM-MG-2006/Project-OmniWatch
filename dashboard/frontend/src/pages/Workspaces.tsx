import { useCallback, useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  apiError,
  createWorkspace,
  deleteWorkspace,
  listWorkspaces,
  renameWorkspace,
  switchWorkspace,
  type Workspace,
} from '../api/client'
import { getSession, setAccessToken, SESSION_INVALID_EVENT } from '../auth/session'

const inputClass =
  'w-full px-3 py-2 text-sm bg-[#0a0a0f] border border-[#2a2a2a] rounded font-mono text-text-primary focus:outline-none focus:border-accent-cyan/60'
const btnPrimary =
  'px-4 py-1.5 text-xs font-mono rounded bg-accent-cyan/20 hover:bg-accent-cyan/30 text-accent-cyan border border-accent-cyan/30 disabled:opacity-40'
const btnDanger =
  'px-3 py-1 text-xs font-mono rounded bg-red-900/20 hover:bg-red-900/40 text-red-400 border border-red-900/40 disabled:opacity-40'
const btnGhost =
  'px-3 py-1 text-xs font-mono rounded text-text-muted hover:text-text-primary hover:bg-[#1a1a1a] disabled:opacity-40'

export function Workspaces() {
  const navigate = useNavigate()
  const [workspaces, setWorkspaces] = useState<Workspace[]>([])
  const [loading, setLoading] = useState(true)
  const [locked, setLocked] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [createName, setCreateName] = useState('')
  const [creating, setCreating] = useState(false)
  const [renamingId, setRenamingId] = useState<string | null>(null)
  const [renameValue, setRenameValue] = useState('')
  const [confirmDeleteId, setConfirmDeleteId] = useState<string | null>(null)
  const [busyId, setBusyId] = useState<string | null>(null)

  const activeWs = getSession()?.workspaceId ?? null

  const refresh = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const list = await listWorkspaces()
      setWorkspaces(list)
      setLocked(false)
    } catch (err) {
      const status = (err as { response?: { status?: number } })?.response?.status
      if (status === 401) {
        setLocked(true)
        setWorkspaces([])
      } else if (status === 403) {
        // Cross-user/unknown id reads surface as 403 (never 404-leak).
        setError('Workspace not found or access denied.')
      } else {
        setError(apiError(err, 'Failed to load workspaces.'))
      }
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void refresh()
    const onInvalid = () => {
      setLocked(true)
      setWorkspaces([])
    }
    window.addEventListener(SESSION_INVALID_EVENT, onInvalid)
    return () => window.removeEventListener(SESSION_INVALID_EVENT, onInvalid)
  }, [refresh])

  const handleCreate = async (e: FormEvent) => {
    e.preventDefault()
    const name = createName.trim()
    if (!name) {
      setError('Workspace name is required.')
      return
    }
    setCreating(true)
    setError(null)
    setNotice(null)
    try {
      const res = await createWorkspace({ name })
      setCreateName('')
      setNotice(`Workspace “${res.workspace.name}” created.`)
      await refresh()
      navigate(`/workspaces/${res.workspace.workspace_id}/onboarding`)
    } catch (err) {
      const status = (err as { response?: { status?: number } })?.response?.status
      setError(
        status === 409
          ? 'A workspace with that name already exists (409).'
          : apiError(err, 'Failed to create workspace.'),
      )
    } finally {
      setCreating(false)
    }
  }

  const handleSwitch = async (id: string) => {
    setBusyId(id)
    setError(null)
    try {
      // Switch re-issues the access JWT with the new `ws` claim; the whole
      // page context (headers, guards) follows the fresh token, so a stale
      // claim can never leak the previous workspace's data.
      const res = await switchWorkspace(id)
      setAccessToken(res.access_token, res.expires_in)
      setNotice('Workspace switched — context refreshed.')
      await refresh()
      navigate('/')
    } catch (err) {
      setError(apiError(err, 'Switch failed — workspace not found or access denied.'))
    } finally {
      setBusyId(null)
    }
  }

  const handleRename = async (id: string) => {
    const name = renameValue.trim()
    if (!name) {
      setError('New name must not be empty.')
      return
    }
    setBusyId(id)
    setError(null)
    try {
      await renameWorkspace(id, name)
      setRenamingId(null)
      setNotice('Workspace renamed (slug unchanged).')
      await refresh()
    } catch (err) {
      const status = (err as { response?: { status?: number } })?.response?.status
      setError(
        status === 409 ? 'A workspace with that name already exists (409).' : apiError(err, 'Rename failed.'),
      )
    } finally {
      setBusyId(null)
    }
  }

  const handleDelete = async (id: string) => {
    setBusyId(id)
    setError(null)
    try {
      await deleteWorkspace(id)
      setConfirmDeleteId(null)
      setNotice('Workspace deleted (tombstone — data retained per policy).')
      await refresh()
    } catch (err) {
      setError(apiError(err, 'Delete failed — workspace not found or access denied.'))
    } finally {
      setBusyId(null)
    }
  }

  if (locked) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <div className="card p-6 w-full max-w-md rounded-lg border border-[#2a2a2a] text-center">
          <div className="text-sm text-text-primary font-mono">Session expired — please log in again.</div>
          <a href="/login" className="mt-3 inline-block px-4 py-1.5 text-xs font-mono rounded bg-accent-cyan/20 text-accent-cyan border border-accent-cyan/30">
            Go to login
          </a>
        </div>
      </div>
    )
  }

  return (
    <div className="p-4 flex flex-col gap-4 max-w-3xl mx-auto w-full">
      <div>
        <h1 className="text-lg font-semibold text-text-primary">Workspaces</h1>
        <p className="text-xs text-text-muted font-mono">One workspace per application — data never crosses workspaces.</p>
      </div>

      {error && (
        <div role="alert" className="px-3 py-2 text-xs font-mono rounded bg-red-900/20 border border-red-900/40 text-red-400">
          {error}
        </div>
      )}
      {notice && (
        <div role="status" className="px-3 py-2 text-xs font-mono rounded bg-accent-cyan/10 border border-accent-cyan/30 text-accent-cyan">
          {notice}
        </div>
      )}

      <form onSubmit={handleCreate} className="card rounded-lg border border-[#2a2a2a] p-4 flex gap-2">
        <input
          value={createName}
          onChange={(e) => setCreateName(e.target.value)}
          placeholder="New workspace name (e.g. checkout-api)"
          aria-label="New workspace name"
          className={inputClass}
        />
        <button type="submit" disabled={creating} className={`${btnPrimary} whitespace-nowrap`}>
          {creating ? 'Creating…' : 'Create'}
        </button>
      </form>

      {loading ? (
        <div className="text-xs text-text-muted font-mono animate-pulse">Loading workspaces…</div>
      ) : workspaces.length === 0 ? (
        <div className="card rounded-lg border border-[#2a2a2a] p-8 text-center">
          <div className="text-sm text-text-primary">No workspaces yet.</div>
          <div className="mt-1 text-xs text-text-muted font-mono">Create your first workspace above to begin onboarding.</div>
        </div>
      ) : (
        <div className="flex flex-col gap-2">
          {workspaces.map((ws) => (
            <div key={ws.workspace_id} className="card rounded-lg border border-[#2a2a2a] p-4 flex items-center gap-3">
              <div className="flex-1 min-w-0">
                {renamingId === ws.workspace_id ? (
                  <div className="flex gap-2">
                    <input
                      value={renameValue}
                      onChange={(e) => setRenameValue(e.target.value)}
                      aria-label="New workspace name"
                      className={inputClass}
                    />
                    <button onClick={() => void handleRename(ws.workspace_id)} disabled={busyId === ws.workspace_id} className={btnPrimary}>
                      Save
                    </button>
                    <button onClick={() => setRenamingId(null)} className={btnGhost}>Cancel</button>
                  </div>
                ) : (
                  <>
                    <div className="text-sm text-text-primary font-medium truncate">
                      {ws.name}
                      {ws.workspace_id === activeWs && (
                        <span className="ml-2 text-[10px] font-mono px-2 py-0.5 rounded-full bg-accent-cyan/15 text-accent-cyan border border-accent-cyan/30">
                          active
                        </span>
                      )}
                    </div>
                    <div className="text-[11px] text-text-muted font-mono truncate">
                      slug: {ws.slug} · db: {ws.clickhouse_database} · topics: {ws.kafka_topic_prefix || '(default bare names)'}
                    </div>
                  </>
                )}
                {confirmDeleteId === ws.workspace_id && (
                  <div role="alert" className="mt-2 px-3 py-2 text-xs font-mono rounded bg-red-900/20 border border-red-900/40 text-red-400">
                    Delete “{ws.name}”? This tombstones the workspace (data retained 7d, never wiped here).
                    <span className="ml-2 inline-flex gap-2">
                      <button onClick={() => void handleDelete(ws.workspace_id)} disabled={busyId === ws.workspace_id} className={btnDanger}>
                        Confirm delete
                      </button>
                      <button onClick={() => setConfirmDeleteId(null)} className={btnGhost}>Cancel</button>
                    </span>
                  </div>
                )}
              </div>
              {renamingId !== ws.workspace_id && (
                <div className="flex gap-1 shrink-0">
                  {ws.workspace_id !== activeWs && (
                    <button onClick={() => void handleSwitch(ws.workspace_id)} disabled={busyId === ws.workspace_id} className={btnPrimary}>
                      Switch
                    </button>
                  )}
                  <button
                    onClick={() => navigate(`/workspaces/${ws.workspace_id}/onboarding`)}
                    className={btnGhost}
                  >
                    Setup
                  </button>
                  <button
                    onClick={() => {
                      setRenamingId(ws.workspace_id)
                      setRenameValue(ws.name)
                    }}
                    className={btnGhost}
                  >
                    Rename
                  </button>
                  <button onClick={() => setConfirmDeleteId(ws.workspace_id)} className={btnGhost}>
                    Delete
                  </button>
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
