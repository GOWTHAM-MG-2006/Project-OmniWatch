import { useCallback, useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  apiError,
  listWorkspaces,
  switchWorkspace,
  type Workspace,
} from '../api/client'
import {
  getSession,
  setAccessToken,
  SESSION_INVALID_EVENT,
} from '../auth/session'

/**
 * Workspace switcher (ENTRY-4): lists my workspaces, switches the active
 * one by re-issuing the JWT (`ws` claim) and reloading page context.
 * A 401 anywhere re-locks the switcher to the logged-out state.
 */
export function WorkspaceSwitcher() {
  const navigate = useNavigate()
  const [workspaces, setWorkspaces] = useState<Workspace[]>([])
  const [open, setOpen] = useState(false)
  const [switching, setSwitching] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const activeWs = getSession()?.workspaceId ?? null
  const authed = getSession() !== null
  const active = workspaces.find((w) => w.workspace_id === activeWs) ?? null

  const refresh = useCallback(async () => {
    if (!getSession()) {
      setWorkspaces([])
      return
    }
    try {
      setWorkspaces(await listWorkspaces())
      setError(null)
    } catch (err) {
      const status = (err as { response?: { status?: number } })?.response?.status
      if (status === 401) setWorkspaces([])
      else setError(apiError(err, 'Failed to load workspaces.'))
    }
  }, [])

  useEffect(() => {
    void refresh()
    const onInvalid = () => setWorkspaces([])
    window.addEventListener(SESSION_INVALID_EVENT, onInvalid)
    return () => window.removeEventListener(SESSION_INVALID_EVENT, onInvalid)
  }, [refresh])

  if (!authed) return null

  const handleSwitch = async (id: string) => {
    if (id === activeWs) {
      setOpen(false)
      return
    }
    setSwitching(true)
    setError(null)
    try {
      const res = await switchWorkspace(id)
      setAccessToken(res.access_token, res.expires_in)
      setOpen(false)
      await refresh()
      // Full context refresh under the new `ws` claim (guards re-verify,
      // dashboard calls carry the new workspace header from here on).
      window.location.assign('/')
    } catch (err) {
      setError(apiError(err, 'Switch failed — workspace not found or access denied.'))
    } finally {
      setSwitching(false)
    }
  }

  return (
    <div className="relative">
      <button
        onClick={() => {
          setOpen((o) => !o)
          if (!open) void refresh()
        }}
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-label="Switch workspace"
        className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-xl bg-bg-deep text-text-primary hover:bg-bg-deep/70 transition-colors border border-border-default max-w-[220px]"
      >
        <span className="w-2 h-2 rounded-full bg-accent-cyan shrink-0" aria-hidden="true" />
        <span className="truncate font-mono">{active ? active.name : 'Select workspace'}</span>
        <span aria-hidden="true" className="text-text-muted">▾</span>
      </button>
      {open && (
        <div
          role="listbox"
          aria-label="Workspaces"
          className="absolute right-0 mt-1 w-64 rounded-lg border border-border-default bg-bg-card shadow-modal z-50 overflow-hidden"
        >
          {error && <div role="alert" className="px-3 py-2 text-[11px] font-mono text-red-400">{error}</div>}
          <div className="max-h-64 overflow-auto py-1">
            {workspaces.length === 0 && !error && (
              <div className="px-3 py-2 text-[11px] font-mono text-text-muted">No workspaces yet.</div>
            )}
            {workspaces.map((ws) => (
              <button
                key={ws.workspace_id}
                role="option"
                aria-selected={ws.workspace_id === activeWs}
                disabled={switching}
                onClick={() => void handleSwitch(ws.workspace_id)}
                className={`w-full text-left px-3 py-2 text-xs font-mono hover:bg-bg-deep transition-colors disabled:opacity-40 ${
                  ws.workspace_id === activeWs ? 'text-accent-cyan' : 'text-text-primary'
                }`}
              >
                <span className="block truncate">
                  {ws.workspace_id === activeWs ? '● ' : '○ '}{ws.name}
                </span>
                <span className="block text-[10px] text-text-muted truncate">{ws.slug}</span>
              </button>
            ))}
          </div>
          <button
            onClick={() => {
              setOpen(false)
              navigate('/workspaces')
            }}
            className="w-full px-3 py-2 text-[11px] font-mono text-accent-cyan hover:bg-bg-deep border-t border-border-default"
          >
            Manage workspaces…
          </button>
        </div>
      )}
    </div>
  )
}
