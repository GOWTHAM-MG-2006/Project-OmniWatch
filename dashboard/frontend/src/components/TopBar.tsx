import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useTimeRange, TIMEFRAMES, type Timeframe } from '../hooks/useTimeRange'
import { CopilotDrawer } from './CopilotDrawer'
import { WorkspaceSwitcher } from './WorkspaceSwitcher'
import { clearSession, getSession, SESSION_INVALID_EVENT } from '../auth/session'
import { identityLogout } from '../api/client'
import { useEffect } from 'react'

export function TopBar() {
  const { timeRange, setTimeRange } = useTimeRange()
  const [copilotOpen, setCopilotOpen] = useState(false)
  const [email, setEmail] = useState<string | null>(() => getSession()?.email ?? null)
  const navigate = useNavigate()

  useEffect(() => {
    const sync = () => setEmail(getSession()?.email ?? null)
    sync()
    window.addEventListener(SESSION_INVALID_EVENT, sync)
    return () => window.removeEventListener(SESSION_INVALID_EVENT, sync)
  }, [])

  const handleLogout = async () => {
    const refreshToken = getSession()?.refreshToken
    clearSession()
    window.dispatchEvent(new Event(SESSION_INVALID_EVENT))
    navigate('/login', { replace: true })
    // Best-effort server revocation (fire-and-forget after local clear so
    // a failed call can never leave the UI logged in).
    if (refreshToken) {
      try {
        await identityLogout(refreshToken)
      } catch {
        // already logged out locally — nothing to show
      }
    }
  }

  const authed = email !== null || getSession() !== null

  return (
    <>
      <header className="h-12 bg-bg-card border-b border-border-default flex items-center px-4 gap-4 shrink-0">
        {/* Logo */}
        <div className="font-heading text-accent-cyan font-bold text-lg tracking-tight">
          OmniWatch
        </div>

        {/* Timeframe Picker — writes to URL ?timeRange= */}
        <div className="flex items-center gap-1 ml-4" role="group" aria-label="Timeframe">
          {TIMEFRAMES.map((tf: Timeframe) => (
            <button
              key={tf}
              onClick={() => setTimeRange(tf)}
              aria-pressed={timeRange === tf}
              className={`px-2 py-1 text-xs rounded transition-colors ${
                timeRange === tf
                  ? 'bg-accent-cyan/20 text-accent-cyan'
                  : 'text-text-muted hover:text-text-primary hover:bg-bg-deep'
              }`}
            >
              {tf}
            </button>
          ))}
        </div>

        {/* Spacer */}
        <div className="flex-1" />

        {/* Workspace switcher (ENTRY-4) */}
        {authed && <WorkspaceSwitcher />}

        {/* Session identity + logout */}
        {authed && (
          <div className="flex items-center gap-2">
            {email && <span className="text-[11px] font-mono text-text-muted truncate max-w-[180px]">{email}</span>}
            <button
              onClick={() => void handleLogout()}
              className="px-2 py-1.5 text-xs font-mono text-red-400 hover:text-red-300"
            >
              Logout
            </button>
          </div>
        )}

        {/* Copilot Button */}
        <button
          onClick={() => setCopilotOpen(true)}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-xl bg-accent-cyan/10 text-accent-cyan hover:bg-accent-cyan/20 transition-colors"
        >
          <svg
            xmlns="http://www.w3.org/2000/svg"
            width="14"
            height="14"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
          </svg>
          Copilot
        </button>
      </header>
      <CopilotDrawer open={copilotOpen} onClose={() => setCopilotOpen(false)} />
    </>
  )
}
