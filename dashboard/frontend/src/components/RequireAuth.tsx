import { useEffect, useState } from 'react'
import type { ReactNode } from 'react'
import { Navigate, useLocation } from 'react-router-dom'
import { getSession, SESSION_INVALID_EVENT } from '../auth/session'
import { identityMe } from '../api/client'
import { setSessionEmail } from '../auth/session'

type GuardState = 'checking' | 'anonymous' | 'no-workspace' | 'ready'

/**
 * Route guard with a verified handshake (DASH-auth discipline): a cached
 * token alone never unlocks a route — GET /auth/me must return 200 first.
 * Any 401 (expired/tampered/mismatched) clears the session (interceptor)
 * and drops the guard back to anonymous, re-locking the page.
 */
export function useSessionGuard(): GuardState {
  const [state, setState] = useState<GuardState>(() => (getSession() ? 'checking' : 'anonymous'))

  useEffect(() => {
    let cancelled = false
    const verify = async () => {
      if (!getSession()) {
        if (!cancelled) setState('anonymous')
        return
      }
      if (!cancelled) setState('checking')
      try {
        const me = await identityMe()
        if (cancelled) return
        setSessionEmail(me.email)
        const session = getSession()
        setState(session?.workspaceId ? 'ready' : 'no-workspace')
      } catch {
        if (!cancelled) setState('anonymous')
      }
    }
    void verify()
    const onInvalid = () => {
      if (!cancelled) setState('anonymous')
    }
    window.addEventListener(SESSION_INVALID_EVENT, onInvalid)
    return () => {
      cancelled = true
      window.removeEventListener(SESSION_INVALID_EVENT, onInvalid)
    }
  }, [])

  return state
}

export function GuardLoading() {
  return (
    <div className="flex items-center justify-center min-h-[400px]">
      <div className="text-xs text-text-muted font-mono animate-pulse">Verifying session…</div>
    </div>
  )
}

/** Requires an authenticated session (any workspace state). */
export function RequireAuth({ children }: { children: ReactNode }) {
  const state = useSessionGuard()
  const location = useLocation()
  if (state === 'checking') return <GuardLoading />
  if (state === 'anonymous') return <Navigate to="/login" replace state={{ from: location.pathname }} />
  return <>{children}</>
}

/** Requires an authenticated session WITH an active workspace (`ws` claim). */
export function RequireWorkspace({ children }: { children: ReactNode }) {
  const state = useSessionGuard()
  const location = useLocation()
  if (state === 'checking') return <GuardLoading />
  if (state === 'anonymous') return <Navigate to="/login" replace state={{ from: location.pathname }} />
  if (state === 'no-workspace') return <Navigate to="/workspaces" replace />
  return <>{children}</>
}

/**
 * Landing redirect: `/` → `/login` unauthenticated, → `/workspaces`
 * authenticated without `ws`, → dashboard (children) when ws active.
 */
export function LandingRoute({ children }: { children: ReactNode }) {
  const state = useSessionGuard()
  if (state === 'checking') return <GuardLoading />
  if (state === 'anonymous') return <Navigate to="/login" replace />
  if (state === 'no-workspace') return <Navigate to="/workspaces" replace />
  return <>{children}</>
}
