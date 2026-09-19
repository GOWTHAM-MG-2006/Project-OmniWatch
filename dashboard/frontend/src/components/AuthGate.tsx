import { useState, useEffect } from 'react'
import { clickhouseTables, neo4jSchema, fetchMinioBuckets } from '../api/client'

interface AuthGateProps {
  service: 'clickhouse' | 'neo4j' | 'minio'
  onAuth: () => void
  children: React.ReactNode
}

function clearServiceCreds(service: string): void {
  if (service === 'clickhouse') {
    sessionStorage.removeItem('ch_user')
    sessionStorage.removeItem('ch_password')
  } else if (service === 'neo4j') {
    sessionStorage.removeItem('neo4j_user')
    sessionStorage.removeItem('neo4j_password')
  } else if (service === 'minio') {
    sessionStorage.removeItem('minio_access_key')
    sessionStorage.removeItem('minio_secret_key')
  }
  sessionStorage.removeItem(`${service}_authenticated`)
}

// Verify credentials against the backend with a real downstream handshake.
// Resolves on a verified 200, rejects otherwise — the gate unlocks ONLY here.
async function verifyService(service: string): Promise<void> {
  if (service === 'clickhouse') {
    await clickhouseTables()
  } else if (service === 'neo4j') {
    await neo4jSchema()
  } else {
    const res = await fetchMinioBuckets()
    if (res.error) throw new Error(res.error)
  }
}

export function AuthGate({ service, onAuth, children }: AuthGateProps) {
  const [authenticated, setAuthenticated] = useState(false)
  const [showForm, setShowForm] = useState(true)
  const [verifying, setVerifying] = useState(false)
  const [authError, setAuthError] = useState<string | null>(null)

  useEffect(() => {
    // Clear sessionStorage on browser close
    const handleBeforeUnload = () => {
      if (service === 'clickhouse') {
        sessionStorage.removeItem('ch_user')
        sessionStorage.removeItem('ch_password')
      } else if (service === 'neo4j') {
        sessionStorage.removeItem('neo4j_user')
        sessionStorage.removeItem('neo4j_password')
      } else if (service === 'minio') {
        sessionStorage.removeItem('minio_access_key')
        sessionStorage.removeItem('minio_secret_key')
      }
      sessionStorage.removeItem(`${service}_authenticated`)
    }
    window.addEventListener('beforeunload', handleBeforeUnload)
    return () => window.removeEventListener('beforeunload', handleBeforeUnload)
  }, [service])

  // Revalidate any cached flag with a real handshake — a stale flag from
  // an earlier session never unlocks the page on its own.
  useEffect(() => {
    const stored = sessionStorage.getItem(`${service}_authenticated`)
    if (stored !== 'true') return
    let cancelled = false
    setVerifying(true)
    verifyService(service).then(() => {
      if (cancelled) return
      setAuthenticated(true)
      setShowForm(false)
      onAuth()
    }).catch(() => {
      if (cancelled) return
      clearServiceCreds(service)
      setAuthenticated(false)
      setShowForm(true)
    }).finally(() => {
      if (!cancelled) setVerifying(false)
    })
    return () => { cancelled = true }
  }, [service, onAuth])

  // A 401 on this service's endpoints (axios interceptor) re-locks the gate.
  useEffect(() => {
    const onInvalid = (ev: Event) => {
      const forService = (ev as CustomEvent<{ service?: string }>).detail?.service
      if (forService && forService !== service) return
      setAuthenticated(false)
      setShowForm(true)
      setAuthError('Credentials rejected by the server (401) — please reconnect.')
    }
    window.addEventListener('omniwatch:auth-invalid', onInvalid)
    return () => window.removeEventListener('omniwatch:auth-invalid', onInvalid)
  }, [service])

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    const formData = new FormData(e.currentTarget)

    if (service === 'clickhouse') {
      sessionStorage.setItem('ch_user', formData.get('user') as string)
      sessionStorage.setItem('ch_password', formData.get('password') as string)
    } else if (service === 'neo4j') {
      sessionStorage.setItem('neo4j_user', formData.get('user') as string)
      sessionStorage.setItem('neo4j_password', formData.get('password') as string)
    } else if (service === 'minio') {
      sessionStorage.setItem('minio_access_key', formData.get('access_key') as string)
      sessionStorage.setItem('minio_secret_key', formData.get('secret_key') as string)
    }

    // Stash first, verify, then commit the flag ONLY on a verified 200.
    // Wrong/empty creds stay locked with the server's message shown.
    setVerifying(true)
    setAuthError(null)
    try {
      await verifyService(service)
      sessionStorage.setItem(`${service}_authenticated`, 'true')
      setAuthenticated(true)
      setShowForm(false)
      onAuth()
    } catch (err) {
      clearServiceCreds(service)
      setAuthenticated(false)
      setShowForm(true)
      const msg = (err as { response?: { data?: { error?: string } }; message?: string })
      setAuthError(msg?.response?.data?.error || msg?.message || 'Authentication failed — check credentials.')
    } finally {
      setVerifying(false)
    }
  }

  const handleLogout = () => {
    clearServiceCreds(service)
    setAuthenticated(false)
    setShowForm(true)
    setAuthError(null)
  }

  if (authenticated) {
    return (
      <div>
        <div className="flex items-center justify-between mb-4">
          <div className="text-xs text-text-muted font-mono">
            ✓ {service.toUpperCase()} authenticated
          </div>
          <button
            onClick={handleLogout}
            className="text-xs text-red-400 hover:text-red-300 font-mono"
          >
            Logout
          </button>
        </div>
        {children}
      </div>
    )
  }

  if (!showForm) return null

  return (
    <div className="flex items-center justify-center min-h-[400px]">
      <div className="card p-6 w-full max-w-md rounded-lg border border-[#2a2a2a]">
        <h2 className="text-lg font-semibold text-text-primary mb-4">
          {service.toUpperCase()} Authentication
        </h2>
        {authError && (
          <div className="mb-4 px-3 py-2 text-xs font-mono rounded bg-red-900/20 border border-red-900/40 text-red-400">
            {authError}
          </div>
        )}
        <form onSubmit={handleSubmit} className="space-y-4">
          {service === 'clickhouse' && (
            <>
              <div>
                <label className="block text-xs text-text-muted mb-1">User</label>
                <input
                  name="user"
                  defaultValue="omniwatch"
                  className="w-full px-3 py-2 text-sm bg-[#0a0a0f] border border-[#2a2a2a] rounded font-mono"
                  required
                />
              </div>
              <div>
                <label className="block text-xs text-text-muted mb-1">Password</label>
                <input
                  name="password"
                  type="password"
                  defaultValue="omniwatch"
                  className="w-full px-3 py-2 text-sm bg-[#0a0a0f] border border-[#2a2a2a] rounded font-mono"
                />
              </div>
            </>
          )}
          {service === 'neo4j' && (
            <>
              <div>
                <label className="block text-xs text-text-muted mb-1">User</label>
                <input
                  name="user"
                  defaultValue="neo4j"
                  className="w-full px-3 py-2 text-sm bg-[#0a0a0f] border border-[#2a2a2a] rounded font-mono"
                  required
                />
              </div>
              <div>
                <label className="block text-xs text-text-muted mb-1">Password</label>
                <input
                  name="password"
                  type="password"
                  defaultValue="omniwatch"
                  className="w-full px-3 py-2 text-sm bg-[#0a0a0f] border border-[#2a2a2a] rounded font-mono"
                />
              </div>
            </>
          )}
          {service === 'minio' && (
            <>
              <div>
                <label className="block text-xs text-text-muted mb-1">Access Key</label>
                <input
                  name="access_key"
                  defaultValue="omniwatch"
                  className="w-full px-3 py-2 text-sm bg-[#0a0a0f] border border-[#2a2a2a] rounded font-mono"
                  required
                />
              </div>
              <div>
                <label className="block text-xs text-text-muted mb-1">Secret Key</label>
                <input
                  name="secret_key"
                  type="password"
                  defaultValue="omniwatch"
                  className="w-full px-3 py-2 text-sm bg-[#0a0a0f] border border-[#2a2a2a] rounded font-mono"
                />
              </div>
            </>
          )}
          <button
            type="submit"
            disabled={verifying}
            className="w-full py-2 text-sm font-mono rounded bg-accent-cyan/20 hover:bg-accent-cyan/30 text-accent-cyan border border-accent-cyan/30 disabled:opacity-40"
          >
            {verifying ? 'Verifying…' : 'Connect'}
          </button>
        </form>
      </div>
    </div>
  )
}
