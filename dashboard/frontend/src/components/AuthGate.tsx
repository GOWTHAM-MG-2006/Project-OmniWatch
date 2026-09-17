import { useState, useEffect } from 'react'

interface AuthGateProps {
  service: 'clickhouse' | 'neo4j' | 'minio'
  onAuth: () => void
  children: React.ReactNode
}

export function AuthGate({ service, onAuth, children }: AuthGateProps) {
  const [authenticated, setAuthenticated] = useState(false)
  const [showForm, setShowForm] = useState(true)

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

  useEffect(() => {
    const stored = sessionStorage.getItem(`${service}_authenticated`)
    if (stored === 'true') {
      setAuthenticated(true)
      setShowForm(false)
      onAuth()
    }
  }, [service, onAuth])

  const handleSubmit = (e: React.FormEvent<HTMLFormElement>) => {
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
    
    sessionStorage.setItem(`${service}_authenticated`, 'true')
    setAuthenticated(true)
    setShowForm(false)
    onAuth()
  }

  const handleLogout = () => {
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
    setAuthenticated(false)
    setShowForm(true)
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
                  defaultValue="minioadmin"
                  className="w-full px-3 py-2 text-sm bg-[#0a0a0f] border border-[#2a2a2a] rounded font-mono"
                  required
                />
              </div>
              <div>
                <label className="block text-xs text-text-muted mb-1">Secret Key</label>
                <input
                  name="secret_key"
                  type="password"
                  defaultValue="minioadmin"
                  className="w-full px-3 py-2 text-sm bg-[#0a0a0f] border border-[#2a2a2a] rounded font-mono"
                />
              </div>
            </>
          )}
          <button
            type="submit"
            className="w-full py-2 text-sm font-mono rounded bg-accent-cyan/20 hover:bg-accent-cyan/30 text-accent-cyan border border-accent-cyan/30"
          >
            Connect
          </button>
        </form>
      </div>
    </div>
  )
}
