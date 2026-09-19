import { useState } from 'react'
import type { FormEvent } from 'react'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import { apiError, identityLogin, identityMe } from '../api/client'
import { getSession, setSession } from '../auth/session'

const inputClass =
  'w-full px-3 py-2 text-sm bg-[#0a0a0f] border border-[#2a2a2a] rounded font-mono text-text-primary focus:outline-none focus:border-accent-cyan/60'

export function Login() {
  const navigate = useNavigate()
  const location = useLocation()
  const [email, setEmail] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleSubmit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    const form = new FormData(e.currentTarget)
    // Password lives only in this handler's scope — never persisted
    // (no sessionStorage/localStorage write); dropped after the call.
    const password = String(form.get('password') ?? '')
    const emailValue = String(form.get('email') ?? '').trim()
    if (!emailValue || !password) {
      setError('Email and password are required.')
      return
    }
    setSubmitting(true)
    setError(null)
    try {
      const pair = await identityLogin(emailValue, password)
      setSession({
        accessToken: pair.access_token,
        refreshToken: pair.refresh_token,
        expiresIn: pair.expires_in,
        email: emailValue,
      })
      // Verified handshake before routing: the session counts only after
      // GET /auth/me returns 200 (DASH-auth flag-only-on-200 discipline).
      const me = await identityMe()
      setEmail(me.email)
      const from = (location.state as { from?: string } | null)?.from
      if (from && from !== '/login' && from !== '/register') {
        navigate(from, { replace: true })
      } else {
        navigate(getSession()?.workspaceId ? '/' : '/workspaces', { replace: true })
      }
    } catch (err) {
      // Wrong creds stay locked on this page with the server message.
      setError(apiError(err, 'Login failed — check email and password.'))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="flex items-center justify-center min-h-full py-10">
      <div className="card p-6 w-full max-w-md rounded-lg border border-[#2a2a2a]">
        <h1 className="font-heading text-accent-cyan font-bold text-lg tracking-tight">OmniWatch</h1>
        <h2 className="text-sm font-semibold text-text-primary mt-1 mb-4">Log in</h2>
        {error && (
          <div role="alert" className="mb-4 px-3 py-2 text-xs font-mono rounded bg-red-900/20 border border-red-900/40 text-red-400">
            {error}
          </div>
        )}
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label htmlFor="login-email" className="block text-xs text-text-muted mb-1">Email</label>
            <input
              id="login-email"
              name="email"
              type="email"
              autoComplete="username"
              required
              defaultValue={email}
              onChange={(e) => setEmail(e.target.value)}
              className={inputClass}
            />
          </div>
          <div>
            <label htmlFor="login-password" className="block text-xs text-text-muted mb-1">Password</label>
            <input
              id="login-password"
              name="password"
              type="password"
              autoComplete="current-password"
              required
              className={inputClass}
            />
          </div>
          <button
            type="submit"
            disabled={submitting}
            className="w-full py-2 text-sm font-mono rounded bg-accent-cyan/20 hover:bg-accent-cyan/30 text-accent-cyan border border-accent-cyan/30 disabled:opacity-40"
          >
            {submitting ? 'Verifying…' : 'Log in'}
          </button>
        </form>
        <p className="mt-4 text-xs text-text-muted font-mono">
          No account? <Link to="/register" className="text-accent-cyan hover:underline">Register</Link>
        </p>
      </div>
    </div>
  )
}
