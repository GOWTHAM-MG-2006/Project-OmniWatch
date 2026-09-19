import { useState } from 'react'
import type { FormEvent } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { apiError, identityLogin, identityRegister } from '../api/client'
import { setSession } from '../auth/session'

const inputClass =
  'w-full px-3 py-2 text-sm bg-[#0a0a0f] border border-[#2a2a2a] rounded font-mono text-text-primary focus:outline-none focus:border-accent-cyan/60'

export function Register() {
  const navigate = useNavigate()
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleSubmit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    const form = new FormData(e.currentTarget)
    const email = String(form.get('email') ?? '').trim()
    // Password lives only in this handler's scope — never persisted;
    // dropped after register+auto-login complete.
    const password = String(form.get('password') ?? '')
    const confirm = String(form.get('confirm') ?? '')
    if (!email || !password) {
      setError('Email and password are required.')
      return
    }
    if (password.length < 10) {
      setError('Password must be at least 10 characters (server enforces 422).')
      return
    }
    if (password !== confirm) {
      setError('Passwords do not match.')
      return
    }
    setSubmitting(true)
    setError(null)
    try {
      await identityRegister(email, password)
      // Auto-login: fresh accounts land authenticated (empty workspaces).
      const pair = await identityLogin(email, password)
      setSession({
        accessToken: pair.access_token,
        refreshToken: pair.refresh_token,
        expiresIn: pair.expires_in,
        email,
      })
      navigate('/workspaces', { replace: true })
    } catch (err) {
      setError(apiError(err, 'Registration failed.'))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="flex items-center justify-center min-h-full py-10">
      <div className="card p-6 w-full max-w-md rounded-lg border border-[#2a2a2a]">
        <h1 className="font-heading text-accent-cyan font-bold text-lg tracking-tight">OmniWatch</h1>
        <h2 className="text-sm font-semibold text-text-primary mt-1 mb-4">Create account</h2>
        {error && (
          <div role="alert" className="mb-4 px-3 py-2 text-xs font-mono rounded bg-red-900/20 border border-red-900/40 text-red-400">
            {error}
          </div>
        )}
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label htmlFor="register-email" className="block text-xs text-text-muted mb-1">Email</label>
            <input
              id="register-email"
              name="email"
              type="email"
              autoComplete="username"
              required
              className={inputClass}
            />
          </div>
          <div>
            <label htmlFor="register-password" className="block text-xs text-text-muted mb-1">
              Password (min 10 chars)
            </label>
            <input
              id="register-password"
              name="password"
              type="password"
              autoComplete="new-password"
              required
              minLength={10}
              className={inputClass}
            />
          </div>
          <div>
            <label htmlFor="register-confirm" className="block text-xs text-text-muted mb-1">Confirm password</label>
            <input
              id="register-confirm"
              name="confirm"
              type="password"
              autoComplete="new-password"
              required
              className={inputClass}
            />
          </div>
          <button
            type="submit"
            disabled={submitting}
            className="w-full py-2 text-sm font-mono rounded bg-accent-cyan/20 hover:bg-accent-cyan/30 text-accent-cyan border border-accent-cyan/30 disabled:opacity-40"
          >
            {submitting ? 'Creating…' : 'Register'}
          </button>
        </form>
        <p className="mt-4 text-xs text-text-muted font-mono">
          Have an account? <Link to="/login" className="text-accent-cyan hover:underline">Log in</Link>
        </p>
      </div>
    </div>
  )
}
