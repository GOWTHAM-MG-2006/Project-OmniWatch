/**
 * OmniWatch — Dashboard Frontend
 * Component: Session store (ENTRY-4)
 * Phase: entry-point
 * Purpose: JWT session holder for identity auth (register/login/refresh).
 * Inputs: TokenResponse from identity/auth.py (/auth/login, /auth/refresh,
 *          /workspaces/{id}/switch re-issues access with the `ws` claim)
 * Outputs: Authorization Bearer header value + active workspace id for
 *          api/client.ts interceptors; session-invalid events for re-lock.
 *
 * JWT STORAGE CHOICE (documented per plan todo 4):
 * - Source of truth is IN-MEMORY (module-level `let`). A page reload loses
 *   the in-memory copy, so it is mirrored to `sessionStorage` (per-tab store
 *   cleared automatically when the tab closes — NOT `localStorage`, which
 *   persists across sessions and widens the theft window).
 * - Why not httpOnly cookies: the identity service issues JWTs in a JSON
 *   response body (identity/auth.py TokenResponse) and has no Set-Cookie /
 *   SameSite / CSRF path; the SPA talks to it cross-origin in dev
 *   (vite :5173 -> identity :8012). Adopting httpOnly cookies would require
 *   a backend cookie-issuance change — out of scope for todo 4 (frontend
 *   only; backend untouched). Recorded here so a later hardening pass can
 *   revisit it without re-auditing the tradeoff.
 * - XSS mitigations for the sessionStorage mirror: (1) short-lived access
 *   tokens (OMNIWATCH_JWT_TTL_S default 3600) with refresh rotation
 *   (replay -> 401); (2) tokens are NEVER written to URLs, logs, or
 *   console output; (3) raw passwords are NEVER persisted — the password
 *   string lives only in the submit handler's local state and is dropped
 *   after the login/register call resolves; (4) any 401 on an
 *   authenticated call clears the session and re-locks the UI (no silent
 *   data mixing, no stale-`ws` leakage); (5) explicit logout clears both
 *   the memory copy and the mirror and revokes the refresh token
 *   server-side via POST /auth/logout.
 */

export interface SessionState {
  accessToken: string
  refreshToken: string
  expiresIn: number
  /** Active workspace id = JWT `ws` claim (set on switch; null until then). */
  workspaceId: string | null
  email: string | null
}

// In-memory source of truth. Null until login/register/refresh/switch.
let memorySession: SessionState | null = null

const ACCESS_KEY = 'omniwatch_access_token'
const REFRESH_KEY = 'omniwatch_refresh_token'
const EXPIRES_KEY = 'omniwatch_token_expires_in'
const WS_KEY = 'omniwatch_active_workspace'
const EMAIL_KEY = 'omniwatch_user_email'

export const SESSION_INVALID_EVENT = 'omniwatch:session-invalid'

export function notifySessionInvalid(): void {
  window.dispatchEvent(new Event(SESSION_INVALID_EVENT))
}

/** Decode the JWT payload (no verification — display/redirect use only). */
export function decodeJwtPayload(token: string): Record<string, unknown> | null {
  try {
    const parts = token.split('.')
    if (parts.length !== 3) return null
    const b64 = parts[1].replace(/-/g, '+').replace(/_/g, '/')
    return JSON.parse(atob(b64)) as Record<string, unknown>
  } catch {
    return null
  }
}

function wsFromToken(accessToken: string): string | null {
  const claims = decodeJwtPayload(accessToken)
  const ws = claims?.['ws']
  return typeof ws === 'string' && ws.length > 0 ? ws : null
}

/** Persist a session: memory first, sessionStorage mirror second. */
export function setSession(input: {
  accessToken: string
  refreshToken: string
  expiresIn: number
  email?: string | null
}): SessionState {
  const state: SessionState = {
    accessToken: input.accessToken,
    refreshToken: input.refreshToken,
    expiresIn: input.expiresIn,
    workspaceId: wsFromToken(input.accessToken),
    email: input.email ?? memorySession?.email ?? null,
  }
  memorySession = state
  try {
    sessionStorage.setItem(ACCESS_KEY, state.accessToken)
    sessionStorage.setItem(REFRESH_KEY, state.refreshToken)
    sessionStorage.setItem(EXPIRES_KEY, String(state.expiresIn))
    if (state.workspaceId) sessionStorage.setItem(WS_KEY, state.workspaceId)
    else sessionStorage.removeItem(WS_KEY)
    if (state.email) sessionStorage.setItem(EMAIL_KEY, state.email)
  } catch {
    // sessionStorage unavailable (private mode) — memory still authoritative.
  }
  return state
}

/** Replace only the access token (workspace switch re-issues `ws`). */
export function setAccessToken(accessToken: string, expiresIn?: number): SessionState | null {
  if (!memorySession && !sessionStorage.getItem(ACCESS_KEY)) return null
  const current = getSession()
  if (!current) return null
  return setSession({
    accessToken,
    refreshToken: current.refreshToken,
    expiresIn: expiresIn ?? current.expiresIn,
    email: current.email,
  })
}

export function setSessionEmail(email: string): void {
  if (memorySession) memorySession.email = email
  try {
    sessionStorage.setItem(EMAIL_KEY, email)
  } catch {
    // ignore — memory is authoritative
  }
}

/** Read the session (memory first, sessionStorage mirror as fallback). */
export function getSession(): SessionState | null {
  if (memorySession) return memorySession
  try {
    const accessToken = sessionStorage.getItem(ACCESS_KEY)
    const refreshToken = sessionStorage.getItem(REFRESH_KEY)
    if (!accessToken || !refreshToken) return null
    memorySession = {
      accessToken,
      refreshToken,
      expiresIn: Number(sessionStorage.getItem(EXPIRES_KEY) ?? '3600'),
      workspaceId: sessionStorage.getItem(WS_KEY) ?? wsFromToken(accessToken),
      email: sessionStorage.getItem(EMAIL_KEY),
    }
    return memorySession
  } catch {
    return memorySession
  }
}

/** Clear memory + mirror (logout, 401 re-lock). Never throws. */
export function clearSession(): void {
  memorySession = null
  try {
    sessionStorage.removeItem(ACCESS_KEY)
    sessionStorage.removeItem(REFRESH_KEY)
    sessionStorage.removeItem(EXPIRES_KEY)
    sessionStorage.removeItem(WS_KEY)
    sessionStorage.removeItem(EMAIL_KEY)
  } catch {
    // ignore
  }
}

export function isAuthenticated(): boolean {
  return getSession() !== null
}

/** True when the JWT carries an active workspace (`ws` claim). */
export function hasActiveWorkspace(): boolean {
  return getSession()?.workspaceId !== null && getSession()?.workspaceId !== undefined
}
