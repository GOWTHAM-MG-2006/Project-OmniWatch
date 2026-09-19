/**
 * OmniWatch — Dashboard Frontend
 * Component: useFetch hook
 * Phase: 11
 * Purpose: Generic data-fetching hook with loading/error/data states + auto-refresh
 */

import { useState, useEffect, useCallback, useRef } from 'react'
import { SESSION_INVALID_EVENT } from '../auth/session'

interface UseFetchResult<T> {
  data: T | null
  loading: boolean
  error: string | null
  refetch: () => void
}

export function useFetch<T>(fetcher: () => Promise<T>, deps: unknown[] = []): UseFetchResult<T> {
  const [data, setData] = useState<T | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const mountedRef = useRef(true)

  const doFetch = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const result = await fetcher()
      if (mountedRef.current) {
        setData(result)
      }
    } catch (err) {
      if (mountedRef.current) {
        setError(err instanceof Error ? err.message : 'Unknown error')
      }
    } finally {
      if (mountedRef.current) {
        setLoading(false)
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps)

  useEffect(() => {
    mountedRef.current = true
    doFetch()
    return () => { mountedRef.current = false }
  }, [doFetch])

  return { data, loading, error, refetch: doFetch }
}

/**
 * Session-aware fetch: identical states to useFetch, plus re-lock on 401.
 * Any `omniwatch:session-invalid` event (fired by the api/client.ts
 * interceptors on an authenticated 401) flips this hook into a locked
 * error state so the page stops rendering session data — the same
 * verify-then-flag discipline as the DB AuthGates, extended to the JWT
 * session. The authenticated flag is set ONLY after a successful fetch.
 */
export function useSessionFetch<T>(
  fetcher: () => Promise<T>,
  deps: unknown[] = [],
): UseFetchResult<T> & { locked: boolean } {
  const [locked, setLocked] = useState(false)
  const inner = useFetch<T>(async () => {
    try {
      const result = await fetcher()
      setLocked(false)
      return result
    } catch (err) {
      const status = (err as { response?: { status?: number } })?.response?.status
      if (status === 401) setLocked(true)
      throw err
    }
  }, deps)

  useEffect(() => {
    const onInvalid = () => setLocked(true)
    window.addEventListener(SESSION_INVALID_EVENT, onInvalid)
    return () => window.removeEventListener(SESSION_INVALID_EVENT, onInvalid)
  }, [])

  return { ...inner, locked }
}
