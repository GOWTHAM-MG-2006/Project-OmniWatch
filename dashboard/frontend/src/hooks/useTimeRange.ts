/**
 * OmniWatch — Dashboard Frontend
 * Component: useTimeRange hook
 * Purpose: Global timeframe picker state synced to URL ?timeRange= (1h/6h/24h/7d), default 24h
 * Inputs: URL searchParams
 * Outputs: [timeRange, setTimeRange, hours]
 */

import { useCallback, useMemo } from 'react'
import { useSearchParams } from 'react-router-dom'

export type Timeframe = '1h' | '6h' | '24h' | '7d'

const VALID: readonly Timeframe[] = ['1h', '6h', '24h', '7d'] as const
const DEFAULT: Timeframe = '24h'

const HOURS_MAP: Record<Timeframe, number> = {
  '1h': 1,
  '6h': 6,
  '24h': 24,
  '7d': 168,
}

function normalize(raw: string | null): Timeframe {
  if (!raw) return DEFAULT
  const v = raw.trim().toLowerCase()
  return (VALID as readonly string[]).includes(v) ? (v as Timeframe) : DEFAULT
}

export function useTimeRange(): {
  timeRange: Timeframe
  hours: number
  setTimeRange: (tf: Timeframe) => void
} {
  const [searchParams, setSearchParams] = useSearchParams()
  const timeRange = useMemo(() => normalize(searchParams.get('timeRange')), [searchParams])

  const hours = HOURS_MAP[timeRange]

  const setTimeRange = useCallback(
    (tf: Timeframe) => {
      const next = new URLSearchParams(searchParams)
      next.set('timeRange', tf)
      setSearchParams(next, { replace: true })
    },
    [searchParams, setSearchParams],
  )

  return { timeRange, hours, setTimeRange }
}

export function timeframeToHours(tf: string | null | undefined): number {
  return HOURS_MAP[normalize(tf ?? null)] ?? 24
}

export const TIMEFRAMES = VALID
export const DEFAULT_TIMERANGE = DEFAULT
