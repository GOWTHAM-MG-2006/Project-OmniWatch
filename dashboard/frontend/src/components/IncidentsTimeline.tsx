/**
 * OmniWatch — Dashboard Frontend
 * Component: IncidentsTimeline
 * Phase: 11
 * Purpose: Stacked area chart showing incidents per hour by severity (Recharts)
 */

import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'

interface TimelinePoint {
  hour: string
  incident_count: number
  severity: string
}

interface IncidentsTimelineProps {
  data: TimelinePoint[]
}

/** Transform API array-of-objects into grouped-by-hour format for stacked chart */
function groupByHour(raw: TimelinePoint[]) {
  const map = new Map<string, Record<string, unknown>>()
  for (const pt of raw) {
    const label = pt.hour.length > 13 ? pt.hour.slice(11, 16) : pt.hour
    if (!map.has(pt.hour)) {
      map.set(pt.hour, { hour: label, P1: 0, P2: 0, P3: 0, P4: 0 })
    }
    const bucket = map.get(pt.hour)!
    bucket[pt.severity] = (bucket[pt.severity] as number) + pt.incident_count
  }
  return Array.from(map.values())
}

export function IncidentsTimeline({ data }: IncidentsTimelineProps) {
  if (!data.length) {
    return (
      <div className="h-full flex items-center justify-center text-text-muted text-sm">
        No timeline data
      </div>
    )
  }

  const grouped = groupByHour(data)

  return (
    <ResponsiveContainer width="100%" height="100%">
      <AreaChart data={grouped} margin={{ top: 4, right: 4, left: -12, bottom: 0 }}>
        <defs>
          <linearGradient id="gP1" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="#ef4444" stopOpacity={0.3} />
            <stop offset="95%" stopColor="#ef4444" stopOpacity={0} />
          </linearGradient>
          <linearGradient id="gP2" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="#f59e0b" stopOpacity={0.3} />
            <stop offset="95%" stopColor="#f59e0b" stopOpacity={0} />
          </linearGradient>
          <linearGradient id="gP3" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="#00d4ff" stopOpacity={0.3} />
            <stop offset="95%" stopColor="#00d4ff" stopOpacity={0} />
          </linearGradient>
          <linearGradient id="gP4" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="#a1a1aa" stopOpacity={0.3} />
            <stop offset="95%" stopColor="#a1a1aa" stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="#2a2a2a" />
        <XAxis dataKey="hour" tick={{ fill: '#a1a1aa', fontSize: 10 }} axisLine={{ stroke: '#2a2a2a' }} />
        <YAxis tick={{ fill: '#a1a1aa', fontSize: 10 }} axisLine={{ stroke: '#2a2a2a' }} allowDecimals={false} />
        <Tooltip
          contentStyle={{ background: '#1a1a1a', border: '1px solid #2a2a2a', borderRadius: 8, fontSize: 12 }}
          labelStyle={{ color: '#a1a1aa' }}
        />
        <Area type="monotone" dataKey="P1" stackId="1" stroke="#ef4444" fill="url(#gP1)" />
        <Area type="monotone" dataKey="P2" stackId="1" stroke="#f59e0b" fill="url(#gP2)" />
        <Area type="monotone" dataKey="P3" stackId="1" stroke="#00d4ff" fill="url(#gP3)" />
        <Area type="monotone" dataKey="P4" stackId="1" stroke="#a1a1aa" fill="url(#gP4)" />
      </AreaChart>
    </ResponsiveContainer>
  )
}
