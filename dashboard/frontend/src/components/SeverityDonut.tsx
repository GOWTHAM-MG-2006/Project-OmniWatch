/**
 * OmniWatch — Dashboard Frontend
 * Component: SeverityDonut
 * Phase: 11
 * Purpose: Donut chart showing incident count by severity (Recharts)
 */

import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from 'recharts'

const SEVERITY_COLORS: Record<string, string> = {
  P1: '#ef4444',
  P2: '#f59e0b',
  P3: '#00d4ff',
  P4: '#a1a1aa',
}

interface SeverityDonutProps {
  data: { severity: string; cnt: number }[]
}

export function SeverityDonut({ data }: SeverityDonutProps) {
  if (!data.length) {
    return (
      <div className="h-full flex items-center justify-center text-text-muted text-sm">
        No data
      </div>
    )
  }

  const total = data.reduce((s, d) => s + (d.cnt ?? 0), 0)

  return (
    <div className="h-full flex items-center gap-4">
      <ResponsiveContainer width="60%" height="100%">
        <PieChart>
          <Pie
            data={data}
            dataKey="cnt"
            nameKey="severity"
            cx="50%"
            cy="50%"
            innerRadius="55%"
            outerRadius="80%"
            strokeWidth={0}
          >
            {data.map((entry) => (
              <Cell key={entry.severity} fill={SEVERITY_COLORS[entry.severity] ?? '#6b7280'} />
            ))}
          </Pie>
          <Tooltip
            contentStyle={{ background: '#1a1a1a', border: '1px solid #2a2a2a', borderRadius: 8, fontSize: 12 }}
            labelStyle={{ color: '#a1a1aa' }}
            formatter={(value: number) => [`${value} (${((value / (total || 1)) * 100).toFixed(0)}%)`, 'Count']}
          />
        </PieChart>
      </ResponsiveContainer>

      <div className="flex flex-col gap-1 text-xs">
        {data.map((d) => (
          <div key={d.severity} className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full" style={{ background: SEVERITY_COLORS[d.severity] ?? '#6b7280' }} />
            <span className="text-text-muted">{d.severity}</span>
            <span className="text-text-primary font-mono">{d.cnt}</span>
          </div>
        ))}
        <div className="border-t border-border-default pt-1 mt-1 text-text-muted">
          Total: <span className="text-text-primary font-mono">{Number.isFinite(total) ? total : 0}</span>
        </div>
      </div>
    </div>
  )
}
