/**
 * OmniWatch — Dashboard Frontend
 * Component: KpiCard
 * Phase: 11
 * Purpose: Single KPI tile for the overview dashboard
 */

interface KpiCardProps {
  label: string
  value: string | number
  delta?: number
  deltaLabel?: string
}

export function KpiCard({ label, value, delta, deltaLabel }: KpiCardProps) {
  return (
    <div className="card p-4 flex flex-col justify-between">
      <div className="text-text-muted text-[10px] uppercase tracking-widest">{label}</div>
      <div className="font-heading text-3xl mt-1 text-text-primary">{value}</div>
      {delta !== undefined && (
        <div className={`text-xs mt-1 ${delta >= 0 ? 'text-status-warning' : 'text-status-healthy'}`}>
          {delta >= 0 ? '↑' : '↓'} {Math.abs(delta)} {deltaLabel ?? ''}
        </div>
      )}
    </div>
  )
}
