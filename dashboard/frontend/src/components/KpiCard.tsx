/**
 * OmniWatch — Dashboard Frontend
 * Component: KpiCard
 * Phase: 11
 * Purpose: Single KPI tile for the overview dashboard — Stitch design polished
 */

interface KpiCardProps {
  label: string
  value: string | number
  delta?: number
  deltaLabel?: string
  color?: 'cyan' | 'violet' | 'green' | 'red'
}

const COLOR_MAP = {
  cyan: {
    value: 'text-[#00d4ff]',
    glow: '0 0 20px rgba(0, 212, 255, 0.15)',
    border: 'border-[rgba(0,212,255,0.2)]',
  },
  violet: {
    value: 'text-[#d2bbff]',
    glow: '0 0 20px rgba(124, 58, 237, 0.15)',
    border: 'border-[rgba(124,58,237,0.2)]',
  },
  green: {
    value: 'text-[#22c55e]',
    glow: '0 0 20px rgba(34, 197, 94, 0.15)',
    border: 'border-[rgba(34,197,94,0.2)]',
  },
  red: {
    value: 'text-[#ef4444]',
    glow: '0 0 20px rgba(239, 68, 68, 0.15)',
    border: 'border-[rgba(239,68,68,0.2)]',
  },
}

export function KpiCard({ label, value, delta, deltaLabel, color = 'cyan' }: KpiCardProps) {
  const palette = COLOR_MAP[color]
  return (
    <div
      className="col-span-6 card p-4 flex flex-col justify-between rounded-lg border transition-all duration-200 hover:scale-[1.02]"
      style={{ boxShadow: palette.glow, borderColor: palette.glow.includes('0.15') ? palette.glow.replace('0.15', '0.25') : undefined }}
    >
      <div className="text-[#a1a1aa] text-[10px] uppercase tracking-widest font-mono">{label}</div>
      <div className={`font-heading text-3xl mt-1 ${palette.value}`} style={{ fontFamily: "'Space Grotesk', sans-serif" }}>
        {value}
      </div>
      {delta !== undefined && (
        <div className={`text-xs mt-1 ${delta >= 0 ? 'text-[#f59e0b]' : 'text-[#22c55e]'}`}>
          {delta >= 0 ? '↑' : '↓'} {Math.abs(delta)} {deltaLabel ?? ''}
        </div>
      )}
    </div>
  )
}
