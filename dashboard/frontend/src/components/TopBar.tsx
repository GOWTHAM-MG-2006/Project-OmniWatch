import { useTimeRange, TIMEFRAMES, type Timeframe } from '../hooks/useTimeRange'

export function TopBar() {
  const { timeRange, setTimeRange } = useTimeRange()

  return (
    <header className="h-12 bg-bg-card border-b border-border-default flex items-center px-4 gap-4 shrink-0">
      {/* Logo */}
      <div className="font-heading text-accent-cyan font-bold text-lg tracking-tight">
        OmniWatch
      </div>

      {/* Timeframe Picker — writes to URL ?timeRange= */}
      <div className="flex items-center gap-1 ml-4" role="group" aria-label="Timeframe">
        {TIMEFRAMES.map((tf: Timeframe) => (
          <button
            key={tf}
            onClick={() => setTimeRange(tf)}
            aria-pressed={timeRange === tf}
            className={`px-2 py-1 text-xs rounded transition-colors ${
              timeRange === tf
                ? 'bg-accent-cyan/20 text-accent-cyan'
                : 'text-text-muted hover:text-text-primary hover:bg-bg-deep'
            }`}
          >
            {tf}
          </button>
        ))}
      </div>

      {/* Spacer */}
      <div className="flex-1" />
    </header>
  )
}
