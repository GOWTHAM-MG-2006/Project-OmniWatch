import { useState } from 'react'

type Timeframe = '1h' | '6h' | '24h' | '7d'

export function TopBar() {
  const [timeframe, setTimeframe] = useState<Timeframe>('24h')

  return (
    <header className="h-12 bg-bg-card border-b border-border-default flex items-center px-4 gap-4 shrink-0">
      {/* Logo */}
      <div className="font-heading text-accent-cyan font-bold text-lg tracking-tight">
        OmniWatch
      </div>

      {/* Timeframe Picker */}
      <div className="flex items-center gap-1 ml-4">
        {(['1h', '6h', '24h', '7d'] as Timeframe[]).map((tf) => (
          <button
            key={tf}
            onClick={() => setTimeframe(tf)}
            className={`px-2 py-1 text-xs rounded transition-colors ${
              timeframe === tf
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
