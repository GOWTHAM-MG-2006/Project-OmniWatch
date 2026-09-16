import { useState } from 'react'
import { useTimeRange, TIMEFRAMES, type Timeframe } from '../hooks/useTimeRange'
import { CopilotDrawer } from './CopilotDrawer'

export function TopBar() {
  const { timeRange, setTimeRange } = useTimeRange()
  const [copilotOpen, setCopilotOpen] = useState(false)

  return (
    <>
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

        {/* Copilot Button */}
        <button
          onClick={() => setCopilotOpen(true)}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-xl bg-accent-cyan/10 text-accent-cyan hover:bg-accent-cyan/20 transition-colors"
        >
          <svg
            xmlns="http://www.w3.org/2000/svg"
            width="14"
            height="14"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
          </svg>
          Copilot
        </button>
      </header>
      <CopilotDrawer open={copilotOpen} onClose={() => setCopilotOpen(false)} />
    </>
  )
}
