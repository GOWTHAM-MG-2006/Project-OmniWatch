import { useState } from 'react'

type Timeframe = '1h' | '6h' | '24h' | '7d'

interface TopBarProps {
  onSearch: (query: string) => void
}

export function TopBar({ onSearch }: TopBarProps) {
  const [timeframe, setTimeframe] = useState<Timeframe>('24h')
  const [segment, setSegment] = useState('production')
  const [searchQuery, setSearchQuery] = useState('')

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault()
    if (searchQuery.trim()) {
      onSearch(searchQuery.trim())
    }
  }

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

      {/* Segment Dropdown */}
      <select
        value={segment}
        onChange={(e) => setSegment(e.target.value)}
        className="bg-bg-deep border border-border-default rounded px-2 py-1 text-xs text-text-primary appearance-auto"
      >
        <option value="production">production</option>
        <option value="staging">staging</option>
        <option value="development">development</option>
      </select>

      {/* Spacer */}
      <div className="flex-1" />

      {/* CoPilot Search */}
      <form onSubmit={handleSearch} className="flex items-center gap-2">
        <div className="relative">
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="CoPilot: Ask anything..."
            className="w-64 bg-bg-deep border border-border-default rounded px-3 py-1 text-xs text-text-primary placeholder:text-text-muted focus:outline-none focus:border-accent-cyan"
          />
          <span className="absolute right-2 top-1/2 -translate-y-1/2 text-text-muted text-[10px]">
            ⌘K
          </span>
        </div>
      </form>
    </header>
  )
}
