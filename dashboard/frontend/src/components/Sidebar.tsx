import { NavLink } from 'react-router-dom'

const navItems = [
  { path: '/', label: 'Overview', icon: '◉' },
  { path: '/incidents', label: 'Incidents', icon: '⚡' },
  { path: '/topology', label: 'Topology', icon: '◈' },
  { path: '/knowledge', label: 'Knowledge', icon: '◆' },
  { path: '/reports', label: 'Reports', icon: '◇' },
  { path: '/security', label: 'Security', icon: '▣' },
  { path: '/storage', label: 'Storage', icon: '⬢' },
]

export function Sidebar() {
  return (
    <aside className="w-48 bg-bg-card border-r border-border-default flex flex-col shrink-0">
      {/* Navigation */}
      <nav className="flex-1 py-2">
        {navItems.map((item) => (
          <NavLink
            key={item.path}
            to={item.path}
            end={item.path === '/'}
            className={({ isActive }) =>
              `flex items-center gap-2 px-4 py-2 text-sm transition-colors ${
                isActive
                  ? 'bg-accent-cyan/10 text-accent-cyan border-r-2 border-accent-cyan'
                  : 'text-text-muted hover:text-text-primary hover:bg-bg-deep'
              }`
            }
          >
            <span className="text-base">{item.icon}</span>
            <span>{item.label}</span>
          </NavLink>
        ))}
      </nav>

      {/* Entity Counts Legend */}
      <div className="border-t border-border-default p-3">
        <div className="text-[10px] text-text-muted uppercase tracking-wider mb-2">
          Entities
        </div>
        <div className="space-y-1 text-xs">
          <div className="flex justify-between">
            <span className="text-text-muted">Services</span>
            <span className="text-text-primary">—</span>
          </div>
          <div className="flex justify-between">
            <span className="text-text-muted">Databases</span>
            <span className="text-text-primary">—</span>
          </div>
          <div className="flex justify-between">
            <span className="text-text-muted">Infra</span>
            <span className="text-text-primary">—</span>
          </div>
        </div>
      </div>
    </aside>
  )
}
