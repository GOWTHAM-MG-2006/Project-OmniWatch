# OmniWatch Dashboard Design System

**Stitch Fallback** — Created manually when Stitch MCP was unavailable.

## Color Palette (Dark Mode)

### Backgrounds
| Token | Hex | Usage |
|-------|-----|-------|
| `--bg-primary` | `#0f0f0f` | Main page background |
| `--bg-deep` | `#0a0a0f` | Deep background variant |
| `--bg-card` | `#1a1a1a` | Card/panel background |
| `--bg-card-alt` | `#1e1e2e` | Alternative card background |

### Accents
| Token | Hex | Usage |
|-------|-----|-------|
| `--accent-cyan` | `#00d4ff` | Primary accent, links, highlights |
| `--accent-violet` | `#7c3aed` | Secondary accent, badges |

### Borders
| Token | Hex | Usage |
|-------|-----|-------|
| `--border-default` | `#2a2a2a` | Default border color |

### Text
| Token | Hex | Usage |
|-------|-----|-------|
| `--text-primary` | `#e4e4e7` | Primary text (white) |
| `--text-muted` | `#a1a1aa` | Muted/secondary text |

## Typography

### Font Families
| Role | Font | Tailwind Class |
|------|------|----------------|
| Body | Inter, Geist Sans | `font-sans` |
| Headlines | Space Grotesk | `font-heading` |
| Monospace | JetBrains Mono | `font-mono` |

### Font Sizes
- `text-xs`: 12px / 0.75rem
- `text-sm`: 14px / 0.875rem
- `text-base`: 16px / 1rem
- `text-lg`: 18px / 1.125rem
- `text-xl`: 20px / 1.25rem
- `text-2xl`: 24px / 1.5rem
- `text-3xl`: 30px / 1.875rem

## Spacing Scale
- Standard Tailwind spacing (4px increments)
- Custom: `gap-1` = 4px, `gap-2` = 8px, `gap-3` = 12px, `gap-4` = 16px

## Grid System
- **24-column CSS grid** (`grid-cols-24`)
- Gap: 8px (`gap-2`)
- Breakpoints: standard Tailwind (sm/md/lg/xl/2xl)

## Border Radius
- `rounded-lg` (8px) for cards
- `rounded-xl` (12px) for modals/panels

## Shadows
- Card: `0 1px 3px rgba(0,0,0,0.3)`
- Modal: `0 4px 16px rgba(0,0,0,0.5)`

## Status Colors
| Status | Color | Hex |
|--------|-------|-----|
| Healthy | Green | `#22c55e` |
| Warning | Amber | `#f59e0b` |
| Critical | Red | `#ef4444` |
| Info | Cyan | `#00d4ff` |

## Components
- Cards use `bg-card` with `border-default` border
- Interactive elements use `accent-cyan` for hover states
- Status badges use status color + subtle background tint
