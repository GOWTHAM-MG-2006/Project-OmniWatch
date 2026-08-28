# OmniWatch Dashboard Design System

**Stitch Project**: `projects/18001051394810965271` (OmniWatch Dashboard)
**Design System**: `assets/7b830e7b8d3e41af87827c95061aa97c` (OmniWatch Dashboard)
**Color Mode**: DARK | **Variant**: FIDELITY | **Roundness**: ROUND_EIGHT

---

## Color Palette (Material Design 3 Surface Tones)

### Surface Tones
| Token | Hex | Usage |
|-------|-----|-------|
| `surface.DEFAULT` | `#121416` | Default surface |
| `surface.dim` | `#121416` | Dimmed surface |
| `surface.bright` | `#37393c` | Bright surface |
| `surface.container-lowest` | `#0c0e11` | Lowest container |
| `surface.container-low` | `#1a1c1e` | Low container |
| `surface.container` | `#1e2022` | Standard container |
| `surface.container-high` | `#282a2c` | High container |
| `surface.container-highest` | `#333537` | Highest container |
| `surface.variant` | `#333537` | Variant surface |
| `surface.tint` | `#3cd7ff` | Cyan tint |

### On-Surface
| Token | Hex | Usage |
|-------|-----|-------|
| `on-surface.DEFAULT` | `#e2e2e5` | Primary text on surface |
| `on-surface.variant` | `#bbc9cf` | Muted text on surface |

### Primary (Cyan - Brand)
| Token | Hex | Usage |
|-------|-----|-------|
| `primary.DEFAULT` | `#a8e8ff` | Primary action |
| `primary.container` | `#00d4ff` | Primary container |
| `on-primary` | `#003642` | Text on primary |
| `on-primary-container` | `#00586b` | Text on primary container |
| `primary.fixed` | `#b4ebff` | Fixed primary |
| `primary.fixed-dim` | `#3cd7ff` | Dimmed fixed primary |
| `on-primary-fixed` | `#001f27` | Text on fixed primary |
| `on-primary-fixed-variant` | `#004e5f` | Text on fixed primary variant |
| `inverse-primary` | `#00677e` | Inverse primary |

### Secondary (Violet)
| Token | Hex | Usage |
|-------|-----|-------|
| `secondary.DEFAULT` | `#d2bbff` | Secondary action |
| `secondary.container` | `#6001d1` | Secondary container |
| `on-secondary` | `#3f008e` | Text on secondary |
| `on-secondary-container` | `#c9aeff` | Text on secondary container |

### Tertiary (Amber)
| Token | Hex | Usage |
|-------|-----|-------|
| `tertiary.DEFAULT` | `#ffd9a1` | Tertiary action |
| `tertiary.container` | `#feb528` | Tertiary container |

### Error (Red)
| Token | Hex | Usage |
|-------|-----|-------|
| `error.DEFAULT` | `#ffb4ab` | Error action |
| `error.container` | `#93000a` | Error container |

### Backgrounds
| Token | Hex | Usage |
|-------|-----|-------|
| `bg.primary` | `#0f0f0f` | Main page background |
| `bg.deep` | `#0a0a0f` | Deep background |
| `bg.card` | `#1a1a1a` | Card background |
| `bg.card-alt` | `#1e1e2e` | Alternative card |

### Accents (Convenience)
| Token | Hex | Usage |
|-------|-----|-------|
| `accent.cyan` | `#00d4ff` | Primary accent |
| `accent.violet` | `#7c3aed` | Secondary accent |

### Borders
| Token | Hex | Usage |
|-------|-----|-------|
| `border.default` | `#2a2a2a` | Default border |

### Text
| Token | Hex | Usage |
|-------|-----|-------|
| `text.primary` | `#e2e2e5` | Primary text |
| `text.muted` | `#a1a1aa` | Muted text |

### Status Colors
| Status | Color | Hex |
|--------|-------|-----|
| Healthy | Green | `#22c55e` |
| Warning | Amber | `#f59e0b` |
| Critical | Red | `#ef4444` |
| Info | Cyan | `#00d4ff` |

---

## Typography Scale

| Token | Size | Line Height | Weight | Font Family |
|-------|------|-------------|--------|-------------|
| `headline-xl` | 30px | 36px | 700 | Space Grotesk |
| `headline-lg` | 24px | 32px | 600 | Space Grotesk |
| `headline-md` | 20px | 28px | 600 | Space Grotesk |
| `body-lg` | 18px | 28px | 400 | Inter |
| `body-md` | 16px | 24px | 400 | Inter |
| `body-sm` | 14px | 20px | 400 | Inter |
| `label-md` | 14px | 20px | 500 | JetBrains Mono |
| `label-sm` | 12px | 16px | 500 | JetBrains Mono |

### Font Families
| Role | Font | Tailwind Class |
|------|------|----------------|
| Body | Inter, Geist Sans | `font-sans` |
| Headlines | Space Grotesk | `font-heading` |
| Monospace | JetBrains Mono | `font-mono` |

---

## Spacing Scale (4px Baseline)

| Token | Value |
|-------|-------|
| `unit-xs` | 4px |
| `unit-sm` | 8px |
| `unit-md` | 12px |
| `unit-lg` | 16px |
| `gutter` | 8px |
| `margin-mobile` | 16px |
| `margin-desktop` | 24px |

---

## Grid System
- **24-column CSS grid** (`grid-cols-24`)
- Gap: 8px (`gap-2`)
- Breakpoints: standard Tailwind (sm/md/lg/xl/2xl)

---

## Border Radius
| Token | Value | Usage |
|-------|-------|-------|
| `card` | 8px | Cards |
| `panel` | 12px | Modals/panels |
| `lg` | 1rem | Large elements |
| `xl` | 1.5rem | Extra large |
| `full` | 9999px | Pills/badges |

---

## Shadows
| Token | Value | Usage |
|-------|-------|-------|
| `card` | `0 1px 3px rgba(0,0,0,0.3)` | Standard cards |
| `card-hover` | `0 4px 12px rgba(0,212,255,0.08)` | Card hover |
| `modal` | `0 4px 16px rgba(0,0,0,0.5)` | Modals/drawers |
| `glow-cyan` | `0 0 20px rgba(0,212,255,0.15)` | Cyan glow |
| `glow-violet` | `0 0 20px rgba(124,58,237,0.15)` | Violet glow |

---

## Animations
| Token | Definition |
|-------|------------|
| `pulse-slow` | `pulse 3s cubic-bezier(0.4,0,0.6,1) infinite` |
| `glow` | `glow 2s ease-in-out infinite alternate` |

### Keyframes
```css
@keyframes glow {
  0% { box-shadow: 0 0 5px rgba(0, 212, 255, 0.2); }
  100% { box-shadow: 0 0 20px rgba(0, 212, 255, 0.4); }
}
```

---

## CSS Variables (index.css)
All design tokens exposed as CSS custom properties under `:root`:
- `--sw-*` prefix for Stitch tokens (surface, primary, secondary, etc.)
- `--bg-*`, `--accent-*`, `--border-*`, `--text-*` legacy aliases
- `--gradient-*` for gradients
- `--shadow-*` for shadows
- `--font-*` for font families

---

## Component Standards

### Cards
- Background: `bg-card` / `surface.container`
- Border: `border-default` / `outline.DEFAULT`
- Radius: `rounded-card` (8px)
- Hover: `border-primary/20` + `shadow-card-hover`

### Interactive Elements
- Primary actions: `bg-primary-container` text `on-primary-container`
- Secondary actions: `bg-secondary-container` text `on-secondary-container`
- Hover states use `accent-cyan` / `primary` variants

### Status Badges
- Colored dot + label with `status-*` colors
- Subtle background tint matching status color
- Monospace font for labels

### Topology (React Flow)
- Node types: Service (blue), Database (green), Infrastructure (orange), Pattern (purple)
- Edge labels: CALLS (latency), READS_FROM (duration), DEPENDS_ON
- Layouts: Force (dagre), Vertical, Horizontal
- Performance cap: 4000 nodes / 10000 edges

### AI Overlay
- Recharts ReferenceDot for anomaly markers
- Forecast dashed confidence bands
- Ollama CoPilot search bar integration

---

## Dynatrace-Level Compliance
✅ 24-column grid system
✅ 18+ visualization types supported via Recharts + React Flow
✅ Role-based filtering via variables/segments
✅ Real data behind every tile (no placeholders)
✅ Evidence-linked thresholds
✅ Dark mode with Material Design 3 surface tones
✅ Space Grotesk + Inter typography (modern, clean)
✅ ROUND_EIGHT (8px) border radius
✅ Cyan/Violet brand accents
✅ Comprehensive shadow/glow system
✅ Smooth animations (fade-in, pulse, glow)

---

## Integration Status
- ✅ `tailwind.config.js` — Full token integration (193 lines)
- ✅ `src/index.css` — CSS variables + utility classes (212 lines)
- ✅ `DESIGN.md` — This documentation
- ✅ Stitch project created: `projects/18001051394810965271`
- ⚠️ Stitch MCP `create_design_system` — Rate limited / validation error (fallback documented)
- ✅ Frontend builds clean: `npm run build` → 951kb, 911 modules, 7.8s

---

## Next Steps (Tasks 12-13)
1. Generate 6 dashboard screens conceptually using design tokens
2. Create variants for each screen (REFINE creative range)
3. Integrate into existing React components (Overview, IncidentExplorer, Topology, KnowledgeBase, GenAIReports, Security)
