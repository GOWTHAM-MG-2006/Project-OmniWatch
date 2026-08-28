/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      // 24-column grid from Dynatrace R&D report §Design
      gridTemplateColumns: {
        '24': 'repeat(24, minmax(0, 1fr))',
      },

      // ─── Stitch Design System Tokens ───────────────────────────
      // Source: assets/7b830e7b8d3e41af87827c95061aa97c (OmniWatch Dashboard)
      // Color mode: DARK | Variant: FIDELITY | Roundness: ROUND_EIGHT

      colors: {
        // ── Material Design Surface Tones ──
        surface: {
          DEFAULT: '#121416',
          dim: '#121416',
          bright: '#37393c',
          'container-lowest': '#0c0e11',
          'container-low': '#1a1c1e',
          container: '#1e2022',
          'container-high': '#282a2c',
          'container-highest': '#333537',
          variant: '#333537',
          tint: '#3cd7ff',
        },
        'on-surface': {
          DEFAULT: '#e2e2e5',
          variant: '#bbc9cf',
        },
        'inverse-surface': '#e2e2e5',
        'inverse-on-surface': '#2f3033',
        outline: {
          DEFAULT: '#859398',
          variant: '#3c494e',
        },

        // ── Primary (Cyan) ──
        primary: {
          DEFAULT: '#a8e8ff',
          container: '#00d4ff',
          'on-primary': '#003642',
          'on-primary-container': '#00586b',
          fixed: '#b4ebff',
          'fixed-dim': '#3cd7ff',
          'on-primary-fixed': '#001f27',
          'on-primary-fixed-variant': '#004e5f',
        },
        'inverse-primary': '#00677e',

        // ── Secondary (Violet) ──
        secondary: {
          DEFAULT: '#d2bbff',
          container: '#6001d1',
          'on-secondary': '#3f008e',
          'on-secondary-container': '#c9aeff',
          fixed: '#eaddff',
          'fixed-dim': '#d2bbff',
          'on-secondary-fixed': '#25005a',
          'on-secondary-fixed-variant': '#5a00c6',
        },

        // ── Tertiary (Amber) ──
        tertiary: {
          DEFAULT: '#ffd9a1',
          container: '#feb528',
          'on-tertiary': '#432c00',
          'on-tertiary-container': '#6c4900',
          fixed: '#ffdeae',
          'fixed-dim': '#ffba3d',
          'on-tertiary-fixed': '#281900',
          'on-tertiary-fixed-variant': '#604100',
        },

        // ── Error (Red) ──
        error: {
          DEFAULT: '#ffb4ab',
          container: '#93000a',
          'on-error': '#690005',
          'on-error-container': '#ffdad6',
        },

        // ── Backgrounds ──
        bg: {
          primary: '#0f0f0f',
          deep: '#0a0a0f',
          card: '#1a1a1a',
          'card-alt': '#1e1e2e',
        },

        // ── Accent (convenience aliases) ──
        accent: {
          cyan: '#00d4ff',
          violet: '#7c3aed',
        },

        // ── Border ──
        border: {
          default: '#2a2a2a',
        },

        // ── Text ──
        text: {
          primary: '#e2e2e5',
          muted: '#a1a1aa',
        },

        // ── Status Colors ──
        status: {
          healthy: '#22c55e',
          warning: '#f59e0b',
          critical: '#ef4444',
          info: '#00d4ff',
        },

        // ── Named Colors (Stitch Fidelity palette) ──
        'on-background': '#e2e2e5',
        'on-error': '#690005',
        'on-error-container': '#ffdad6',
      },

      // ── Typography Scale ──
      fontSize: {
        'headline-xl': ['30px', { lineHeight: '36px', fontWeight: '700', fontFamily: 'Space Grotesk' }],
        'headline-lg': ['24px', { lineHeight: '32px', fontWeight: '600', fontFamily: 'Space Grotesk' }],
        'headline-md': ['20px', { lineHeight: '28px', fontWeight: '600', fontFamily: 'Space Grotesk' }],
        'body-lg': ['18px', { lineHeight: '28px', fontWeight: '400', fontFamily: 'Inter' }],
        'body-md': ['16px', { lineHeight: '24px', fontWeight: '400', fontFamily: 'Inter' }],
        'body-sm': ['14px', { lineHeight: '20px', fontWeight: '400', fontFamily: 'Inter' }],
        'label-md': ['14px', { lineHeight: '20px', fontWeight: '500', fontFamily: 'JetBrains Mono' }],
        'label-sm': ['12px', { lineHeight: '16px', fontWeight: '500', fontFamily: 'JetBrains Mono' }],
      },

      // Font families
      fontFamily: {
        sans: ['Inter', 'Geist Sans', 'system-ui', 'sans-serif'],
        heading: ['Space Grotesk', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'Consolas', 'monospace'],
      },

      // ── Spacing Scale (4px baseline) ──
      spacing: {
        'unit-xs': '4px',
        'unit-sm': '8px',
        'unit-md': '12px',
        'unit-lg': '16px',
        'gutter': '8px',
        'margin-mobile': '16px',
        'margin-desktop': '24px',
      },

      // Border radius
      borderRadius: {
        'card': '8px',
        'panel': '12px',
        'sm': '0.25rem',
        'DEFAULT': '0.5rem',
        'md': '0.75rem',
        'lg': '1rem',
        'xl': '1.5rem',
        'full': '9999px',
      },

      // Box shadows
      boxShadow: {
        'card': '0 1px 3px rgba(0, 0, 0, 0.3)',
        'card-hover': '0 4px 12px rgba(0, 212, 255, 0.08)',
        'modal': '0 4px 16px rgba(0, 0, 0, 0.5)',
        'glow-cyan': '0 0 20px rgba(0, 212, 255, 0.15)',
        'glow-violet': '0 0 20px rgba(124, 58, 237, 0.15)',
      },

      // ── Animations ──
      animation: {
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'glow': 'glow 2s ease-in-out infinite alternate',
      },

      keyframes: {
        glow: {
          '0%': { boxShadow: '0 0 5px rgba(0, 212, 255, 0.2)' },
          '100%': { boxShadow: '0 0 20px rgba(0, 212, 255, 0.4)' },
        },
      },
    },
  },
  plugins: [],
}
