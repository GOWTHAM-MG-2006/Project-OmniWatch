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

      // Color tokens (dark mode dashboard)
      colors: {
        bg: {
          primary: '#0f0f0f',
          deep: '#0a0a0f',
          card: '#1a1a1a',
          'card-alt': '#1e1e2e',
        },
        accent: {
          cyan: '#00d4ff',
          violet: '#7c3aed',
        },
        border: {
          default: '#2a2a2a',
        },
        text: {
          primary: '#e4e4e7',
          muted: '#a1a1aa',
        },
        status: {
          healthy: '#22c55e',
          warning: '#f59e0b',
          critical: '#ef4444',
          info: '#00d4ff',
        },
      },

      // Font families
      fontFamily: {
        sans: ['Inter', 'Geist Sans', 'system-ui', 'sans-serif'],
        heading: ['Space Grotesk', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'Consolas', 'monospace'],
      },

      // Border radius
      borderRadius: {
        'card': '8px',
        'panel': '12px',
      },

      // Box shadows
      boxShadow: {
        'card': '0 1px 3px rgba(0, 0, 0, 0.3)',
        'modal': '0 4px 16px rgba(0, 0, 0, 0.5)',
      },
    },
  },
  plugins: [],
}
