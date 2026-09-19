import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8011',
        changeOrigin: true,
      },
      // Identity service (identity/main.py, port 8012). The frontend calls
      // it via the relative '/identity' prefix so no host is hardcoded and
      // no CORS preflight is needed in dev.
      '/identity': {
        target: 'http://localhost:8012',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/identity/, ''),
      },
    },
  },
})
