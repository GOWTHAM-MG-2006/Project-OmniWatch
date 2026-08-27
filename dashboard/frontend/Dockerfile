# ---------------------------------------------------------------------------
# OmniWatch — Dashboard Frontend Dockerfile
# Phase: 11
# Purpose: Multi-stage build — compile React SPA with Node, serve with nginx
# Inputs:  dashboard/frontend/src/ (React + TypeScript + Tailwind source)
# Outputs: nginx container serving static SPA on port 3000 with /api proxy
# ---------------------------------------------------------------------------

# == Stage 1: builder — compile React + TypeScript ==========================
FROM node:20-alpine AS builder

WORKDIR /app

# -- Install dependencies (cached layer) --
COPY dashboard/frontend/package.json dashboard/frontend/package-lock.json ./
RUN npm ci

# -- Copy source and build --
COPY dashboard/frontend/ ./
RUN npm run build

# == Stage 2: runtime — lean nginx image ====================================
FROM nginx:alpine AS runtime

# -- Remove default nginx config --
RUN rm /etc/nginx/conf.d/default.conf

# -- Copy custom nginx config --
COPY dashboard/frontend/nginx.conf /etc/nginx/conf.d/default.conf

# -- Copy built SPA from builder --
COPY --from=builder /app/dist /usr/share/nginx/html

# -- Expose port --
EXPOSE 3000

# -- Health check --
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:3000/healthz || exit 1

# -- Run nginx in foreground --
CMD ["nginx", "-g", "daemon off;"]
