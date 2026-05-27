# ── Stage 1: build the Vite bundle ────────────────────────────────────────────
FROM node:24-alpine AS builder
WORKDIR /app

COPY package*.json ./
RUN npm ci --ignore-scripts

COPY . .
RUN npm run build          # outputs to /app/dist

# ── Stage 2: serve with nginx ─────────────────────────────────────────────────
FROM nginx:1.30-alpine

LABEL org.opencontainers.image.title="Pattern Assessment Frontend"
LABEL org.opencontainers.image.description="React/Vite SPA served by nginx with /api proxy"

# Remove default nginx config
RUN rm /etc/nginx/conf.d/default.conf

# Copy built assets and our nginx config
COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --retries=3 \
  CMD wget -q -O- http://localhost:80/health || exit 1

CMD ["nginx", "-g", "daemon off;"]
