# ── Stage 1: install production dependencies ──────────────────────────────────
FROM node:24-alpine AS deps
WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev --ignore-scripts

# ── Stage 2: lean runtime image ───────────────────────────────────────────────
FROM node:24-alpine

LABEL org.opencontainers.image.title="Pattern Assessment Backend"
LABEL org.opencontainers.image.description="Node/Express scoring engine + OpenAI integration"

WORKDIR /app

# Copy only prod node_modules and source
COPY --from=deps /app/node_modules ./node_modules
COPY src/ ./src/

# Non-root user for security.
# Create the payload storage directory BEFORE switching user so Docker
# initialises the named volume with appuser ownership on first mount.
# Without this the volume is root-owned and the non-root process gets EACCES.
RUN addgroup -S appgroup && adduser -S appuser -G appgroup \
 && mkdir -p /app/data/payloads \
 && chown -R appuser:appgroup /app/data
USER appuser

EXPOSE 3001
ENV NODE_ENV=production

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -q -O- http://localhost:3001/health || exit 1

CMD ["node", "src/server.js"]
