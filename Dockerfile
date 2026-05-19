# =============================================================================
# Portuguese Financial Regulation MCP — multi-stage Dockerfile
# =============================================================================
# Build:  docker build -t portuguese-financial-regulation-mcp .
# Run:    docker run --rm -p 3000:3000 portuguese-financial-regulation-mcp
#
# The image bakes the pre-built database at /app/data/cmvm.db.
# Override with CMVM_DB_PATH for a custom location.
#
# Multi-stage build:
#  - builder: installs all deps (incl. better-sqlite3 native build), compiles TS
#  - production: copies built node_modules + dist, runs as non-root mcp user
#
# The builder stage runs `npm rebuild better-sqlite3` to ensure the native
# binding is present, then we COPY --from=builder /app/node_modules to the
# production stage instead of re-running `npm ci --omit=dev` (which would
# strip the binding).
# =============================================================================

# --- Stage 1: Build TypeScript + native deps ---
FROM node:20-alpine AS builder

# Toolchain for better-sqlite3 native build
RUN apk add --no-cache python3 make g++

WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci
RUN npm rebuild better-sqlite3
COPY tsconfig.json ./
COPY src/ src/
COPY scripts/ scripts/
RUN npm run build

# --- Stage 2: Production ---
FROM node:20-alpine AS production

WORKDIR /app
ENV NODE_ENV=production
ENV CMVM_DB_PATH=/app/data/cmvm.db

COPY package.json package-lock.json* ./
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist

# Bake the pre-built CMVM database into the image
COPY data/database.db data/cmvm.db

# Non-root user (UID 1001) for security
RUN addgroup -S -g 1001 mcp && \
    adduser -S -u 1001 -G mcp mcp && \
    chown -R mcp:mcp /app
USER mcp

EXPOSE 3000

# Health check: verify HTTP server responds
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/health',r=>{process.exit(r.statusCode===200?0:1)}).on('error',()=>process.exit(1))"

CMD ["node", "dist/src/http-server.js"]
