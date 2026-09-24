# ── Stage 1: UI Builder ──────────────────────────────────────────
FROM node:24-alpine AS ui-builder
WORKDIR /app/ui
COPY ui/package*.json ./
RUN npm install
COPY ui/ ./
RUN npm run build

# ── Stage 2: Go Controller Builder ───────────────────────────────
FROM golang:1.27-bookworm AS go-builder
WORKDIR /app
COPY control-plane/go.mod control-plane/go.sum ./control-plane/
RUN cd control-plane && go mod download
COPY control-plane/ ./control-plane/
COPY --from=ui-builder /app/control-plane/internal/console/dist ./control-plane/internal/console/dist
RUN cd control-plane && CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags="-s -w" -o /app/velda-cp ./cmd

# ── Stage 3: Production Runtime ──────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=go-builder /app/velda-cp /app/velda-cp

EXPOSE 8080
EXPOSE 9099

ENTRYPOINT ["/app/velda-cp"]
