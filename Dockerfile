# syntax=docker/dockerfile:1.6

# ============================================================================
# STAGE 1: Build Environment
# ============================================================================
FROM golang:1.21-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for build
RUN useradd -m -u 1000 builder
USER builder
WORKDIR /home/builder/app

# Cache dependencies
COPY --chown=builder:builder go.mod go.sum ./
RUN go mod download

# Copy actual source
COPY --chown=builder:builder . .

# Build release binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o ttdust cmd/server/main.go

# ============================================================================
# STAGE 2: Runtime Environment (Production)
# ============================================================================
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

# Labels
LABEL org.opencontainers.image.title="TTDust"
LABEL org.opencontainers.image.description="Data Infrastructure Platform"
LABEL org.opencontainers.image.version="1.0.0"
LABEL org.opencontainers.image.vendor="TTDust"

# Copy binary from builder
COPY --from=builder /home/builder/app/ttdust /usr/local/bin/ttdust

# Copy migrations (needed for embedded migrations)
COPY --from=builder /home/builder/app/migrations /app/migrations

# Create data directories
WORKDIR /app

# Expose ports
# 8080: REST API
# 9090: gRPC API
EXPOSE 8080 9090

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/ttdust", "health"]

# Run as non-root
USER nonroot:nonroot

# Entry point
ENTRYPOINT ["/usr/local/bin/ttdust"]
CMD ["serve"]

# ============================================================================
# STAGE 3: Debug variant
# ============================================================================
FROM debian:bookworm-slim AS debug

# Install debugging tools
RUN apt-get update && apt-get install -y \
    curl \
    netcat-openbsd \
    dnsutils \
    procps \
    strace \
    htop \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 ttdust
USER ttdust

COPY --from=builder /home/builder/app/ttdust /usr/local/bin/ttdust
COPY --from=builder /home/builder/app/migrations /app/migrations

WORKDIR /app
EXPOSE 8080 9090

ENTRYPOINT ["/usr/local/bin/ttdust"]
CMD ["serve"]
