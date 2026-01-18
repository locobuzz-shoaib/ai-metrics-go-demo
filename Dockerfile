# CreatorVerse AI Token Metrics Service Dockerfile
# Multi-stage build for production optimization

# Stage 1: Build stage
FROM golang:1.21-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /build

# Copy go mod files first for better layer caching
COPY go.mod go.sum* ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

# Copy source code and build
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-w -s" -o creatorverse-ai-token-metrics-service .

# Stage 2: Runtime stage
FROM alpine:3.19 AS runtime

# Install runtime dependencies and create non-root user
RUN apk --no-cache add ca-certificates tzdata && \
    addgroup -g 1000 appgroup && \
    adduser -D -u 1000 -G appgroup appuser

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/creatorverse-ai-token-metrics-service .

# Copy configuration file
COPY appsettings.json .

# Set ownership
RUN chown -R appuser:appgroup /app

USER appuser

EXPOSE 8080

ENTRYPOINT ["./creatorverse-ai-token-metrics-service"]
