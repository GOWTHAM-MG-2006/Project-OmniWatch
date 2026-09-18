# Reproducible-build epoch. Override at build time:
#   docker build --build-arg SOURCE_DATE_EPOCH=$(git log -1 --format=%ct) ...
ARG SOURCE_DATE_EPOCH=0
# Build stage
FROM golang:1.23-alpine AS builder
ARG SOURCE_DATE_EPOCH
ENV SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}
WORKDIR /app
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
# Deterministic COPY ordering: explicit dir list (no wildcards), alphabetical.
COPY cmd ./cmd
COPY configs ./configs
COPY internal ./internal
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o omniwatch-agent ./cmd/agent

# Runtime stage - non-root user
FROM gcr.io/distroless/static-debian12
USER nonroot:nonroot
COPY --from=builder /app/omniwatch-agent /omniwatch-agent
COPY --from=builder /app/configs/agent.yaml /etc/omniwatch/agent.yaml
COPY --from=builder /app/configs/alerts /etc/omniwatch/alerts
EXPOSE 8080
ENTRYPOINT ["/omniwatch-agent"]
CMD ["--config", "/etc/omniwatch/agent.yaml"]
