# Build stage
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o omniwatch-agent ./cmd/agent

# Runtime stage - non-root user
FROM gcr.io/distroless/static-debian12
USER nonroot:nonroot
COPY --from=builder /app/omniwatch-agent /omniwatch-agent
COPY --from=builder /app/configs/agent.yaml /etc/omniwatch/agent.yaml
EXPOSE 8080
ENTRYPOINT ["/omniwatch-agent"]
