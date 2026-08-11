# Example Dockerfile for a qless server.
#
# This builds the example program in examples/main.go. For your own service, change the final `go build`
# path to your main package. Everything else applies as-is, since a qless server is just a normal Go HTTP server.
#
# Build from the repository root:
#   docker build -f deploy/docker/Dockerfile -t qless-example .
# Run:
#   docker run --rm -p 8080:8080 -e API_KEY=secret123 qless-example

FROM golang:1.25 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./examples

# Distroless: no shell, no package manager, tiny attack surface.
# If you need a shell or an in-container healthcheck command, swap for alpine.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/server /server

ENV PORT=8080
EXPOSE 8080

# SIGTERM triggers the example's graceful shutdown: drain HTTP (3s), then let
# qless finish queued jobs (6s). Ensure the runtime's stop grace period exceeds
# the sum, or in-flight jobs are killed instead of drained.
STOPSIGNAL SIGTERM

ENTRYPOINT ["/server"]
