# LessorMQ, the Go server: one static binary that serves HTTP on port 8080.
#
#   docker build -f server/Dockerfile -t lessormq server
#
# It migrates the schema on start (MIGRATE=0 skips that), so the image needs nothing but
# DATABASE_QUEUE_URL; REDIS_URL only when several nodes share the work.

# Built on the build machine's platform for the target's (no emulation for arm64).
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /lessormq ./cmd/lessormq

FROM gcr.io/distroless/static-debian12:nonroot
LABEL org.opencontainers.image.title="LessorMQ" \
      org.opencontainers.image.description="Message queue over HTTP where the server decides what happens to every message" \
      org.opencontainers.image.source="https://github.com/gsid-nl/lessormq" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /lessormq /usr/local/bin/lessormq
USER 1000:1000
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/lessormq"]
CMD ["serve"]
