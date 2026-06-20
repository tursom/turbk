# syntax=docker/dockerfile:1.7

FROM golang:1.26-alpine AS go-build
WORKDIR /src
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}
COPY go.mod go.sum* ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=0.1.0-dev
ARG COMMIT=unknown
ARG DATE=unknown
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build \
    -ldflags="-s -w -X github.com/tursom/turbk/internal/version.Version=${VERSION} -X github.com/tursom/turbk/internal/version.Commit=${COMMIT} -X github.com/tursom/turbk/internal/version.Date=${DATE}" \
    -o /out/turbk-agent ./cmd/turbk-agent

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata \
 && mkdir -p /backup/source
COPY --from=go-build /out/turbk-agent /usr/local/bin/turbk-agent

USER root
ENTRYPOINT ["/usr/local/bin/turbk-agent"]
