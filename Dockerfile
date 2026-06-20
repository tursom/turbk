FROM node:22-alpine AS web-build
WORKDIR /src/web
COPY web/package*.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM golang:1.26-alpine AS go-build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
COPY --from=web-build /src/web/dist /src/web/dist
ARG VERSION=0.1.0-dev
ARG COMMIT=unknown
ARG DATE=unknown
RUN CGO_ENABLED=0 go build \
    -ldflags="-s -w -X github.com/turbk/turbk/internal/version.Version=${VERSION} -X github.com/turbk/turbk/internal/version.Commit=${COMMIT} -X github.com/turbk/turbk/internal/version.Date=${DATE}" \
    -o /out/turbk ./cmd/turbk \
 && CGO_ENABLED=0 go build \
    -ldflags="-s -w -X github.com/turbk/turbk/internal/version.Version=${VERSION} -X github.com/turbk/turbk/internal/version.Commit=${COMMIT} -X github.com/turbk/turbk/internal/version.Date=${DATE}" \
    -o /out/turbk-agent ./cmd/turbk-agent

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata \
 && mkdir -p /var/lib/turbk/state /var/lib/turbk/repo /var/lib/turbk/restore /app/web/dist
COPY --from=go-build /out/turbk /usr/local/bin/turbk
COPY --from=go-build /out/turbk-agent /usr/local/bin/turbk-agent
COPY --from=web-build /src/web/dist /app/web/dist

USER root
EXPOSE 8080
ENV TURBK_LISTEN=:8080 \
    TURBK_PUBLIC_URL=http://localhost:8080 \
    TURBK_WEB_DIR=/app/web/dist \
    TURBK_ADMIN_USERNAME=admin \
    TURBK_SESSION_TTL_HOURS=24 \
    TURBK_STATE_DIR=/var/lib/turbk/state \
    TURBK_REPO_DIR=/var/lib/turbk/repo \
    TURBK_RESTORE_ROOTS=/var/lib/turbk/restore
ENTRYPOINT ["/usr/local/bin/turbk"]
