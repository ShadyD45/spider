# Multi-stage Containerfile for Spider Artifact Mesh
FROM golang:1.22-alpine AS builder

WORKDIR /build

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /build/bin/tracker ./cmd/tracker
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /build/bin/spiderd ./cmd/spiderd
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /build/bin/spiderctl ./cmd/spiderctl
RUN cp /build/bin/spiderd /build/bin/artifactd && cp /build/bin/spiderctl /build/bin/artifactctl

FROM alpine:latest

RUN apk add --no-cache ca-certificates bash curl tzdata

WORKDIR /app

COPY --from=builder /build/bin/* /usr/local/bin/
COPY --from=builder /build/bin/* /app/

RUN mkdir -p /var/lib/artifactd /data/models /data/output

VOLUME ["/var/lib/artifactd", "/data"]

CMD ["/usr/local/bin/spiderd"]
