# Runtime-only image. Binaries are built on the host via scripts/build-image.{sh,ps1}.
FROM docker.io/library/alpine:3.24.1

RUN apk add --no-cache ca-certificates tzdata \
    && mkdir -p /var/lib/spider /data/models /data/output

WORKDIR /app

COPY dist/linux/tracker \
     dist/linux/spiderd \
     dist/linux/spiderctl \
     /usr/local/bin/

VOLUME ["/var/lib/spider", "/data"]

CMD ["/usr/local/bin/spiderd"]
