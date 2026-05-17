# syntax=docker/dockerfile:1

FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/placefs ./cmd/placefs

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends fuse3 ca-certificates tini wget \
 && rm -rf /var/lib/apt/lists/* \
 && ln -sf /usr/bin/fusermount3 /usr/local/bin/fusermount

COPY --from=build /out/placefs /usr/local/bin/placefs
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

VOLUME ["/mnt/hot", "/mnt/cold", "/mnt/place"]
EXPOSE 9090

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
