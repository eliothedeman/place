# syntax=docker/dockerfile:1

FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/place ./cmd/place

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends fuse3 ca-certificates tini \
 && rm -rf /var/lib/apt/lists/* \
 && ln -sf /usr/bin/fusermount3 /usr/local/bin/fusermount

COPY --from=build /out/place /usr/local/bin/place
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

VOLUME ["/mnt/hot", "/mnt/cold", "/mnt/place"]

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
