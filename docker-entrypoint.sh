#!/bin/sh
set -eu

# Hot/cold/mount paths are fixed by the volume layout in docker-compose.yml.
# Extra placefs flags (--hot-max-bytes, --tick, --admin-addr, etc.) can
# be appended via `command:` in compose; they land in "$@".
exec /usr/local/bin/placefs \
    --hot=/mnt/hot \
    --cold=/mnt/cold \
    --mount=/mnt/place \
    "$@"
