#!/bin/sh
set -eu

exec /usr/local/bin/place mount \
    --hot=/mnt/hot \
    --cold=/mnt/cold \
    --mount=/mnt/place \
    --debug=false \
    --fuse-debug=false \
    --pprof-addr= \
    "$@"
