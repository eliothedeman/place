#!/bin/sh
set -eu

# placefs picks up the legacy --hot/--cold/--mount paths verbatim. If the
# bound /mnt/hot still contains old-format data (.place.db + .place/segments)
# from a previous deploy of the legacy `place mount` binary, placefs runs
# the in-place migrator on first start before mounting — no separate step
# is required.
exec /usr/local/bin/placefs \
    --hot=/mnt/hot \
    --cold=/mnt/cold \
    --mount=/mnt/place \
    "$@"
