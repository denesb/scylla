#!/bin/bash

path=$(dirname $0)
bin=$(basename $0)

exec -a $bin ${path}/../scylla -c1 -m2G --overprovisioned --blocked-reactor-notify-ms=99999 --relaxed-dma --unsafe-bypass-fsync=1 --mbind=0 --lock-memory=0 --idle-poll-time-us=0 --poll-aio=0 "$@"
