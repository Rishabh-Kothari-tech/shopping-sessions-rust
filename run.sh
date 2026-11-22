#!/bin/sh
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
export MIMALLOC_ARENA_EAGER_COMMIT=1
export MIMALLOC_PURGE_DELAY=-1
export MIMALLOC_ALLOW_LARGE_OS_PAGES=1
exec /usr/local/bin/shopping-sessions
