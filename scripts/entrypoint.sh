#!/bin/sh
set -e

echo "Starting Telemetry Validator Service..."
echo "Starting command: $*"
exec "$@"
