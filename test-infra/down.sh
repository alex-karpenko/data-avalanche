#!/usr/bin/env bash
# Stop everything and drop volumes (all profiles).
set -euo pipefail
cd "$(dirname "$0")"
docker compose --profile matrix --profile init down -v --remove-orphans
