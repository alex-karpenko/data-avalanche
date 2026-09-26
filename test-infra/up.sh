#!/usr/bin/env bash
# Start the integration-test infrastructure and create the test bucket in every emulator.
#   test-infra/up.sh            default profile
#   test-infra/up.sh matrix     default + older engine versions
set -euo pipefail
cd "$(dirname "$0")"
profiles=()
if [[ "${1:-}" == "matrix" ]]; then profiles=(--profile matrix); fi
# ${a[@]+...} keeps `set -u` happy with an empty array on macOS bash 3.2.
docker compose ${profiles[@]+"${profiles[@]}"} up -d --wait
for init in rustfs-init azurite-init; do
  docker compose --profile init run --rm "$init"
done
echo "test-infra ready — export DATA_AVALANCHE_IT=1 to run integration tests"
