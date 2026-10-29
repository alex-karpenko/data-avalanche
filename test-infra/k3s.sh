#!/usr/bin/env bash
# Starts the smoke deploy's k3s (compose profile k3s) beside test-infra's containers, waits for
# its node, builds the image (unless --no-build: CI builds it with its cache) and imports it into
# k3s's containerd. test-infra/up.sh runs first. The kubeconfig: test-infra/.k3s/kubeconfig.yaml.
set -euo pipefail
cd "$(dirname "$0")"
docker compose --profile k3s up -d k3s
export KUBECONFIG="$PWD/.k3s/kubeconfig.yaml"
for _ in $(seq 1 90); do
  [ -f "$KUBECONFIG" ] && kubectl wait --for=condition=Ready node/k3s --timeout=2s >/dev/null 2>&1 && break
  sleep 2
done
kubectl wait --for=condition=Ready node/k3s --timeout=10s
if [ "${1:-}" != --no-build ]; then
  docker build -f ../deploy/docker/Dockerfile -t data-avalanche:k3s ..
fi
docker save data-avalanche:k3s | docker compose --profile k3s exec -T k3s ctr images import -
