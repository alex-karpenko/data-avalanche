#!/usr/bin/env bash
# Keep the host ports compose.yaml publishes out of the kernel's ephemeral range (Linux only).
#
# Every published port but Azurite's lies inside Linux's default ephemeral range (32768-60999).
# An outbound connection (an image pull, a crate download) can be given one of them, and a
# container published on it then fails to start: "failed to bind host port ... address already
# in use". Reserved ports are never given out as ephemeral ones; explicit binds, Docker's
# included, still take them. CI runs this right after checkout, before any container starts.
# Needs root (sudo); the reservation lasts until the machine restarts.
set -euo pipefail
cd "$(dirname "$0")"

if [[ "$(uname -s)" != Linux ]]; then
  echo "reserve-ports: not Linux, nothing to do"
  exit 0
fi

# Every profile's published ports: a service added to compose.yaml is covered without an edit here.
profiles=()
for p in $(docker compose config --profiles); do profiles+=(--profile "$p"); done
ports=$(docker compose ${profiles[@]+"${profiles[@]}"} config --format json |
  jq -r '[.services[].ports[]?.published | select(. != null) | tonumber] | unique | map(tostring) | join(",")')
if [[ -z "$ports" ]]; then
  echo "reserve-ports: compose.yaml publishes no ports" >&2
  exit 1
fi

current=$(cat /proc/sys/net/ipv4/ip_local_reserved_ports)
sudo sysctl -q -w net.ipv4.ip_local_reserved_ports="${current:+$current,}$ports"
echo "reserve-ports: reserved $(cat /proc/sys/net/ipv4/ip_local_reserved_ports)"

# A socket that took one of the ports before the reservation still holds it: name it, so a
# container that then fails to bind says why.
filter=$(sed 's/[0-9][0-9]*/sport = :&/g; s/,/ or /g' <<<"$ports")
taken=$(ss -Htuan "( $filter )" || true)
if [[ -n "$taken" ]]; then
  echo "::warning::test-infra host ports already in use before the reservation:"
  echo "$taken"
fi
