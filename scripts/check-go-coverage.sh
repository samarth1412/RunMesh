#!/usr/bin/env bash
set -euo pipefail

profile=${1:?usage: check-go-coverage.sh PROFILE MINIMUM}
minimum=${2:?usage: check-go-coverage.sh PROFILE MINIMUM}
coverage=$(go tool cover -func="$profile" | awk '/^total:/ {gsub("%", "", $3); print $3}')

awk -v coverage="$coverage" -v minimum="$minimum" 'BEGIN {
  if (coverage + 0 < minimum + 0) {
    printf "coverage %.1f%% is below required %.1f%%\n", coverage, minimum > "/dev/stderr"
    exit 1
  }
  printf "coverage %.1f%% meets required %.1f%%\n", coverage, minimum
}'
