#!/usr/bin/env bash
# Builds the three services. Needs ff-logistic-base and ff-logistic-shared installed first
# (../base/build.sh, then ../shared/build.sh): each service runs one of shared's domain cores.
# The gateway is nginx and has nothing to build here (gateway/Dockerfile).
#
#   ./build.sh
#   ./build.sh -DskipTests
set -euo pipefail
cd "$(dirname "$0")"
./mvnw -B install "$@"
