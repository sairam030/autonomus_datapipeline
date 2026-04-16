#!/usr/bin/env bash
set -euo pipefail

# Build and push the two local images used by this compose stack.
# Usage:
#   scripts/docker_build_and_push.sh <dockerhub_user> [tag]
# Example:
#   scripts/docker_build_and_push.sh knight123 v1

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <dockerhub_user> [tag]"
  exit 1
fi

DOCKER_USER="$1"
TAG="${2:-latest}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PIPELINE_IMAGE="${DOCKER_USER}/autonomous-pipeline:${TAG}"
SPARK_IMAGE="${DOCKER_USER}/autonomous-spark:${TAG}"

echo "[1/4] Building ${PIPELINE_IMAGE}"
docker build -t "${PIPELINE_IMAGE}" "${ROOT_DIR}"

echo "[2/4] Building ${SPARK_IMAGE}"
docker build -t "${SPARK_IMAGE}" "${ROOT_DIR}"

echo "[3/4] Pushing ${PIPELINE_IMAGE}"
docker push "${PIPELINE_IMAGE}"

echo "[4/4] Pushing ${SPARK_IMAGE}"
docker push "${SPARK_IMAGE}"

cat <<EOF

Done.
Use these in your env when deploying:
PIPELINE_IMAGE=${PIPELINE_IMAGE}
SPARK_IMAGE=${SPARK_IMAGE}

EOF
