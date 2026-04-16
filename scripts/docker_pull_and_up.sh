#!/usr/bin/env bash
set -euo pipefail

# Pull and start stack from prebuilt images without rebuilding.
# Requires PIPELINE_IMAGE and SPARK_IMAGE in environment or .env/.env.images.
# Usage:
#   scripts/docker_pull_and_up.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ -f .env.images ]]; then
  echo "Using .env.images for image overrides"
  docker compose --env-file .env --env-file .env.images pull
  docker compose --env-file .env --env-file .env.images up -d --no-build
else
  echo "No .env.images found, using current environment/.env"
  docker compose pull
  docker compose up -d --no-build
fi

docker compose ps
