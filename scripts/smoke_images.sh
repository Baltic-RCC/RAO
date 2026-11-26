#!/usr/bin/env bash
# Smoke test of the built RAO images: the container starts, imports its own code and
# the installed pypowsybl version is supported by RAO parameters (RAO_PARAMETERS_VERSION_MAP).
# Worker modules are not imported on purpose: they connect to RabbitMQ at import time.
#
# usage: scripts/smoke_images.sh <tag> [registry]      e.g. scripts/smoke_images.sh dev balticrcc
#        EXPECTED_VERSION=1.2.0 scripts/smoke_images.sh 1.2.0
set -euo pipefail

TAG="${1:?usage: $0 <tag> [registry]}"
REGISTRY="${2:-balticrcc}"
EXPECTED_VERSION="${EXPECTED_VERSION:-}"

# Importing integrations.rmq / the S3 helpers builds their settings at import time and those have required
# credentials (see config/settings.py). Dummy values are enough: nothing connects during the imports.
# A developer machine usually has config/.env, which is not part of the images, so check this in a container.
DUMMY_SETTINGS=(
  -e RMQ_USERNAME=smoke -e RMQ_PASSWORD=smoke
  -e MINIO_USERNAME=smoke -e MINIO_PASSWORD=smoke
)

run_in() {
  local image="$1" code="$2"
  echo "::group::smoke test ${image}"
  docker run --rm -e EXPECTED_VERSION "${DUMMY_SETTINGS[@]}" --entrypoint python "${image}" -c "${code}"
  echo "::endgroup::"
}

version_check='
import os, sys
expected = os.environ.get("EXPECTED_VERSION")
actual = os.environ.get("APP_VERSION")
if expected and actual != expected:
    sys.exit(f"APP_VERSION is {actual!r}, expected {expected!r}")
print("APP_VERSION =", actual, "| GIT_SHA =", os.environ.get("GIT_SHA"))
'

run_in "${REGISTRY}/rao:${TAG}" "
${version_check}
import config, integrations.rmq, rao.handlers
import pypowsybl
from rao.parameters.manager import RaoSettingsManager, LoadflowSettingsManager
RaoSettingsManager()
LoadflowSettingsManager().build_pypowsybl_parameters()
print('rao image OK, pypowsybl', pypowsybl.__version__)
"

run_in "${REGISTRY}/rao-input-retriever:${TAG}" "
${version_check}
import config, integrations.rmq, input_retriever.handlers
print('input-retriever image OK')
"

# Test tooling must not leak into runtime images
echo "::group::runtime image content"
for image in "${REGISTRY}/rao:${TAG}" "${REGISTRY}/rao-input-retriever:${TAG}"; do
  if docker run --rm --entrypoint python "${image}" -c "import pytest" 2>/dev/null; then
    echo "pytest found in ${image}" >&2
    exit 1
  fi
done
echo "no test tooling in runtime images"
echo "::endgroup::"
