# Base image with third-party dependencies, shared by all RAO services.
# Digest-pinned for reproducible builds; Dependabot (docker ecosystem) proposes digest updates.
# Platform has to be defined if using with ARM64 architecture: FROM --platform=linux/amd64 ...
FROM python:3.11-slim@sha256:61e01f72680f86a5279ad510818ab7b2ee9f049822589d4e34e33e044360356c AS rao-base

# Set non root user
ARG GID=1000
ARG UID=1000
RUN groupadd -g ${GID} appuser && useradd --create-home --no-log-init -u ${UID} -g ${GID} appuser

# Install tools to set up python environment
RUN apt-get update && apt-get install -y --no-install-recommends procps && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --upgrade setuptools pip uv

# Set working directory
WORKDIR /app

# Sync python modules as dependencies
# --frozen: use uv.lock as is, never re-resolve (pyproject lists an internal index not reachable from CI)
# --no-dev: keep test tooling (pytest) out of the runtime image
COPY pyproject.toml .
COPY uv.lock .
RUN uv export --frozen --no-dev -o pylock.toml
RUN uv pip sync pylock.toml --system && uv cache clean

# Copy license file and dependencies license reference
RUN mkdir -p /licenses
COPY LICENSE /licenses/
COPY licenses/ /licenses/
