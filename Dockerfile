# Define base container image (platform has to be defined if using with ARM64 architecture)
FROM python:3.11-slim AS rao-base
#FROM --platform=linux/amd64 python:3.11-slim as rao-base

# Install tools to set up python environment
RUN apt-get update && apt-get install -y --no-install-recommends procps && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade setuptools pip uv

# Set working directory
WORKDIR /app

# Sync python modules as dependencies
COPY pyproject.toml .
COPY uv.lock .
RUN uv export -o pylock.toml
RUN uv pip sync pylock.toml --system  && uv cache clean

# Copy license file and dependencies license reference
RUN mkdir -p /licenses
COPY LICENSE /licenses/
COPY licenses/ /licenses/
COPY NOTICE /licenses/
