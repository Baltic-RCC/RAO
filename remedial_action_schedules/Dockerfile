# Define base container image (platform has to be defined if using with ARM64 architecture)
FROM python:3.11-slim
#FROM --platform=linux/amd64 python:3.11-slim

# Install tools to set up python environment
RUN apt-get update && apt-get install -y --no-install-recommends procps && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade setuptools pip uv

# Set working directory
WORKDIR /app

# Sync python modules as dependencies
COPY pylock.toml .
RUN uv pip sync pylock.toml --system  && uv cache clean

# Copy files to container
COPY config/ ./config/
COPY common/ ./common/
COPY integrations/ ./integrations/
COPY remedial_action_schedules/ ./remedial_action_schedules/

# Start process
ENV PYTHONPATH="${PYTHONPATH}:/app"
ENTRYPOINT ["python"]
CMD ["remedial_action_schedules/worker.py"]
#ENTRYPOINT ["/bin/bash"]