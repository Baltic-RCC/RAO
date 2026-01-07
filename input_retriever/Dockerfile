# Define base container image (platform has to be defined if using with ARM64 architecture)
FROM balticrcc/rao-base:latest
#FROM --platform=linux/amd64 python:3.11-slim

# Set working directory
WORKDIR /app

# Copy files to container
COPY config/ ./config/
COPY common/ ./common/
COPY integrations/ ./integrations/
COPY input_retriever/ ./input_retriever/

# Start process
ENV PYTHONPATH="${PYTHONPATH}:/app"
ENTRYPOINT ["python"]
CMD ["input_retriever/worker.py"]
#ENTRYPOINT ["/bin/bash"]