# Base image is replaced by the freshly built one when building with docker-bake.hcl (see "contexts" there)
# Platform has to be defined if using with ARM64 architecture: FROM --platform=linux/amd64 ...
FROM balticrcc/rao-base:latest

# Build metadata, set by CI (kept after the heavy layers so they stay cached)
ARG APP_VERSION=dev
ARG GIT_SHA=unknown

# Set working directory
WORKDIR /app
RUN chown appuser:appuser /app

# Copy files to container, owned by non-root user
COPY --chown=appuser:appuser config/ ./config/
COPY --chown=appuser:appuser common/ ./common/
COPY --chown=appuser:appuser integrations/ ./integrations/
COPY --chown=appuser:appuser input_retriever/ ./input_retriever/

USER appuser

# Start process
ENV PYTHONPATH="${PYTHONPATH:+${PYTHONPATH}:}/app" \
    APP_VERSION=${APP_VERSION} \
    GIT_SHA=${GIT_SHA}
ENTRYPOINT ["python"]
CMD ["input_retriever/worker.py"]
#ENTRYPOINT ["/bin/bash"]
