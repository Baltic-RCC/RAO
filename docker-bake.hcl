# Single definition of how RAO images are built: locally, on pull requests and on releases.
#
#   docker buildx bake                       # build the services (base is built as their dependency)
#   docker buildx bake --load                # ... and load them into the local docker
#   TAGS=1.2.0,1.2,latest docker buildx bake release --push
#
# remedial_action_schedules is intentionally not part of this file: it is not a deployed service.

variable "REGISTRY" {
  default = "balticrcc"
}

# Comma separated image tags, every target gets all of them
variable "TAGS" {
  default = "dev"
}

variable "APP_VERSION" {
  default = "dev"
}

variable "GIT_SHA" {
  default = "unknown"
}

group "default" {
  targets = ["input-retriever", "rao"]
}

# What gets published on a release: services plus the base image they are built on
group "release" {
  targets = ["base", "input-retriever", "rao"]
}

target "_common" {
  context = "."
  labels = {
    "org.opencontainers.image.source"   = "https://github.com/Baltic-RCC/RAO"
    "org.opencontainers.image.licenses" = "MPL-2.0"
    "org.opencontainers.image.version"  = APP_VERSION
    "org.opencontainers.image.revision" = GIT_SHA
  }
}

# Base image takes no build metadata as ARG, so the slow dependency layers stay cached across commits
target "base" {
  inherits   = ["_common"]
  dockerfile = "Dockerfile"
  tags       = [for t in split(",", TAGS) : "${REGISTRY}/rao-base:${t}"]
}

target "_service" {
  inherits = ["_common"]
  args = {
    APP_VERSION = APP_VERSION
    GIT_SHA     = GIT_SHA
  }
  # Service Dockerfiles say "FROM balticrcc/rao-base:latest": build with the base from this very build,
  # not whatever "latest" happens to be in the registry.
  contexts = {
    "balticrcc/rao-base:latest" = "target:base"
  }
}

target "input-retriever" {
  inherits   = ["_service"]
  dockerfile = "input_retriever/Dockerfile"
  tags       = [for t in split(",", TAGS) : "${REGISTRY}/rao-input-retriever:${t}"]
}

target "rao" {
  inherits   = ["_service"]
  dockerfile = "rao/Dockerfile"
  tags       = [for t in split(",", TAGS) : "${REGISTRY}/rao:${t}"]
}
