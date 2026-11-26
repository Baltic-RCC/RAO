# CI-only additions to docker-bake.hcl: GitHub Actions layer cache, one scope per image so the exports
# don't overwrite each other. Not used for local builds (the gha cache backend only exists on runners).
#
#   docker buildx bake -f docker-bake.hcl -f .github/docker-bake.ci.hcl

target "base" {
  cache-from = ["type=gha,scope=rao-base"]
  cache-to   = ["type=gha,scope=rao-base,mode=max"]
}

target "input-retriever" {
  cache-from = ["type=gha,scope=rao-input-retriever"]
  cache-to   = ["type=gha,scope=rao-input-retriever,mode=max"]
}

target "rao" {
  cache-from = ["type=gha,scope=rao"]
  cache-to   = ["type=gha,scope=rao,mode=max"]
}
