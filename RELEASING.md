# Releasing

One version for the whole repository. `pyproject.toml` is the source of truth, the git tag `vX.Y.Z` must match it,
and every image is tagged from it.

## Images

| Image | Content | Command |
|---|---|---|
| `balticrcc/rao-input-retriever` | input retriever worker | `input_retriever/worker.py` |
| `balticrcc/rao` | RAO operator and optimizer workers | operator by default, optimizer: `rao/worker_optimizer.py` |
| `balticrcc/rao-base` | third-party dependencies, base of the images above | – |

`remedial_action_schedules` is not a deployed service and is not built.

## Tags

| Event | Workflow | Tags | Moves? |
|---|---|---|---|
| pull request | `ci.yml` | nothing is published | – |
| push to `main` | `ci.yml` | `edge`, `sha-<7 chars>` | `edge` moves, `sha-*` never |
| manual run on a branch | `publish-dev.yml` | `dev-<branch>`, `sha-<7 chars>` | `dev-<branch>` moves, `sha-*` never |
| tag `v1.4.0` | `release.yml` | `1.4.0`, `1.4`, `latest` | `1.4.0` never; `1.4` moves with patch releases; `latest` moves to the highest stable version |
| tag `v1.5.0-rc.1` | `release.yml` | `1.5.0-rc.1` | never |

Deployments should pin `X.Y.Z` (or the image digest), not `latest` or `edge`.
Every image carries `APP_VERSION` / `GIT_SHA` as environment variables and OCI labels; the same values are added to every
log document sent to Elastic as `app_version` and `git_sha`.

## Testing in your own infrastructure before a release

Three ways to get images, from least to most formal. All of them are built, smoke tested and pushed by CI, with the
same SBOM and provenance attestations as a release.

1. **Latest `main`**: every push to `main` publishes `edge` (moving) and `sha-<7 chars>` (that exact commit).
2. **A branch, before merging**: Actions -> *Publish test build* -> *Run workflow* -> choose the branch. You get
   `dev-<branch>` (slashes become `-`, `feature/x` -> `dev-feature-x`) and `sha-<7 chars>`. Images are tested by the
   smoke test only, not by pytest, so a work-in-progress branch can be published. The workflow file has to exist on
   `main`, and the branch needs `docker-bake.hcl`: merge or rebase `main` into older branches first.
3. **A release candidate**: tag `v1.5.0-rc.1` while `pyproject.toml` says `1.5.0`. This runs the full release pipeline
   (tests, scan, attestations) and publishes `1.5.0-rc.1` plus a GitHub pre-release; it never moves `latest` or `1.5`.
   Use it to test the exact version you are about to release, then tag `v1.5.0` on the same commit.

Test tags (`dev-*`, `sha-*`, `*-rc.*`) are not cleaned up automatically, delete old ones in Docker Hub from time to time.

## Cutting a release

1. Make sure `main` is green.
2. Open a PR that bumps the version (updates `pyproject.toml` and `uv.lock`), merge it:
   ```bash
   uv version 1.4.0
   ```
3. Tag the merge commit and push the tag:
   ```bash
   git tag v1.4.0 <merge-commit>
   git push origin v1.4.0
   ```
4. `release.yml` verifies tag against `pyproject.toml` and that the commit is on `main`, runs tests, builds, smoke
   tests and scans the images, pushes them with SBOM and provenance and creates the GitHub release with generated notes.
   A release created by hand in the GitHub UI beforehand is left as it is.

Pre-release: use a suffix in the tag (`v1.5.0-rc.1`) while `pyproject.toml` says `1.5.0`.

## Vulnerability gate

Trivy findings are reported only until the repository variable `TRIVY_GATE_ENABLED` is set to `true`. From then on the
release fails on CRITICAL vulnerabilities that have a fix available. Accepted risks go to `.trivyignore`, with a comment
saying why and until when.

## Building locally

```bash
docker buildx bake --load                                     # input-retriever and rao, tagged :dev
TAGS=local docker buildx bake release --load                  # including rao-base
bash scripts/smoke_images.sh dev                              # same smoke test as CI
```

## Repository settings this depends on

- Variable `DOCKER_USERNAME`, secret `DOCKER_PASSWORD` (preferably a Docker Hub access token limited to the `balticrcc` repositories).
- Branch protection on `main` requiring the `CI` checks (`test / pytest`, `Build, smoke test and scan images`).
