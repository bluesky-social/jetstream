# syntax=docker/dockerfile:1.7

# ──────────────────────────────────────────────────────────────────────────────
# Build stage
# ──────────────────────────────────────────────────────────────────────────────
FROM --platform=$BUILDPLATFORM golang:1.26-bookworm AS build

# Build-args set by `docker buildx build` (or stamped manually from CI).
# VERSION/COMMIT/DATE are linked into internal/version via -ldflags.
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown

# TARGETOS/TARGETARCH are provided automatically by buildx and let us
# cross-compile for arm64/amd64 from a single Dockerfile.
ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

# Copy go.{mod,sum} first so the dependency-download layer caches independently
# of source changes. Module downloads are the slowest step in a clean build.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

# Now copy the rest of the source. Anything in .dockerignore stays out.
COPY . .

# Static, stripped, reproducible build:
#   - CGO_DISABLED so the binary has no glibc dependency and runs on distroless/static.
#   - -trimpath strips $GOPATH/$HOME from recorded file paths for reproducibility.
#   - -buildvcs=false avoids stamping VCS info we already inject via ldflags
#     (and avoids surprising failures when /src isn't a git checkout).
#   - -extldflags=-static is belt-and-suspenders for fully static linking.
ENV CGO_ENABLED=0
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build \
      -trimpath \
      -buildvcs=false \
      -ldflags " \
        -extldflags=-static \
        -X github.com/bluesky-social/jetstream/internal/version.Version=${VERSION} \
        -X github.com/bluesky-social/jetstream/internal/version.Commit=${COMMIT} \
        -X github.com/bluesky-social/jetstream/internal/version.Date=${DATE}" \
      -o /out/jetstream \
      ./cmd/jetstream

# ──────────────────────────────────────────────────────────────────────────────
# Runtime stage
#
# distroless/static contains: CA certs, /etc/passwd with a `nonroot` user
# (UID 65532), tzdata, and nothing else. No shell, no package manager, no
# busybox — minimal attack surface, smaller image, and clearly authored for
# Go binaries that don't need libc.
# ──────────────────────────────────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

# Bring the binary across. /usr/local/bin is on $PATH and is the conventional
# location even though distroless has no PATH-based shell to care.
COPY --from=build /out/jetstream /usr/local/bin/jetstream

# Document the listeners. EXPOSE is metadata only; it does not publish ports.
EXPOSE 8080 6060

# Run as the unprivileged nonroot user baked into distroless. Setting it
# explicitly (rather than relying on the base image default) makes the
# security posture obvious from `docker inspect`.
USER 65532:65532

# Use the binary as PID 1 directly. No tini/dumb-init: Go's runtime handles
# signals and reaps no children of its own.
ENTRYPOINT ["/usr/local/bin/jetstream"]
CMD ["serve"]
