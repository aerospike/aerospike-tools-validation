# syntax=docker/dockerfile:1
#
# Aerospike asvalidation - Docker image.
#
# https://github.com/aerospike/aerospike-tools-validation
#
# Supports linux/amd64 and linux/arm64. Built by the Build and Release
# workflow from the repo root, which drops the signed .deb under
# deploy-artifacts/ and passes its path per arch as ASVALIDATION_LOCAL_PKG_*.
#
# Base: ubuntu:24.04, the distro the .deb is built on.
#

FROM ubuntu:24.04@sha256:224a1869083a311ef3f13648a154ba79832fbef6364d31493642ca03082da254

ARG ASVALIDATION_LOCAL_PKG_AMD64=""
ARG ASVALIDATION_LOCAL_PKG_ARM64=""
# Path of the entrypoint script inside the build context. .dockerignore
# re-includes exactly this file.
ARG ENTRYPOINT_SRC=docker/docker-entrypoint.sh

LABEL org.opencontainers.image.title="Aerospike asvalidation" \
      org.opencontainers.image.description="asvalidation, a tool that validates and optionally repairs Complex Data Type (CDT) bins in an Aerospike database." \
      org.opencontainers.image.source="https://github.com/aerospike/aerospike-tools-validation" \
      org.opencontainers.image.base.name="docker.io/library/ubuntu:24.04" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.vendor="Aerospike"

# glibc otherwise resolves the POSIX locale, whose ANSI_X3.4-1968 charmap strips
# non-ASCII from tool output.
ENV LANG=C.UTF-8 \
    LC_CTYPE=C.UTF-8

SHELL ["/bin/bash", "-Eeuo", "pipefail", "-c"]

# image-info is the usage title docker-entrypoint.sh prints, so it names the
# package version that is actually installed.
# hadolint ignore=DL3008
RUN --mount=type=bind,source=.,target=/build-ctx \
    export DEBIAN_FRONTEND=noninteractive && \
    ARCH="$(dpkg --print-architecture)" && \
    if [ "${ARCH}" = "amd64" ]; then \
        LOCAL_PKG="${ASVALIDATION_LOCAL_PKG_AMD64}"; \
    else \
        LOCAL_PKG="${ASVALIDATION_LOCAL_PKG_ARM64}"; \
    fi && \
    if [ -z "${LOCAL_PKG}" ] || [ ! -f "/build-ctx/${LOCAL_PKG}" ]; then \
        echo "ERROR: '${LOCAL_PKG}' not in the build context." >&2; \
        echo "Available .deb: $(find /build-ctx -name '*.deb' -printf '%P ' 2>/dev/null)" >&2; \
        echo "The name is derived by the caller from pkg_release.sh; fix the derivation." >&2; \
        exit 1; \
    fi && \
    cp "/build-ctx/${LOCAL_PKG}" /tmp/asvalidation.deb && \
    apt-get update -y && \
    apt-get install -y --no-install-recommends /tmp/asvalidation.deb && \
    command -v asvalidation >/dev/null && \
    PKG_NAME="$(dpkg-deb -f /tmp/asvalidation.deb Package)" && \
    PKG_VERSION="$(dpkg-query -W -f='${Version}' "${PKG_NAME}")" && \
    mkdir -p /usr/local/share/asvalidation && \
    printf 'Aerospike asvalidation image (%s %s)\n' "${PKG_NAME}" "${PKG_VERSION}" \
      > /usr/local/share/asvalidation/image-info && \
    rm -f /tmp/asvalidation.deb && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# The .deb installs /usr/bin/asvalidation; it is on PATH.
# Convention: `docker run --rm IMAGE asvalidation [args]`. No CMD, so running
# the image with no arguments prints usage. Exec form so the script -- and,
# after exec, asvalidation -- is PID 1 and gets `docker stop`'s SIGTERM.
COPY --chmod=0755 ${ENTRYPOINT_SRC} /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
