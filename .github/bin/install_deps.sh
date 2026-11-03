#!/usr/bin/env bash
set -xeuo pipefail

# Keep in step with OPENSSL_STATIC_PATH in .build.yml: asvalidation links this
# OpenSSL statically, so the packages need no libssl of a particular version.
OPENSSL_VERSION="3.0.19"
OPENSSL_SHA256="fa5a4143b8aae18be53ef2f3caf29a2e0747430b8bc74d32d88335b94ab63072"

BUILD_DEPS_DEBIAN="build-essential make git curl ca-certificates binutils perl libyaml-dev zlib1g-dev"
BUILD_DEPS_REDHAT="gcc make git ca-certificates binutils perl perl-IPC-Cmd perl-Time-Piece zlib-devel rpm-build which tar gzip findutils"

# Installs headers and static libraries under /usr/local; the top-level
# Makefile links them through OPENSSL_STATIC_PATH=/usr/local/lib.
function build_openssl_static() {
	pushd /tmp
	curl -fsSL --retry 3 "https://github.com/openssl/openssl/releases/download/openssl-${OPENSSL_VERSION}/openssl-${OPENSSL_VERSION}.tar.gz" -o openssl.tar.gz
	echo "${OPENSSL_SHA256}  openssl.tar.gz" | sha256sum -c -
	tar -xzf openssl.tar.gz
	cd "openssl-${OPENSSL_VERSION}"
	./config no-shared no-tests -fPIC --prefix=/usr/local --libdir=lib
	make -j"$(nproc)"
	make install_sw
	popd
	rm -rf "/tmp/openssl-${OPENSSL_VERSION}" /tmp/openssl.tar.gz
}

# EL and Amazon Linux ship libyaml-devel without a static library (and UBI
# without libyaml-devel at all), so build one.
function build_libyaml_static() {
	local LIBYAML_VERSION="0.2.5"
	local LIBYAML_SHA256="c642ae9b75fee120b2d96c712538bd2cf283228d2337df2cf2988e3c02678ef4"
	pushd /tmp
	curl -fsSL --retry 3 "https://github.com/yaml/libyaml/releases/download/${LIBYAML_VERSION}/yaml-${LIBYAML_VERSION}.tar.gz" -o yaml.tar.gz
	echo "${LIBYAML_SHA256}  yaml.tar.gz" | sha256sum -c -
	tar -xzf yaml.tar.gz
	cd "yaml-${LIBYAML_VERSION}"
	./configure --prefix=/usr --libdir=/usr/lib64 --enable-static --disable-shared CFLAGS="-O2 -fPIC"
	make -j"$(nproc)"
	make install
	popd
	rm -rf "/tmp/yaml-${LIBYAML_VERSION}" /tmp/yaml.tar.gz
}

function install_deps_debian_like() {
	rm -rf /var/lib/apt/lists/*
	apt-get clean
	apt-get update -o Acquire::Retries=5
	apt-get install -y --no-install-recommends $BUILD_DEPS_DEBIAN "$@"
	build_openssl_static
	apt-get clean
	rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*
}

function install_deps_redhat_like() {
	dnf clean all
	dnf -y install $BUILD_DEPS_REDHAT
	build_libyaml_static
	build_openssl_static
	dnf clean all
}

function install_deps_debian12() {
	install_deps_debian_like
}

function install_deps_debian13() {
	install_deps_debian_like
}

function install_deps_ubuntu22.04() {
	install_deps_debian_like
}

function install_deps_ubuntu24.04() {
	install_deps_debian_like
}

function install_deps_ubuntu26.04() {
	# libterm-readline-perl-perl: provides Term::ReadLine so debconf can use
	# the Readline frontend instead of printing frontend-init warnings.
	install_deps_debian_like libterm-readline-perl-perl
}

function install_deps_el8() {
	install_deps_redhat_like
}

function install_deps_el9() {
	install_deps_redhat_like
}

function install_deps_el10() {
	install_deps_redhat_like
}

function install_deps_amzn2023() {
	install_deps_redhat_like
}
