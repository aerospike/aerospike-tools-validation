# Builds the asvalidation packages from bin/asvalidation:
#   make -C pkg deb      dpkg-deb, for Debian and Ubuntu
#   make -C pkg rpm      rpmbuild, for EL and Amazon Linux
#   make -C pkg osx-pkg  pkgbuild, for macOS
# dpkg-deb and rpmbuild rather than fpm: the internal build images that
# .build.yml uses have them and have no fpm.

# Variables required for this Makefile
PKG_DIR = $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
BIN = $(PKG_DIR)/../bin/asvalidation
BUILD_DIR = $(PKG_DIR)/build
STAGE_DIR = $(BUILD_DIR)/root
TARGET_DIR = $(PKG_DIR)/../target/packages
# Package variables
BUILD_DISTRO ?= $(shell $(PKG_DIR)/../.github/bin/os_version.sh)
# Packages up to 2.2.1 were named asvalidation. deb/control and
# rpm/asvalidation.spec replace that package, so apt and dnf swap it out
# instead of installing both.
NAME = aerospike-asvalidation
# Same source as the binary's version (top-level Makefile). CI passes
# VERSION=<BUILD_VERSION> on the command line to both.
VERSION := $(shell $(PKG_DIR)/../build/version -v)
ARCH = $(shell uname -m)
# dpkg and rpm spell architectures differently: dpkg wants amd64/arm64
# where uname and rpm say x86_64/aarch64. macOS uname -m already reports
# arm64 -- the dpkg spelling, but not rpm's.
DEB_ARCH = $(patsubst aarch64,arm64,$(patsubst x86_64,amd64,$(ARCH)))
RPM_ARCH = $(patsubst arm64,aarch64,$(ARCH))
# Package names never carry the "rcN" pre-release tag: rcN becomes the package
# iteration, so 2.3.0-rc2 packages as version 2.3.0 iteration 2.
PKG_RELEASE_SH = $(PKG_DIR)/../.github/bin/pkg_release.sh
PKG_VERSION := $(shell $(PKG_RELEASE_SH) '$(VERSION)' version)
ITERATION := $(or $(ITERATION),$(shell $(PKG_RELEASE_SH) '$(VERSION)' iteration))
# For names with no iteration field of their own (macOS .pkg).
RELEASE := $(PKG_VERSION)-$(ITERATION)
# rpm Version forbids '-'.
RPM_VERSION := $(subst -,_,$(PKG_VERSION))
# The deb revision embeds the distro per Debian convention
# (e.g. 2.3.0-1debian12); the rpm Release gets it as the dist tag (1.el9).
DEB_ITERATION := $(ITERATION)$(BUILD_DISTRO)
DEB_FILE = $(TARGET_DIR)/$(NAME)_$(PKG_VERSION)-$(DEB_ITERATION)_$(DEB_ARCH).deb
RPM_FILE = $(TARGET_DIR)/$(NAME)-$(RPM_VERSION)-$(ITERATION).$(BUILD_DISTRO).$(RPM_ARCH).rpm
RPM_TOP = $(BUILD_DIR)/rpm
# pkgbuild --version rejects hyphens and underscores; collapse both to dots
# (2.3.0-1 -> 2.3.0.1). This is the version *inside* the pkg only; the file
# name below keeps the canonical hyphenated RELEASE.
MAC_VERSION := $(subst _,.,$(subst -,.,$(RELEASE)))
MAC_ROOT = $(BUILD_DIR)/mac-root
PKG_ID = com.aerospike.asvalidation
# macOS artifact file name, lower case and '-' separated so a Homebrew cask can
# interpolate it (<arch> is `uname -m`, the token a cask's `arch` stanza emits):
#
#     aerospike-asvalidation-<version>-<iteration>-macos-<arch>.pkg
#
# One pkg per architecture, not per macOS generation: macOS is forward
# compatible, so the oldest-runner build serves every supported release.
MAC_PKG = $(TARGET_DIR)/$(NAME)-$(RELEASE)-macos-$(ARCH).pkg
# Support floor, asserted by check_min_os.sh and passed to the compiles as
# MACOSX_DEPLOYMENT_TARGET. Unpinned, clang would stamp the runner's own SDK.
MACOS_MIN_VERSION = 14.0


.PHONY: deb
deb: prep
	install -d $(STAGE_DIR)/DEBIAN
	sed -e 's/@VERSION@/$(PKG_VERSION)-$(DEB_ITERATION)/' -e 's/@ARCH@/$(DEB_ARCH)/' \
		$(PKG_DIR)/deb/control > $(STAGE_DIR)/DEBIAN/control
	dpkg-deb --root-owner-group -Z xz --build $(STAGE_DIR) $(DEB_FILE)

.PHONY: rpm
rpm: prep
	rm -rf $(RPM_TOP)
	install -d $(RPM_TOP)
	sed -e 's/@VERSION@/$(RPM_VERSION)/' -e 's/@RELEASE@/$(ITERATION)/' \
		$(PKG_DIR)/rpm/asvalidation.spec > $(RPM_TOP)/asvalidation.spec
	rpmbuild -bb --target $(RPM_ARCH) \
		--define '_topdir $(RPM_TOP)' \
		--define '_stagedir $(STAGE_DIR)' \
		--define 'dist .$(BUILD_DISTRO)' \
		$(RPM_TOP)/asvalidation.spec
	mv $(RPM_TOP)/RPMS/$(RPM_ARCH)/$(NAME)-$(RPM_VERSION)-$(ITERATION).$(BUILD_DISTRO).$(RPM_ARCH).rpm $(RPM_FILE)

# PKG_VERSION, ITERATION and BUILD_DISTRO all come from $(shell ...), which
# yields the empty string on failure without failing the build; unguarded, an
# empty one ships aerospike-asvalidation_-<distro>_<arch>.deb past every later check.
.PHONY: check-version
check-version:
	@test -n '$(VERSION)' \
		|| { echo "ERROR: VERSION is empty -- build/version failed and none was passed; refusing to build an unversioned package" >&2; exit 1; }
	@test -n '$(PKG_VERSION)' \
		|| { echo "ERROR: empty package version -- $(PKG_RELEASE_SH) failed for VERSION='$(VERSION)'" >&2; exit 1; }
	@test -n '$(ITERATION)' \
		|| { echo "ERROR: empty package iteration -- $(PKG_RELEASE_SH) failed for VERSION='$(VERSION)'" >&2; exit 1; }
	@test -n '$(BUILD_DISTRO)' \
		|| { echo "ERROR: empty BUILD_DISTRO -- os_version.sh failed and none was passed" >&2; exit 1; }

.PHONY: prep
prep: check-version
	install -d $(TARGET_DIR)
	rm -rf $(STAGE_DIR)
	install -d $(STAGE_DIR)/opt/aerospike/bin
	install -pm 755 $(BIN) $(STAGE_DIR)/opt/aerospike/bin
	install -d $(STAGE_DIR)/usr/bin
	ln -sf /opt/aerospike/bin/asvalidation $(STAGE_DIR)/usr/bin/asvalidation

# Staging for macOS installer: install to /usr/local/aerospike/bin with symlink at /usr/local/bin.
.PHONY: prep-mac
prep-mac: check-version
	@echo '$(ARCH)' | grep -qE '^(arm64|x86_64)$$' \
		|| { echo "ERROR: architecture token is '$(ARCH)', not arm64 or x86_64 -- uname -m failed; refusing to build a misnamed pkg" >&2; exit 1; }
	install -d $(TARGET_DIR)
	rm -rf $(MAC_ROOT)
	install -d $(MAC_ROOT)/usr/local/aerospike/bin
	install -d $(MAC_ROOT)/usr/local/bin
	install -pm 755 $(BIN) $(MAC_ROOT)/usr/local/aerospike/bin/asvalidation
	ln -sf ../aerospike/bin/asvalidation $(MAC_ROOT)/usr/local/bin/asvalidation

.PHONY: osx-pkg
osx-pkg: prep-mac
	$(PKG_DIR)/../.github/bin/check_min_os.sh $(MACOS_MIN_VERSION) $(MAC_ROOT)
	pkgbuild \
		--root $(MAC_ROOT) \
		--identifier $(PKG_ID) \
		--version $(MAC_VERSION) \
		--install-location / \
		--scripts $(PKG_DIR)/mac-scripts \
		$(MAC_PKG)

.PHONY: clean
clean:
	rm -rf $(TARGET_DIR)
	rm -rf $(BUILD_DIR)

print-% : ; @echo $($*)
