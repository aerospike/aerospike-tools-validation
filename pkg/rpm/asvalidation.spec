Name: asvalidation
Version: @VERSION@
Release: @RELEASE@%{?dist}
Summary: Aerospike Validation Tool
License: Apache-2.0
URL: https://github.com/aerospike/aerospike-tools-validation

# Package the binary as the top-level Makefile built it, as the old spec did:
# no strip, no debuginfo package.
%global debug_package %{nil}
%global __os_install_post %{nil}

%description
asvalidation validates, and optionally repairs, Complex Data Type (CDT) bins
in an Aerospike database.

%install
cp -a %{_stagedir}/. %{buildroot}/

%files
%defattr(-,root,root,-)
/opt/aerospike/bin/asvalidation
/usr/bin/asvalidation
