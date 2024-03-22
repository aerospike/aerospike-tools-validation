Name: asvalidation
Version: @VERSION@
Release: 1%{?dist}
Summary: Aerospike Validation Tool
License: Apache 2.0 license
Group: Application
BuildArch: @ARCH@
%description
This tool scans all records in a namespace and validates bins with Complex Data Type (CDT) values, optionally attempting to repair any damage detected. Records with unrecoverable CDT errors are backed up in asbackup format if an output file is specified. Records without CDTs or detected errors are ignored.
%define _topdir dist
%define __spec_install_post /usr/lib/rpm/brp-compress

%package tools
Summary: The Aerospike Validation Tool
Group: Applications
%description tools
Tools for use with the Aerospike database
%files
%defattr(-,aerospike,aerospike)
/opt/aerospike/bin/asvalidation
%defattr(-,root,root)
/usr/bin/asvalidation

%prep
ln -sf /opt/aerospike/bin/asvalidation %{buildroot}/usr/bin/asvalidation

%pre tools
echo Installing /opt/aerospike/asvalidation
if ! id -g aerospike >/dev/null 2>&1; then
        echo "Adding group aerospike"
        /usr/sbin/groupadd -r aerospike
fi
if ! id -u aerospike >/dev/null 2>&1; then
        echo "Adding user aerospike"
        /usr/sbin/useradd -r -d /opt/aerospike -c 'Aerospike server' -g aerospike aerospike
fi

%preun tools
if [ $1 -eq 0 ]
then
        echo Removing /opt/aerospike/asvalidation
fi
