# Sourced by the test bed scripts. Any value can be overridden from the environment.

: "${ASVAL_BUILD_IMAGE:=asval-build:local}"

# 8.1.2.5 EE, used for performance runs.
: "${ASVAL_SERVER_81:=aerospike/aerospike-server-enterprise@sha256:538b7333c0d35ebc9b6773080da44388d3c41a9d060af4d3a01b8ff1d4cd0ac8}"
# 8.2.0.0 EE, first release that rejects list/map values nested deeper than 64.
: "${ASVAL_SERVER_82:=artifact.aerospike.io/database-docker-test-local/aerospike-server-enterprise@sha256:175dd9a16c2b6d0128309ad528587cfbc01313f51165f7c1c94f334a33328760}"

# asbackup/asrestore 4.5.9, used by the upgrade repro.
: "${ASVAL_BACKUP_IMAGE:=aerospike/aerospike-asbackup@sha256:f5bbce0f7a74f03348032f3a1ca5b79fa55b242c399995fc0e59949816ccd86a}"

: "${ASVAL_NET:=asval-net}"
: "${ASVAL_SRV:=asval-srv}"

# Server and client run on disjoint CPUs so they do not steal time from each other.
: "${ASVAL_SERVER_CPUS:=0-3}"
: "${ASVAL_CLIENT_CPUS:=6-9}"
