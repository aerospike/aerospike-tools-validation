# asvalidation test bed

Docker-based setup for loading CDT datasets into a local Aerospike server and
benchmarking asvalidation against them. Nothing here is part of the shipped tool.

## Layout

| Path | Purpose |
|---|---|
| `docker/Dockerfile` | Linux build image: gcc, OpenSSL, libyaml, GNU time, valgrind, perf, gdb, and C clients 7.3.0 and 7.6.2 under `/opt/client-<version>`. `CLIENTREPO` defaults to 7.6.2. |
| `docker/env.sh` | Server images (pinned by digest), network and container names, CPU sets. Every value can be overridden from the environment. |
| `docker/testbed.sh` | `image`, `up`, `down`, `run`, `srv-cpu` helpers. |
| `docker/aerospike-perf.conf` | One in-memory namespace per dataset. |
| `docker/aerospike-upgrade.conf` | Two file-backed namespaces (`up`, `up2`) for the upgrade repro. |
| `gen/gen_cdt.c` | Dataset loader (`load`), expected-class scanner (`expect`) and single-record hex dump (`dump`). |
| `gen/ref_walk.c` | Reference depth walk shared by `gen_cdt` and `probe`. |
| `bench/load.sh` | Loads every dataset at its benchmark size and prints the expected classes. |
| `bench/run.sh`, `bench/stats.py` | A/B (or A/A) benchmark runner and summary. |
| `bench/crash.sh` | Reproduces the stack overflow on deeply nested data. |
| `upgrade/run.sh` | Loads deep data on 8.1, backs it up, swaps the server to 8.2 on the same data and shows what fails. |
| `upgrade/probe.c`, `upgrade/asval.lua` | Per-key read, scan, rewrite and UDF checks, tallied by expected class. |

## Quick start

```sh
test/docker/testbed.sh image                 # once, builds asval-build:local
test/docker/testbed.sh up 81                 # 8.1 server on CPUs 0-3
test/docker/testbed.sh run make              # bin/asvalidation (client 7.6.2)
test/docker/testbed.sh run make BENCH=1      # bin/asvalidation-bench
test/bench/load.sh                           # all datasets, several minutes
test/bench/run.sh --ns d2m                   # A/A run of the bench build
test/bench/suite.sh --out /tmp/aa            # A/A over all datasets, about 2 min
test/bench/crash.sh                          # stack overflow repro on d5
```

To build against the client version pinned in `.build.yml`:

```sh
test/docker/testbed.sh run sh -c 'make clean && CLIENTREPO=/opt/client-7.3.0 make'
```

The 8.2 server image lives in the internal registry (`artifact.aerospike.io`).
Pull it once with your credentials, or point `ASVAL_SERVER_82` at another 8.2
image.

## Bench build

`make BENCH=1` compiles with `-DASVAL_BENCH` into `obj/bench/` and
`bin/asvalidation-bench`. The normal build is unchanged. At exit the bench binary
prints one line to stderr:

```
BENCH noop=0 threads=1 records=10000 wall_ns=... worker_cpu_ns=... check_ns=...
```

- `wall_ns`: from worker start to the last worker exit (monotonic clock).
- `worker_cpu_ns`: CPU time of all worker threads (`CLOCK_THREAD_CPUTIME_ID`).
  Each worker receives and parses its scan's records, so this is the client cost.
- `check_ns`: time spent inside `cdt_check()`.

With `ASVAL_BENCH_NOOP=1` the binary skips `cdt_check()` entirely. The cost of
validation is a normal run minus a no-op run.

## Datasets

| Namespace | Shape | Records |
|---|---|---|
| `d1` | Scalar bins only (int, 16-48 char string) | 5,000,000 |
| `d2m` | One unordered map bin, 50-500 entries, mixed values | 2,000 |
| `d2l` | One unordered list bin, about 1 KiB, mixed values | 300,000 |
| `d3` | 10,000-element ordered list or key-ordered map | 4,000 |
| `d4a` | D2M/D2L records, 5% of them with an extra deep bin | 4,000 |
| `d4b` | Same, 10% with an extra deep bin | 4,000 |
| `d5` | Chains nested 200,000 levels deep | 4 |

The deep bins in `d4a`/`d4b` cycle through list chains, `[1,[1,...]]` chains and
map chains at depths 63, 64, 65, 66 and 200, an empty list at 64/65/66, a
flags-only list and map at 65, and a flags-0 ext at 65.

`gen_cdt expect` reads the stored bytes back with deserialization off and
classifies each bin with a simple reference walk, because the server may store
different bytes than were sent. 8.1 rejects maps whose keys are lists or maps
("map has invalid key type"), so key nesting cannot be loaded and is covered by
unit tests instead.

## Upgrade repro

```sh
test/upgrade/run.sh | tee /tmp/upgrade-report.txt    # about 30 s
```

It recreates two Docker volumes, starts 8.1 on `aerospike-upgrade.conf`, loads
4,200 records (every other one with a deep bin) plus 4 records nested 200,000
levels deep, runs asbackup, then restarts the server container on 8.2 with the
same data volume. Each probe result is tallied by the class the reference walk
computes from the stored bytes (`ok`, `strict_only`, `over`). The server is left
running 8.2 with the upgrade config; run `testbed.sh up 81` and `bench/load.sh`
to go back to the benchmark setup.

## Reading a benchmark

`run.sh` does one discarded warmup per side, then `--runs` (default 5) measured
runs per side in ABBA order. `--repeat K` sums K back-to-back scans into one run.
The summary shows, per metric, the min and the median of each side, the B-A change
of both, and the spread ((max - min) / median) of side A.

Read the min first. On a shared machine (Docker Desktop on a laptop) interference
only adds time, so the best run is the closest to the true cost. On the reference
setup an A/A run stays within about 1% on the min of worker CPU per record, and
within about 3% on the median; a real A/B difference has to be larger than that.

asvalidation starts `min(--parallel, nodes)` worker threads, so a one-node test
bed always measures one worker.
