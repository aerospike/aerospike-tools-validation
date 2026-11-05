# Release pipeline tests

`test_execute.bats` runs in the Build and Release workflow (`Verify installation`) on every
Linux distro and macOS runner, against the signed package after it is installed, and against
the Docker image. It checks that asvalidation runs and that **the version it reports matches
the repo's `VERSION` file**, so a package labelled with one version but carrying a binary
stamped with another fails here.

Both printed lines are compared. `print_version()` splits the embedded string on `-`, so
`2.3.0-rc1` reports `Version 2.3.0` / `Build rc1` and a bare `2.3.0` reports no `Build`
line at all. Asserting the `Build` line is what catches a stale rc binary inside a later
rc's package, where the two differ only by the iteration.

```
bats .github/bin/test/test_execute.bats
```

It accepts `EXPECTED_VERSION` to verify a package built from a revision other than the
checkout you run it from. CI sets it to the workflow's `BUILD_VERSION`, which on a dev
build carries the commit sha instead of the `rcN` in the file. It still has to agree with
the VERSION file on `MAJOR.MINOR.PATCH`, so a run wired to the wrong workflow output fails
rather than testing against whatever it was handed.

```
EXPECTED_VERSION=2.3.0-abc123def bats .github/bin/test/test_execute.bats
```

`pkg_release.bats` (the rcN to iteration fold), `test_entrypoint.bats` (the Docker
image entrypoint), `check_min_os.bats` (the macOS floor check, with `otool` and `file`
stubbed on `PATH`) and `validate_inputs.bats` (the Build and Release VERSION gate, read
from the workflow with `yq`) need no package and run on every pull request in
`build-check.yml`.

```
bats .github/bin/test/pkg_release.bats .github/bin/test/test_entrypoint.bats \
    .github/bin/test/check_min_os.bats .github/bin/test/validate_inputs.bats
```

These use `[[ ]]` mid-test, which the bash 3.2 that macOS ships does not treat as a
failure, so run them with bash 4.1 or later.
