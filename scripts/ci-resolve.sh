#!/usr/bin/env bash
# CI's dependency step (ci.yml, job `dependencies`). Fetches every artifact the other jobs need into sbt's caches,
# which that job then saves for them to restore:
# - the library dependencies of every module and configuration, and the Scala tools (`update`);
# - the compiler bridge (`numerics/compile`, the smallest module);
# - the linkage check's resolution, with Scala 2.13.16 forced (PLAT-3);
# - scalafmt itself, which sbt-scalafmt downloads on first use (`scalafmtSbtCheck`).
# Maven Central rate-limits GitHub's runners (HTTP 429, at times reported as "not found"), so each command is
# retried after a growing pause.
#
# Usage: scripts/ci-resolve.sh [pause in seconds before the first retry, default 60]
set -uo pipefail

pause=${1:-60}
attempts=4

run() {
  local n
  for ((n = 1; n <= attempts; n++)); do
    if sbt -v "$@"; then
      return 0
    fi
    if ((n < attempts)); then
      echo "::warning::sbt $* failed (attempt $n of $attempts); retrying in $((n * pause)) s"
      sleep $((n * pause))
    fi
  done
  echo "::error::sbt $* failed $attempts times"
  return 1
}

run update numerics/compile &&
  run -Dpprof.linkageCheck=true update &&
  run scalafmtSbtCheck
