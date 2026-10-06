#!/usr/bin/env bash
# Codespaces and dev-container setup (spike S-06; ADR-0007). Synthetic data only (NN-7).
# Installs the sbt version that project/build.properties pins, then downloads the build's
# dependencies once so that the first `sbt test` starts quickly.
set -euo pipefail
version="$(sed -n 's/^sbt.version=//p' project/build.properties)"
if ! command -v sbt >/dev/null 2>&1; then
  curl -fsSL "https://github.com/sbt/sbt/releases/download/v${version}/sbt-${version}.tgz" | tar -xz -C "$HOME"
  echo 'export PATH="$HOME/sbt/bin:$PATH"' >>"$HOME/.bashrc"
  export PATH="$HOME/sbt/bin:$PATH"
fi
sbt -batch update
echo "Ready. 'sbt ci' runs what CI runs; 'sbt test' runs the tests."
