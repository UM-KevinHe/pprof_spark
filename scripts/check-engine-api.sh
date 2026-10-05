#!/usr/bin/env bash
# Engine API rules: PROJECT_CONTEXT ARCH-2, PLAT-2 and PLAT-7, plus §10.3 (explicit storage levels).
#
# Compiling `engine` against spark-sql-api is necessary but not sufficient. spark-sql-api 4.1.0
# depends on spark-connect-shims, which defines placeholder SparkContext, RDD and JavaRDD
# classes, and the shared SparkSession and Dataset interfaces declare sparkContext(), rdd(),
# toJavaRDD(), checkpoint() and localCheckpoint(). Code using them compiles, then fails under
# Spark Connect. This script scans engine main sources for those names and imports, and every
# module's sources for package placement inside org.apache.spark.
#
# Exempt a line only with a trailing `// api-check: allow <reason>` comment that reviewers see.
#
# Usage: scripts/check-engine-api.sh [repo-root]
#        scripts/check-engine-api.sh --self-test    (negative control: every rule must fire)
set -euo pipefail

self="${BASH_SOURCE[0]}"

if [[ "${1:-}" == "--self-test" ]]; then
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT
  src="$tmp/engine/src/main/scala"
  mkdir -p "$src"
  samples=(
    'import org.apache.spark.ml.Pipeline'
    'import org.apache.spark.mllib.linalg.Vectors'
    'import org.apache.spark.rdd.RDD'
    'import org.apache.spark.api.java.JavaRDD'
    'import org.apache.spark.sql.classic.Dataset'
    'val s = org.apache.spark.sql.connect.SparkSession.builder()'
    'def f(sc: SparkContext) = sc'
    'val sc = spark.sparkContext'
    'val r = ds.rdd'
    'val j = ds.toJavaRDD'
    'val c = ds.localCheckpoint()'
    'val k = ds.checkpoint()'
    'val d = ds.cache()'
  )
  for sample in "${samples[@]}"; do
    printf 'object Sample {\n  %s\n}\n' "$sample" >"$src/Sample.scala"
    if bash "$self" "$tmp" >/dev/null 2>&1; then
      echo "self-test FAILED: rule did not fire for: $sample" >&2
      exit 1
    fi
  done
  printf 'package org.apache.spark.sql\nobject Placed\n' >"$src/Sample.scala"
  if bash "$self" "$tmp" >/dev/null 2>&1; then
    echo "self-test FAILED: package placement inside org.apache.spark not detected" >&2
    exit 1
  fi
  printf 'object Sample {\n  val p = ds.persist(level)\n  val s = spark.sparkContext // api-check: allow sample\n}\n' >"$src/Sample.scala"
  if ! bash "$self" "$tmp" >/dev/null 2>&1; then
    echo "self-test FAILED: compliant source was rejected" >&2
    exit 1
  fi
  echo "check-engine-api self-test passed (${#samples[@]} violations detected, compliant source accepted)"
  exit 0
fi

root="${1:-$(cd "$(dirname "$self")/.." && pwd)}"
status=0

# Arguments: directory, extended regex, explanation.
scan() {
  local hits
  hits="$(grep -rnE --include='*.scala' --include='*.java' --exclude-dir=target --exclude-dir=.git \
    "$2" "$1" 2>/dev/null | grep -v 'api-check: allow' || true)"
  if [[ -n "$hits" ]]; then
    printf 'API rule violated: %s\n%s\n\n' "$3" "$hits" >&2
    status=1
  fi
}

engine_main="$root/engine/src/main"
if [[ -d "$engine_main" ]]; then
  scan "$engine_main" 'org\.apache\.spark\.(ml|mllib)\b' 'Spark ML belongs in the ml module (ARCH-2, NN-5)'
  scan "$engine_main" 'org\.apache\.spark\.rdd\b' 'RDDs are unavailable under Spark Connect (ARCH-2)'
  scan "$engine_main" 'org\.apache\.spark\.api\.java\b' 'the Java RDD API is unavailable under Spark Connect'
  scan "$engine_main" 'org\.apache\.spark\.sql\.(classic|connect)\.[A-Za-z_{]' 'engine uses only the shared Classic/Connect interface (§5.4)'
  scan "$engine_main" '\bSparkContext\b|\bsparkContext\b' 'SparkContext is unavailable under Spark Connect (ARCH-2)'
  scan "$engine_main" '\.rdd\b|\btoJavaRDD\b|\bjavaRDD\b' 'RDD conversions are unavailable under Spark Connect'
  scan "$engine_main" '\b(localCheckpoint|checkpoint)\(' 'checkpoints are unavailable on serverless; use table materialization (§6.7)'
  scan "$engine_main" '\.cache\(\)' 'set storage levels explicitly with persist (§10.3)'
fi
scan "$root" '^[[:space:]]*package[[:space:]]+org\.apache\.spark\b' 'no sources inside org.apache.spark (PLAT-7)'

if [[ "$status" -eq 0 ]]; then
  echo "check-engine-api: OK"
fi
exit "$status"
