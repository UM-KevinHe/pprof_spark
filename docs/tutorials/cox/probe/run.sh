#!/usr/bin/env bash
# Compiles numerics and engine with plain scalac and runs the Cox tutorial probes.
# Usage: JARS=/path/to/spark-4.1.0/jars [WORK=/tmp/pprof-cox-probe] docs/tutorials/cox/probe/run.sh
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../../.." && pwd)"
: "${JARS:?set JARS to a directory holding the Spark 4.1.0 and Scala 2.13 jars}"
work="${WORK:-/tmp/pprof-cox-probe}"
mkdir -p "$work/numerics" "$work/engine" "$work/stub" "$work/probe" "$work/out"
compiler=$(ls "$JARS"/scala-compiler-2.13.*.jar | head -1)
library=$(ls "$JARS"/scala-library-2.13.*.jar | head -1)
reflect=$(ls "$JARS"/scala-reflect-2.13.*.jar | head -1)
scalac=(java -Xss8m -Xmx3g -cp "$compiler:$library:$reflect" scala.tools.nsc.Main -encoding UTF-8 -nowarn)
cat > "$work/stub/BuildInfo.scala" <<'EOS'
package pprof.spark.engine
// Stand-in for sbt-buildinfo's generated object; the probes do not depend on these values.
object BuildInfo {
  val version: String = "probe"
  val gitSha: String = "probe"
  val gitDirty: Boolean = false
  val scalaVersion: String = "2.13"
  val sparkCompileVersion: String = "4.1.0"
}
EOS
cp=$(ls "$JARS"/*.jar | grep -v -i shims | tr '\n' ':')
"${scalac[@]}" -classpath "$library" -d "$work/numerics" $(find "$root/numerics/src/main/scala" -name '*.scala')
"${scalac[@]}" -classpath "$cp$work/numerics" -d "$work/engine" "$work/stub/BuildInfo.scala" \
  $(find "$root/engine/src/main/scala" -name '*.scala')
"${scalac[@]}" -classpath "$cp$work/numerics:$work/engine" -d "$work/probe" "$here/TutorialProbe.scala" "$here/EdgeProbe.scala"
opens=(-XX:+IgnoreUnrecognizedVMOptions
  --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
  --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
  -Djdk.reflect.useDirectMethodHandle=false -Dio.netty.tryReflectionSetAccessible=true)
run=(java -Xmx2g "${opens[@]}" -cp "$cp$work/numerics:$work/engine:$work/probe")
(cd "$root" && "${run[@]}" pprof.spark.engine.cox.TutorialProbe > "$work/out/tutorial-probe.txt" 2> "$work/out/tutorial-probe.log")
(cd "$root" && "${run[@]}" pprof.spark.engine.cox.EdgeProbe > "$work/out/edge-probe.txt")
java -version 2>&1 | head -1 >> "$work/out/tutorial-probe.txt"
echo "$work/out/tutorial-probe.txt"
echo "$work/out/edge-probe.txt"
