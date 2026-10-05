// pprof_spark build. Normative background: docs/PROJECT_CONTEXT.md §5.3, §5.4 and §11.3.

import scala.sys.process.Process
import scala.util.Try

// ---------------------------------------------------------------------------------------------
// Pinned platform (NN-13). Change only together with docs/compatibility.md.
// ---------------------------------------------------------------------------------------------

/** Scala patch shipped by Databricks Runtime 18 LTS. PLAT-3: never newer than the runtime's. */
val runtimeScalaVersion = "2.13.16"

/** Apache Spark line of Databricks Runtime 18 LTS. Open-source 4.1.x is built with 2.13.17. */
val sparkVersion = "4.1.0"

/** Newest munit built with Scala 2.13.16; later releases pull a newer scala-library (OI-04). */
val munitVersion = "1.2.0"

ThisBuild / organization := "pprof.spark" // provisional until decision D-10
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := runtimeScalaVersion
ThisBuild / scalacOptions ++= Seq(
  "-release",
  "17", // PLAT-5: one JAR for JDK 17 and JDK 21
  "-encoding",
  "UTF-8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Wunused:imports,privates,locals",
  "-Werror"
)
ThisBuild / javacOptions ++= Seq("--release", "17")

// PLAT-3 linkage guard. `sbt -Dpprof.linkageCheck=true compile` forces the runtime's Scala
// library onto every classpath, so main code that links against methods added after
// runtimeScalaVersion fails to compile. Compile only: Spark 4.1.x needs 2.13.17 to run tests.
// sbt requires scala-library, scala-reflect and scala-compiler at one version, and the Spark
// Connect client depends on scala-compiler, so all three are forced (OI-28).
val linkageCheck = sys.props.get("pprof.linkageCheck").contains("true")
ThisBuild / dependencyOverrides ++= {
  if (linkageCheck)
    Seq(
      "org.scala-lang" % "scala-library" % runtimeScalaVersion,
      "org.scala-lang" % "scala-reflect" % runtimeScalaVersion,
      "org.scala-lang" % "scala-compiler" % runtimeScalaVersion
    )
  else Nil
}

// ---------------------------------------------------------------------------------------------
// Test JVM for Spark modules
// ---------------------------------------------------------------------------------------------

/** Copied verbatim from org.apache.spark.launcher.JavaModuleOptions at Spark v4.1.0. */
val sparkJavaModuleOptions = Seq(
  "-XX:+IgnoreUnrecognizedVMOptions",
  "--add-modules=jdk.incubator.vector",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
  "-Djdk.reflect.useDirectMethodHandle=false",
  "-Dio.netty.tryReflectionSetAccessible=true",
  "-Dio.netty.allocator.type=pooled",
  "-Dio.netty.handler.ssl.defaultEndpointVerificationAlgorithm=NONE",
  "--enable-native-access=ALL-UNNAMED"
)

/** Settings for modules with Spark on the classpath. Open-source Spark 4.1.x depends on
  * scala-library 2.13.17, so these modules accept the newer library (PLAT-4); the linkage guard
  * above checks main code against 2.13.16. `numerics` deliberately does not use these settings.
  */
lazy val sparkModuleSettings = Seq(
  allowUnsafeScalaLibUpgrade := true,
  Test / fork := true,
  Test / parallelExecution := false,
  // spark-sql-api depends on spark-connect-shims, whose placeholder SparkConf, SparkContext and
  // RDD classes shadow spark-core's real ones when both are on a Classic classpath. Spark's own
  // Classic modules exclude the shims, so Test classpaths drop them too. Main code still
  // compiles against them, as the shared Classic/Connect interface requires (OI-21).
  // The Spark Connect client is an uber jar that repackages about 3,100 classes of spark-sql-api
  // and the Connect server, 1,700 of them differently; placed first, it breaks the in-process
  // Connect server of test layer T8, so it always goes last (ADR-0002).
  Test / dependencyClasspath ~= { classpath =>
    val kept = classpath.filterNot(_.data.getName.startsWith("spark-connect-shims"))
    val (client, rest) = kept.partition(_.data.getName.startsWith("spark-connect-client-jvm"))
    rest ++ client
  },
  // `sbt -Dpprof.test.sparkApi=connect ...` runs Spark suites through Spark Connect (layer T8).
  Test / javaOptions ++=
    sys.props.get("pprof.test.sparkApi").map(mode => s"-Dpprof.test.sparkApi=$mode").toSeq,
  Test / javaOptions ++= sparkJavaModuleOptions ++ Seq(
    "-Xmx2g",
    "-Duser.timezone=UTC",
    "-Dlog4j2.configurationFile=" +
      ((LocalRootProject / baseDirectory).value / "project" / "log4j2-test.properties")
  )
)

def git(args: String*): Option[String] =
  Try(Process("git" +: args).!!.trim).toOption.filter(_.nonEmpty)

// ---------------------------------------------------------------------------------------------
// Modules (§5.4, §6.1)
// ---------------------------------------------------------------------------------------------

lazy val root = (project in file("."))
  .aggregate(numerics, engine, ml, app, testkit, bench)
  .settings(name := "pprof-spark", publish / skip := true)

/** Pure Scala kernels and linear algebra; Scala standard library only (ARCH-1). */
lazy val numerics = project
  .settings(
    name := "pprof-spark-numerics",
    libraryDependencies += "org.scalameta" %% "munit" % munitVersion % Test
  )

/** Distributed engine; compiles against spark-sql-api only (ARCH-2, NN-5). */
lazy val engine = project
  .dependsOn(numerics, testkit % "test->compile")
  .enablePlugins(BuildInfoPlugin)
  .settings(sparkModuleSettings)
  .settings(
    name := "pprof-spark-engine",
    libraryDependencies += "org.apache.spark" %% "spark-sql-api" % sparkVersion % Provided,
    buildInfoPackage := "pprof.spark.engine",
    buildInfoKeys := Seq[BuildInfoKey](
      name,
      version,
      scalaVersion,
      sbtVersion,
      "sparkCompileVersion" -> sparkVersion,
      BuildInfoKey.action("gitSha")(git("rev-parse", "HEAD").getOrElse("unknown")),
      BuildInfoKey.action("gitDirty")(
        git("status", "--porcelain", "--untracked-files=no").isDefined
      )
    )
  )

/** Spark ML adapters; Classic only (ARCH-3). No sources yet. */
lazy val ml = project
  .dependsOn(engine, testkit % "test->compile")
  .settings(sparkModuleSettings)
  .settings(
    name := "pprof-spark-ml",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided
  )

/** Job-runner entry points (§6.12). No sources yet. */
lazy val app = project
  .dependsOn(engine, testkit % "test->compile")
  .settings(sparkModuleSettings)
  .settings(
    name := "pprof-spark-app",
    libraryDependencies += "org.apache.spark" %% "spark-sql-api" % sparkVersion % Provided
  )

/** Shared test harness: local Classic and Connect sessions, tolerances. Never published. */
lazy val testkit = project
  .settings(sparkModuleSettings)
  .settings(
    name := "pprof-spark-testkit",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-connect" % sparkVersion,
      "org.apache.spark" %% "spark-connect-client-jvm" % sparkVersion,
      "org.scalameta" %% "munit" % munitVersion
    )
  )

/** Benchmark workloads (§10.4). Never published. No sources yet. */
lazy val bench = project
  .dependsOn(engine, testkit)
  .settings(sparkModuleSettings)
  .settings(name := "pprof-spark-bench", publish / skip := true)

addCommandAlias("ci", "; scalafmtCheckAll; scalafmtSbtCheck; Test/compile; test")
