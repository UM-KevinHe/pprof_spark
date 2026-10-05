package pprof.spark.engine.metadata

import scala.util.Try

import org.apache.spark.sql.SparkSession

import pprof.spark.engine.BuildInfo

/** Which implementation of Spark's shared Classic/Connect interface a session uses (§5.4).
  *
  * Classification reads only the session's runtime class name, which is public on every tier.
  * Tests that target Classic or Connect assert it, because the session builder's selectors are
  * not reliable in every release (§9.6).
  */
sealed abstract class SparkApi(val name: String) extends Product with Serializable

object SparkApi {
  case object Classic extends SparkApi("classic")
  case object Connect extends SparkApi("connect")
  final case class Unrecognized(sessionClass: String) extends SparkApi("unrecognized")

  private val ClassicPackage = "org.apache.spark.sql.classic."
  private val ConnectPackage = "org.apache.spark.sql.connect."

  /** Classifies `session` by its implementation class. Runs on the driver; starts no job. */
  def of(session: SparkSession): SparkApi = fromClassName(session.getClass.getName)

  def fromClassName(className: String): SparkApi =
    if (className.startsWith(ClassicPackage)) Classic
    else if (className.startsWith(ConnectPackage)) Connect
    else Unrecognized(className)
}

/** The "Software" section of the reproducibility metadata recorded with every fit (NN-11, §6.10).
  *
  * It holds versions and identifiers only, never data values. Compile-time values come from the
  * generated `BuildInfo`; run-time values come from the JVM and the session.
  *
  * @param scalaCompilerVersion `scalaVersion` of the build; PLAT-3 requires it to equal the
  *   runtime's Scala patch version
  * @param scalaLibraryVersion version of the scala-library loaded at run time
  * @param databricksRuntimeVersion value of `DATABRICKS_RUNTIME_VERSION`, when set
  * @param databricksSparkVersion runtime image tag of classic Databricks compute, when readable.
  *   Databricks Runtime 18 receives dated updates under one version number, so these two fields
  *   may not identify the exact runtime build (OI-06)
  */
final case class SoftwareInfo(
    packageVersion: String,
    gitSha: String,
    gitDirty: Boolean,
    scalaCompilerVersion: String,
    scalaLibraryVersion: String,
    sparkCompileVersion: String,
    sparkRuntimeVersion: String,
    sparkApi: SparkApi,
    sessionClass: String,
    javaVersion: String,
    javaVendor: String,
    osArch: String,
    databricksRuntimeVersion: Option[String],
    databricksSparkVersion: Option[String]
) {

  /** Every field as an ordered key-value pair, for logs and run records. */
  def toFields: Seq[(String, String)] = Seq(
    "package.version" -> packageVersion,
    "package.gitSha" -> gitSha,
    "package.gitDirty" -> gitDirty.toString,
    "scala.compilerVersion" -> scalaCompilerVersion,
    "scala.libraryVersion" -> scalaLibraryVersion,
    "spark.compileVersion" -> sparkCompileVersion,
    "spark.runtimeVersion" -> sparkRuntimeVersion,
    "spark.api" -> sparkApi.name,
    "spark.sessionClass" -> sessionClass,
    "java.version" -> javaVersion,
    "java.vendor" -> javaVendor,
    "os.arch" -> osArch,
    "databricks.runtimeVersion" -> databricksRuntimeVersion.getOrElse(""),
    "databricks.sparkVersion" -> databricksSparkVersion.getOrElse("")
  )
}

object SoftwareInfo {

  /** Environment variable that Databricks Runtime sets on the driver. */
  val DatabricksRuntimeEnv: String = "DATABRICKS_RUNTIME_VERSION"

  /** Cluster tag naming the runtime image on classic Databricks compute. */
  val DatabricksSparkVersionConf: String = "spark.databricks.clusterUsageTags.sparkVersion"

  /** Captures the software metadata of this process and of `spark`.
    *
    * Runs on the driver and is Connect-compatible. Under Spark Connect, reading the version and
    * the configuration are server round trips; a configuration key that the server will not
    * reveal is recorded as absent instead of failing the fit.
    */
  def capture(spark: SparkSession): SoftwareInfo = capture(spark, sys.env.get)

  private[metadata] def capture(spark: SparkSession, env: String => Option[String]): SoftwareInfo =
    SoftwareInfo(
      packageVersion = BuildInfo.version,
      gitSha = BuildInfo.gitSha,
      gitDirty = BuildInfo.gitDirty,
      scalaCompilerVersion = BuildInfo.scalaVersion,
      scalaLibraryVersion = scala.util.Properties.versionNumberString,
      sparkCompileVersion = BuildInfo.sparkCompileVersion,
      sparkRuntimeVersion = spark.version,
      sparkApi = SparkApi.of(spark),
      sessionClass = spark.getClass.getName,
      javaVersion = System.getProperty("java.version"),
      javaVendor = System.getProperty("java.vendor"),
      osArch = System.getProperty("os.arch"),
      databricksRuntimeVersion = env(DatabricksRuntimeEnv),
      databricksSparkVersion =
        Try(spark.conf.getOption(DatabricksSparkVersionConf)).toOption.flatten
    )
}
