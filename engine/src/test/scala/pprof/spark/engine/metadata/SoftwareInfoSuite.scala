package pprof.spark.engine.metadata

import pprof.spark.engine.BuildInfo
import pprof.spark.testkit.{SparkSuite, TestSessions}

class SoftwareInfoSuite extends SparkSuite {

  private val expectedApi: SparkApi =
    if (TestSessions.isConnect) SparkApi.Connect else SparkApi.Classic

  test("captures build and run-time versions from the test session") {
    val info = SoftwareInfo.capture(spark, _ => None)
    assertEquals(info.sparkApi, expectedApi)
    assertEquals(info.packageVersion, BuildInfo.version)
    assertEquals(info.scalaCompilerVersion, BuildInfo.scalaVersion)
    assert(info.scalaLibraryVersion.startsWith("2.13."), info.scalaLibraryVersion)
    assert(info.gitSha == "unknown" || info.gitSha.matches("[0-9a-f]{40}"), info.gitSha)
    assertEquals(info.databricksRuntimeVersion, None)
    assertEquals(info.databricksSparkVersion, None)
  }

  test("tests run against the Spark version the engine compiles against") {
    val info = SoftwareInfo.capture(spark, _ => None)
    assertEquals(info.sparkRuntimeVersion, org.apache.spark.SPARK_VERSION)
    assertEquals(info.sparkRuntimeVersion, BuildInfo.sparkCompileVersion)
  }

  test("records the Databricks runtime version from the environment") {
    val env = Map(SoftwareInfo.DatabricksRuntimeEnv -> "18")
    assertEquals(SoftwareInfo.capture(spark, env.get).databricksRuntimeVersion, Some("18"))
  }

  test("classifies sessions by implementation class") {
    assertEquals(
      SparkApi.fromClassName("org.apache.spark.sql.classic.SparkSession"),
      SparkApi.Classic
    )
    assertEquals(
      SparkApi.fromClassName("org.apache.spark.sql.connect.SparkSession"),
      SparkApi.Connect
    )
    assertEquals(
      SparkApi.fromClassName("com.example.Session"),
      SparkApi.Unrecognized("com.example.Session")
    )
  }

  test("fromFields rebuilds what toFields wrote") {
    val info = SoftwareInfo.capture(spark, _ => None)
    assertEquals(SoftwareInfo.fromFields(info.toFields), info)
    val databricks = info.copy(
      databricksRuntimeVersion = Some("18"),
      sparkApi = SparkApi.Unrecognized("x.Session"),
      sessionClass = "x.Session"
    )
    assertEquals(SoftwareInfo.fromFields(databricks.toFields), databricks)
  }

  test("toFields lists every field once, in a stable order") {
    val info = SoftwareInfo.capture(spark, _ => None)
    val keys = info.toFields.map(_._1)
    assertEquals(keys.size, info.productArity)
    assertEquals(keys.distinct.size, keys.size)
    assertEquals(keys.head, "package.version")
  }
}
