package pprof.spark.testkit

import java.nio.file.Files

import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** The shared local Classic Spark session of test layer T3.
  *
  * sbt forks one test JVM per module and runs its suites serially, so suites share this session
  * and must restore any configuration they change (see [[withSqlConf]]). This object is test
  * infrastructure only; production code never keeps a session in global state (§13).
  */
object LocalSpark {

  /** Local master; four task threads exercise concurrency without needing four cores. */
  val Master: String = "local[4]"

  /** Small shuffle-partition count, set explicitly (§11.3). */
  val ShufflePartitions: Int = 4

  /** Session time zone; suites that depend on time zones set their own explicitly (§5.5). */
  val SessionTimeZone: String = "UTC"

  lazy val session: SparkSession = {
    requireSparkCoreClasses()
    val warehouse = Files.createTempDirectory("pprof-spark-warehouse")
    org.apache.spark.sql.classic.SparkSession
      .builder()
      .master(Master)
      .appName("pprof-spark-tests")
      .config("spark.ui.enabled", "false")
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", ShufflePartitions.toString)
      .config("spark.default.parallelism", ShufflePartitions.toString)
      .config("spark.sql.ansi.enabled", "true")
      .config("spark.sql.session.timeZone", SessionTimeZone)
      .config("spark.sql.warehouse.dir", warehouse.toUri.toString)
      .getOrCreate()
  }

  /** Fails fast when spark-connect-shims shadows spark-core on this Classic classpath.
    *
    * The shims define placeholder classes, such as a `SparkConf` without methods. If they come
    * first on the classpath, session creation fails later with an obscure `NoSuchMethodError`.
    * The check is behavioral, so it does not depend on jar names. build.sbt removes the shims
    * from Test classpaths (OI-21).
    */
  def requireSparkCoreClasses(): Unit = {
    val conf = classOf[SparkConf]
    val location = Option(conf.getProtectionDomain.getCodeSource)
      .map(_.getLocation.toString)
      .getOrElse("an unknown location")
    val isPlaceholder = Try(conf.getMethod("set", classOf[String], classOf[String])).isFailure
    require(
      !isPlaceholder,
      s"org.apache.spark.SparkConf from $location lacks set(String, String): spark-connect-shims " +
        "shadows spark-core on this Classic classpath (see sparkModuleSettings in build.sbt)"
    )
  }

}
