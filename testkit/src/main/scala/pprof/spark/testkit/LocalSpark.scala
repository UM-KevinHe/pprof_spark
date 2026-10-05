package pprof.spark.testkit

import java.nio.file.Files

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

  /** Runs `body` with SQL configuration overrides and restores the previous values afterwards. */
  def withSqlConf[A](overrides: (String, String)*)(body: => A): A = {
    val conf = session.conf
    val previous = overrides.map { case (key, _) => key -> conf.getOption(key) }
    overrides.foreach { case (key, value) => conf.set(key, value) }
    try body
    finally
      previous.foreach {
        case (key, Some(value)) => conf.set(key, value)
        case (key, None)        => conf.unset(key)
      }
  }
}
