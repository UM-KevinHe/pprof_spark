package pprof.spark.testkit

import org.apache.spark.sql.SparkSession

/** Base class for suites that run against Spark: the local Classic session (layer T3) or, with
  * `-Dpprof.test.sparkApi=connect`, a Spark Connect session (layer T8). See [[TestSessions]].
  */
abstract class SparkSuite extends munit.FunSuite {

  /** The shared session. A stable identifier, so suites can `import spark.implicits._`. */
  protected final lazy val spark: SparkSession = TestSessions.current

  /** Registers `name` twice: with ANSI SQL mode on and with it off (§5.5). */
  protected def testAnsiOnAndOff(name: String)(body: => Any)(implicit loc: munit.Location): Unit =
    Seq(true, false).foreach { ansi =>
      test(s"$name [ansi=$ansi]") {
        withSqlConf("spark.sql.ansi.enabled" -> ansi.toString)(body)
      }
    }

  /** Runs `body` with session configuration overrides, then restores the previous values. */
  protected def withSqlConf[A](overrides: (String, String)*)(body: => A): A = {
    val conf = spark.conf
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
