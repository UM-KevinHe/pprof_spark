package pprof.spark.testkit

import org.apache.spark.sql.SparkSession

/** Base class for suites that need the shared local Classic session (test layer T3). */
abstract class SparkSuite extends munit.FunSuite {

  /** The shared session. A stable identifier, so suites can `import spark.implicits._`. */
  protected final lazy val spark: SparkSession = LocalSpark.session

  /** Registers `name` twice: with ANSI SQL mode on and with it off (§5.5). */
  protected def testAnsiOnAndOff(name: String)(body: => Any)(implicit loc: munit.Location): Unit =
    Seq(true, false).foreach { ansi =>
      test(s"$name [ansi=$ansi]") {
        LocalSpark.withSqlConf("spark.sql.ansi.enabled" -> ansi.toString)(body)
      }
    }
}
