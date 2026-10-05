package pprof.spark.testkit

import org.apache.spark.sql.SparkSession

/** The Spark session that suites run against: the local Classic session (test layer T3) or a
  * Spark Connect client session (layer T8). The system property `pprof.test.sparkApi` selects
  * it: `classic` (the default) or `connect`; build.sbt passes it on to forked test JVMs.
  */
object TestSessions {

  val Property: String = "pprof.test.sparkApi"

  /** The selected mode, validated. */
  def mode: String = sys.props.getOrElse(Property, "classic") match {
    case selected @ ("classic" | "connect") => selected
    case other                              =>
      throw new IllegalArgumentException(s"$Property must be classic or connect, not '$other'")
  }

  def isConnect: Boolean = mode == "connect"

  lazy val current: SparkSession = if (isConnect) LocalSparkConnect.session else LocalSpark.session
}
