package pprof.spark.testkit

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

import scala.jdk.CollectionConverters._

/** One tolerance class of PROJECT_CONTEXT §8.4.
  *
  * Finite values are accepted when `|actual - expected| <= atol + rtol * |expected|`. An infinite
  * value is accepted only when both are the same infinity, and NaN is never accepted: degenerate
  * results are asserted explicitly (NN-10), never through a tolerance.
  */
final case class Tolerance(name: String, rtol: Double, atol: Double) {
  require(
    rtol >= 0.0 && atol >= 0.0 && !rtol.isInfinite && !atol.isInfinite,
    s"tolerance $name must be finite and non-negative"
  )

  def accepts(actual: Double, expected: Double): Boolean =
    if (actual.isNaN || expected.isNaN) false
    else if (actual.isInfinite || expected.isInfinite) actual == expected
    else math.abs(actual - expected) <= atol + rtol * math.abs(expected)
}

/** The single versioned tolerance file (§8.4), read from the classpath resource
  * `tolerances.conf`. Tests refer to tolerances by class name, never by literal value; values
  * change only under NN-9.
  */
object Tolerances {

  val Resource: String = "tolerances.conf"

  /** Classes defined as `<class>.rtol` and `<class>.atol`. */
  val Classes: Seq[String] = Seq(
    "T-fn",
    "T-iter",
    "T-coef",
    "T-var",
    "T-base",
    "T-res",
    "T-meas",
    "T-test",
    "T-part.objective",
    "T-part.parameters"
  )

  /** Parameters of the rule-based classes T-p and T-flag. */
  val Parameters: Seq[String] =
    Seq("T-p.log10.threshold", "T-p.log10.atol", "T-flag.boundary.rtol")

  /** Every entry of the file, each a finite, non-negative number. */
  lazy val entries: Map[String, Double] = load()

  def apply(cls: String): Tolerance =
    Tolerance(cls, rtol = number(s"$cls.rtol"), atol = number(s"$cls.atol"))

  def number(key: String): Double =
    entries.getOrElse(key, throw new NoSuchElementException(s"$key is not defined in $Resource"))

  private def load(): Map[String, Double] = {
    val stream = Option(getClass.getClassLoader.getResourceAsStream(Resource))
      .getOrElse(throw new IllegalStateException(s"$Resource is not on the classpath"))
    val properties = new Properties()
    try properties.load(new InputStreamReader(stream, StandardCharsets.UTF_8))
    finally stream.close()
    properties.asScala.toMap.map { case (key, raw) => key -> parse(key, raw) }
  }

  private def parse(key: String, raw: String): Double = {
    val value = raw.trim.toDoubleOption
      .getOrElse(throw new IllegalArgumentException(s"$key = '$raw' is not a number"))
    require(value >= 0.0 && !value.isInfinite, s"$key must be finite and non-negative")
    value
  }
}
