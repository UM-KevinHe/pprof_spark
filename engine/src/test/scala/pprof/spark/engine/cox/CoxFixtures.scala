package pprof.spark.engine.cox

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import pprof.spark.testkit.{Fixtures, Tolerances}

/** The Cox fixtures (fixtures/cox) as test inputs and references, for suites other than
  * CoxPHSuite, which keeps its own copies of these helpers.
  */
object CoxFixtures {

  val Stratified: Seq[String] =
    Seq("rc-stratified", "rc-stratified-weights-offset", "lt-stratified", "lt-weights-offset")

  val Methods: Seq[Ties] = Seq(Ties.Breslow, Ties.Efron)

  def tight(ties: Ties): CoxOptions = CoxOptions(ties = ties, eps = 1e-11, maxIterations = 100)

  def info(name: String): JsonNode = Fixtures.json(s"cox/$name/case.json")

  def flag(name: String, key: String): Boolean = Option(info(name).get(key)).exists(_.asBoolean)

  def features(name: String): Seq[String] = {
    val node = info(name).get("features")
    (0 until node.size).map(i => node.get(i).asText())
  }

  def spec(name: String): CoxSpec =
    CoxSpec(
      "time",
      "event",
      features(name),
      if (flag(name, "stratified")) Some("stratum") else None,
      Some("id"),
      if (flag(name, "weighted")) Some("weight") else None,
      if (flag(name, "offset")) Some("offset") else None,
      if (flag(name, "truncated")) Some("entry") else None
    )

  /** The input as a DataFrame, in file order or reversed. */
  def frame(spark: SparkSession, name: String, reverse: Boolean = false): DataFrame = {
    val table = Fixtures.csv(s"cox/$name/input.csv")
    val extra = if (table.columns.contains("entry")) Seq("entry") else Seq.empty
    val names = Seq("id", "stratum", "time", "event", "weight", "offset") ++ extra ++ features(name)
    val index = names.map(table.columns.indexOf(_))
    val rows = table.rows.map { r =>
      Row.fromSeq(
        Seq[Any](r(index(0)).toLong, r(index(1)).toLong, r(index(2)), r(index(3)).toInt) ++
          index.drop(4).map(r(_))
      )
    }
    val schema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("stratum", LongType, nullable = false),
        StructField("time", DoubleType, nullable = false),
        StructField("event", IntegerType, nullable = false),
        StructField("weight", DoubleType, nullable = false),
        StructField("offset", DoubleType, nullable = false)
      ) ++ (extra ++ features(name)).map(StructField(_, DoubleType, nullable = false))
    )
    spark.createDataFrame((if (reverse) rows.reverse else rows).asJava, schema)
  }

  def reference(name: String, source: String, ties: Ties): JsonNode =
    Fixtures.json(s"cox/$name/$source.json").get(ties.name)

  def doubles(node: JsonNode, key: String): Array[Double] = Fixtures.doubles(node.get(key))

  def check(cls: String, what: String, actual: Seq[Double], expected: Array[Double]): Unit = {
    val ratio = Tolerances(cls).worstRatio(actual.toArray, expected)
    assert(ratio <= 1.0, s"$what: worst ratio $ratio under $cls")
  }

  def bits(values: Seq[Double]): Seq[Long] = values.map(java.lang.Double.doubleToLongBits)
}
