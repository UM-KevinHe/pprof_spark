package pprof.spark.engine.cox

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** Standardized measures (Phase 1d specification §1, §3, §7) against pprof_py at its estimates, as
  * for the baseline: the measures move with the estimates, and X-010 moves those slightly.
  */
class CoxMeasuresSuite extends SparkSuite {

  private val cases = Seq(
    "tiny-ties",
    "rc-unstratified",
    "rc-stratified",
    "rc-stratified-weights-offset",
    "lt-stratified",
    "lt-weights-offset"
  )
  private val methods = Seq[Ties](Ties.Breslow, Ties.Efron)

  private def tight(ties: Ties) = CoxOptions(ties = ties, eps = 1e-11, maxIterations = 100)

  private def info(name: String): JsonNode = Fixtures.json(s"cox/$name/case.json")

  private def flag(name: String, key: String): Boolean =
    Option(info(name).get(key)).exists(_.asBoolean)

  private def features(name: String): Seq[String] = {
    val node = info(name).get("features")
    (0 until node.size).map(i => node.get(i).asText())
  }

  private def spec(name: String): CoxSpec =
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

  /** The fixtures' provider: the stratum for stratified fits (two-stage), else id mod 10 (pooled). */
  private def providerColumn(name: String): String =
    if (flag(name, "stratified")) "stratum" else "provider"

  private def frame(name: String): DataFrame = {
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
        StructField("id", LongType),
        StructField("stratum", LongType),
        StructField("time", DoubleType),
        StructField("event", IntegerType),
        StructField("weight", DoubleType),
        StructField("offset", DoubleType)
      ) ++ (extra ++ features(name)).map(StructField(_, DoubleType))
    )
    spark.createDataFrame(rows.asJava, schema).withColumn("provider", col("id") % 10L)
  }

  private def reference(name: String, ties: Ties): JsonNode =
    Fixtures.json(s"cox/$name/pprof_py.json").get(ties.name)

  private def doubles(node: JsonNode, key: String): Array[Double] = Fixtures.doubles(node.get(key))

  private def check(
      cls: String,
      what: String,
      actual: Seq[Double],
      expected: Array[Double]
  ): Unit = {
    val ratio = Tolerances(cls).worstRatio(actual.toArray, expected)
    assert(ratio <= 1.0, s"$what: worst ratio $ratio under $cls")
  }

  private def atReference(fit: CoxFit, name: String, ties: Ties): CoxFit = {
    val coef = doubles(reference(name, ties).get("tight"), "coef")
    fit.copy(coefficients = fit.coefficients.zip(coef).map { case (c, b) => c.copy(estimate = b) })
  }

  for (name <- cases; ties <- methods)
    test(
      s"indirect and direct measures against pprof_py at its estimates (T-base): $name, ${ties.name}"
    ) {
      val fit = atReference(CoxPH.fit(frame(name), spec(name), tight(ties)), name, ties)
      val provider = providerColumn(name)
      val out = CoxMeasures.standardized(
        frame(name),
        fit,
        provider,
        Seq(Standardization.Indirect, Standardization.Direct)
      )
      val m = reference(name, ties).get("measures")
      val expectedProviders =
        (0 until m.get("provider").size).map(i => m.get("provider").get(i).asLong)
      val what = s"$name ${ties.name}"
      val indirect = out(Standardization.Indirect).orderBy(provider).collect()
      assertEquals(indirect.map(_.getAs[Long](provider)).toSeq, expectedProviders)
      assertEquals(
        indirect.map(_.getAs[Long]("observed").toDouble).toSeq,
        doubles(m, "observed").toSeq
      )
      check(
        "T-base",
        s"$what indirect ratio",
        indirect.map(_.getAs[Double]("indirect_ratio")).toSeq,
        doubles(m, "indirect_ratio")
      )
      check(
        "T-base",
        s"$what expected",
        indirect.map(_.getAs[Double]("expected")).toSeq,
        doubles(m, "expected")
      )
      check(
        "T-base",
        s"$what person-time",
        indirect.map(_.getAs[Double]("person_time")).toSeq,
        doubles(m, "person_time")
      )
      val direct = out(Standardization.Direct).orderBy(provider).collect()
      assertEquals(direct.map(_.getAs[Long](provider)).toSeq, expectedProviders)
      check(
        "T-base",
        s"$what direct ratio",
        direct.map(_.getAs[Double]("direct_ratio")).toSeq,
        doubles(m, "direct_ratio")
      )
      check(
        "T-base",
        s"$what direct expected",
        direct.map(_.getAs[Double]("expected")).toSeq,
        doubles(m, "direct_expected")
      )
      assert(direct.forall(_.getAs[Double]("observed") == doubles(m, "total_observed")(0)))
      assert(direct.forall(_.getAs[Long]("n_pop") == m.get("n_pop").asLong))
    }

  test("indirect expected events add up to the observed events") {
    for (name <- Seq("rc-stratified-weights-offset", "lt-weights-offset")) {
      val fit = CoxPH.fit(frame(name), spec(name))
      val totals = CoxMeasures
        .standardized(frame(name), fit, providerColumn(name))(Standardization.Indirect)
        .agg(sum("observed"), sum("expected"))
        .collect()
        .head
      check(
        "T-base",
        s"$name sum of expected",
        Seq(totals.getDouble(1)),
        Array(totals.getLong(0).toDouble)
      )
    }
  }

  test("a provider list filters the output only") {
    val name = "lt-stratified"
    val fit = CoxPH.fit(frame(name), spec(name))
    val all = CoxMeasures.standardized(frame(name), fit, "stratum")(Standardization.Indirect)
    val some = CoxMeasures.standardized(frame(name), fit, "stratum", providers = Some(Seq(2L, 5L)))(
      Standardization.Indirect
    )
    assertEquals(
      some.orderBy("stratum").collect().map(_.toSeq).toSeq,
      all.filter(col("stratum").isin(2L, 5L)).orderBy("stratum").collect().map(_.toSeq).toSeq
    )
  }

  test(
    "measures need the data the model was fitted to (API-3), with or without a provider layout"
  ) {
    val name = "rc-unstratified"
    val fit = CoxPH.fit(frame(name), spec(name))
    for (provider <- Seq("provider", "stratum")) {
      val e = intercept[IllegalArgumentException](
        CoxMeasures.standardized(frame(name).filter(col("id") > 0L), fit, provider)
      )
      assert(e.getMessage.contains("fingerprint"), e.getMessage)
    }
  }
}
