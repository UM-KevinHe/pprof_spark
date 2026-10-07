package pprof.spark.engine.cox

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** Residuals (Phase 1c specification §1, §3, §5). Each reference is compared at its own estimates
  * and covariance, because the residuals move with the estimates: across implementations the tight
  * Breslow estimates differ by up to 2.4e-9, enough to move residuals past T-res (atol 1e-9).
  */
class CoxResidualsSuite extends SparkSuite {

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
    spark.createDataFrame(rows.asJava, schema)
  }

  private def reference(name: String, source: String, ties: Ties): JsonNode =
    Fixtures.json(s"cox/$name/$source.json").get(ties.name)

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

  for (name <- cases; ties <- methods)
    test(s"residuals against pprof_py and R at their estimates (T-res): $name, ${ties.name}") {
      val fit = CoxPH.fit(frame(name), spec(name), tight(ties))
      val fs = features(name)
      for (source <- Seq("pprof_py", "r_survival")) {
        val node = reference(name, source, ties).get("tight")
        val coef = doubles(node, "coef")
        val at = fit.copy(
          coefficients = fit.coefficients.zip(coef).map { case (c, b) => c.copy(estimate = b) },
          covariance = doubles(node, "covariance").toVector,
          naiveCovariance = doubles(node, "covariance").toVector
        )
        val rows = CoxPH.residuals(frame(name), at).orderBy("id").collect()
        val what = s"$name ${ties.name} vs $source"
        def column(c: String) = rows.map(_.getAs[Double](c)).toSeq
        if (source == "pprof_py") {
          val residuals = reference(name, source, ties).get("residuals")
          check(
            "T-res",
            s"$what martingale",
            column("martingale"),
            doubles(residuals, "martingale")
          )
          for (j <- fs.indices) {
            check(
              "T-res",
              s"$what score ${fs(j)}",
              column(s"score_${fs(j)}"),
              Fixtures.doubles(residuals.get("score").get(j))
            )
            check(
              "T-res",
              s"$what dfbeta ${fs(j)}",
              column(s"dfbeta_${fs(j)}"),
              Fixtures.doubles(residuals.get("dfbeta").get(j))
            )
          }
        } else {
          check("T-res", s"$what martingale", column("martingale"), doubles(node, "martingale"))
          for (j <- fs.indices) {
            check(
              "T-res",
              s"$what score ${fs(j)}",
              column(s"score_${fs(j)}"),
              doubles(node, s"score_${j + 1}")
            )
            check(
              "T-res",
              s"$what dfbeta ${fs(j)}",
              column(s"dfbeta_${fs(j)}"),
              doubles(node, s"dfbeta_${j + 1}")
            )
          }
        }
      }
    }

  for (ties <- methods)
    test(s"weighted residuals sum to zero per stratum and to the score: ${ties.name}") {
      val name = "lt-weights-offset"
      val fit = CoxPH.fit(frame(name), spec(name), tight(ties))
      val joined = CoxPH
        .residuals(frame(name), fit)
        .join(frame(name).select("id", "stratum", "weight"), "id")
      val strata =
        joined.groupBy("stratum").agg(sum(col("martingale") * col("weight")).as("total")).collect()
      strata.foreach(r =>
        assert(math.abs(r.getAs[Double]("total")) <= 1e-9, s"stratum ${r.get(0)}: ${r.get(1)}")
      )
      val fs = features(name)
      val totals = joined
        .agg(
          sum(col(s"score_${fs.head}") * col("weight")),
          fs.tail.map(f => sum(col(s"score_$f") * col("weight"))): _*
        )
        .collect()
        .head
      val score = CoxPH
        .evaluateAt(frame(name), spec(name), fit.estimates.toArray, CoxOptions(ties = ties))
        .gradient
      fs.indices
        .foreach(j => assert(math.abs(totals.getDouble(j) - score(j)) <= 1e-8, s"score ${fs(j)}"))
    }

  test("residuals need a row identifier and the fitted data (API-3)") {
    val name = "rc-stratified"
    val fit = CoxPH.fit(frame(name), spec(name))
    intercept[IllegalArgumentException](
      CoxPH.residuals(frame(name), fit.copy(spec = spec(name).copy(rowId = None)))
    )
    val e =
      intercept[IllegalArgumentException](CoxPH.residuals(frame(name).filter(col("id") > 0L), fit))
    assert(e.getMessage.contains("fingerprint"), e.getMessage)
  }
}
