package pprof.spark.engine.cox

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import pprof.spark.engine.data.{InputProblem, InvalidInputException}
import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** Baseline hazard, prediction and their persistence (Phase 1b specification §2, §3, §5, §8).
  *
  * Against pprof_py the baseline and predictions are evaluated at pprof_py's own estimates
  * (function-level parity), so that X-010's half-step difference in the estimates does not enter;
  * against R they are end to end, at pprof_spark's estimates.
  */
class CoxBaselineSuite extends SparkSuite {

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

  private def frame(name: String, reverse: Boolean = false): DataFrame = {
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
    spark.createDataFrame((if (reverse) rows.reverse else rows).asJava, schema)
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

  /** The fit with pprof_py's tight estimates in place of its own. */
  private def atReference(fit: CoxFit, name: String, ties: Ties): CoxFit = {
    val coef = doubles(reference(name, "pprof_py", ties).get("tight"), "coef")
    fit.copy(coefficients = fit.coefficients.zip(coef).map { case (c, b) => c.copy(estimate = b) })
  }

  private def rows(table: DataFrame): Array[Row] = table.orderBy("stratum", "time").collect()

  for (name <- cases; ties <- methods)
    test(
      s"baseline: pprof_py's raw and public baselines at its estimates, and R's end to end (T-base): $name, ${ties.name}"
    ) {
      {
        val fit = CoxPH.fit(frame(name), spec(name), tight(ties))
        val py = reference(name, "pprof_py", ties).get("baseline")
        val raw = py.get("raw")
        val ours = rows(CoxPH.baseline(frame(name), atReference(fit, name, ties)))
        val what = s"$name ${ties.name}"
        assertEquals(ours.map(_.getAs[Double]("time")).toSeq, doubles(raw, "time").toSeq, what)
        val strata = raw.get("stratum")
        assertEquals(
          ours.map(_.getAs[Long]("stratum")).toSeq,
          (0 until strata.size).map(strata.get(_).asLong)
        )
        val cumulative = ours.map(_.getAs[Double]("cumulative_hazard")).toSeq
        check(
          "T-base",
          s"$what cumulative hazard vs raw",
          cumulative,
          doubles(raw, "cumulative_hazard")
        )
        check(
          "T-base",
          s"$what survival vs raw",
          ours.map(_.getAs[Double]("survival")).toSeq,
          doubles(raw, "survival")
        )
        val factor = math.exp(doubles(py, "offset_mean")(0))
        check(
          "T-base",
          s"$what public",
          cumulative.map(_ * factor),
          doubles(py, "public_cumulative_hazard")
        )
        val own = rows(CoxPH.baseline(frame(name), fit))
        val r = reference(name, "r_survival", ties).get("tight")
        val rTime = doubles(r, "basehaz_time")
        val rStratum =
          if (r.has("basehaz_stratum")) doubles(r, "basehaz_stratum")
          else Array.fill(rTime.length)(0.0)
        val lookup = rStratum.zip(rTime).zip(doubles(r, "basehaz_hazard")).toMap
        val expected =
          own.map(row => lookup((row.getAs[Long]("stratum").toDouble, row.getAs[Double]("time"))))
        check(
          "T-base",
          s"$what vs R basehaz",
          own.map(_.getAs[Double]("cumulative_hazard") * factor).toSeq,
          expected
        )
      }
    }

  private def profiles(name: String, ties: Ties, stratum: Long, times: Seq[Double]): DataFrame = {
    val node = reference(name, "pprof_py", ties).get("baseline").get("profiles")
    val offsets = doubles(node, "offset")
    val rows =
      for (k <- 0 until node.get("x").size; t <- times)
        yield Row.fromSeq(
          Seq[Any](k, stratum, t, offsets(k)) ++ Fixtures.doubles(node.get("x").get(k)).toSeq
        )
    val schema = StructType(
      Seq(
        StructField("profile", IntegerType),
        StructField("stratum", LongType),
        StructField("time", DoubleType),
        StructField("offset", DoubleType)
      ) ++ features(name).map(StructField(_, DoubleType))
    )
    spark.createDataFrame(rows.asJava, schema)
  }

  for (name <- cases; ties <- methods)
    test(
      s"predictions match pprof_py on its grids, with a right-continuous step between (T-base): $name, ${ties.name}"
    ) {
      {
        val fit = atReference(CoxPH.fit(frame(name), spec(name), tight(ties)), name, ties)
        val baseline = CoxPH.baseline(frame(name), fit)
        val py = reference(name, "pprof_py", ties).get("baseline")
        val what = s"$name ${ties.name}"
        val first = py.get("predictions").get(0)
        val single = profiles(name, ties, first.get("stratum").asLong, Seq(1.0))
        val scores = CoxPrediction
          .relativeHazard(fit, CoxPrediction.linearPredictor(fit, single))
          .orderBy("profile")
          .collect()
        check(
          "T-base",
          s"$what linear predictor",
          scores.map(_.getAs[Double](CoxPrediction.LinearPredictor)).toSeq,
          doubles(py.get("profiles"), "linear")
        )
        check(
          "T-base",
          s"$what relative hazard",
          scores.map(_.getAs[Double](CoxPrediction.RelativeHazard)).toSeq,
          doubles(py.get("profiles"), "relative_hazard")
        )
        for (i <- 0 until py.get("predictions").size) {
          val block = py.get("predictions").get(i)
          val grid = doubles(block, "time").toSeq
          val between = grid.zip(grid.drop(1)).map { case (a, b) => (a + b) / 2 }
          val probe = grid ++ between ++ Seq(grid.head - 0.5, grid.last + 10.0)
          val predicted = CoxPrediction
            .survival(
              fit,
              baseline,
              profiles(name, ties, block.get("stratum").asLong, probe),
              "time"
            )
            .collect()
            .map(r =>
              (r.getAs[Int]("profile"), r.getAs[Double]("time")) -> (
                r.getAs[Double](CoxPrediction.CumulativeHazard),
                r.getAs[Double](CoxPrediction.Survival)
              )
            )
            .toMap
          for (k <- 0 until block.get("cumulative_hazard").size) {
            check(
              "T-base",
              s"$what stratum ${block.get("stratum")} profile $k hazard",
              grid.map(t => predicted((k, t))._1),
              Fixtures.doubles(block.get("cumulative_hazard").get(k))
            )
            check(
              "T-base",
              s"$what profile $k survival",
              grid.map(t => predicted((k, t))._2),
              Fixtures.doubles(block.get("survival").get(k))
            )
            grid.zip(between).foreach { case (t, m) =>
              assertEquals(predicted((k, m)), predicted((k, t)))
            }
            assertEquals(predicted((k, grid.head - 0.5))._1, 0.0)
            assertEquals(predicted((k, grid.last + 10.0)), predicted((k, grid.last)))
          }
        }
      }
    }

  test("R0: the baseline is bitwise identical across row orders and partitions") {
    val name = "lt-weights-offset"
    val fit = CoxPH.fit(frame(name), spec(name), CoxOptions(ties = Ties.Efron))
    val a = rows(CoxPH.baseline(frame(name), fit))
    val b = rows(CoxPH.baseline(frame(name, reverse = true).repartition(5), fit))
    assertEquals(b.toSeq.map(_.toSeq), a.toSeq.map(_.toSeq))
  }

  test("the baseline needs the data the model was fitted to (API-3)") {
    val name = "rc-stratified"
    val fit = CoxPH.fit(frame(name), spec(name))
    val e = intercept[IllegalArgumentException](
      CoxPH.baseline(frame(name).withColumn("x1", col("x1") + 1.0), fit)
    )
    assert(e.getMessage.contains("fingerprint"), e.getMessage)
  }

  test("predictions fail with counts for unknown strata and invalid values") {
    val name = "rc-stratified"
    val fit = CoxPH.fit(frame(name), spec(name))
    val baseline = CoxPH.baseline(frame(name), fit)
    val bad = profiles(name, Ties.Breslow, 0L, Seq(5.0)) // stratum 0 has no events (§9.5)
      .withColumn("x2", lit(Double.NaN))
    val e =
      intercept[InvalidInputException](CoxPrediction.cumulativeHazard(fit, baseline, bad, "time"))
    assertEquals(
      e.problems.toSet,
      Set[InputProblem](
        InputProblem.InvalidValues("x2", 0L, 3L),
        InputProblem.UnknownStrata("stratum", 3L)
      )
    )
  }

  test("save and load keep the baseline bit for bit (format version 2)") {
    val name = "lt-stratified"
    val fit = CoxPH.fit(frame(name), spec(name), CoxOptions(ties = Ties.Efron))
    val baseline = CoxPH.baseline(frame(name), fit)
    val path = Files.createTempDirectory("pprof-cox").resolve("model").toString
    CoxFitIO.save(spark, fit, path, Some(baseline))
    assertEquals(CoxFitIO.load(spark, path), fit)
    val loaded = CoxFitIO.loadBaseline(spark, path).getOrElse(fail("no baseline saved"))
    assertEquals(rows(loaded).toSeq.map(_.toSeq), rows(baseline).toSeq.map(_.toSeq))
    val without = Files.createTempDirectory("pprof-cox").resolve("model").toString
    CoxFitIO.save(spark, fit, without)
    assertEquals(CoxFitIO.loadBaseline(spark, without), None)
  }
}
