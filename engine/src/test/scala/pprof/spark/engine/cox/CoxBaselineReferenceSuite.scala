package pprof.spark.engine.cox

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, signum, when}

import pprof.spark.engine.cox.CoxFixtures._
import pprof.spark.testkit.SparkSuite

/** The baseline's reference points (Phase 1b specification §2a; production finding C1, D-49):
  * `raw` (x = 0, offset 0), `mean_offset` (pprof_py's `baseline_hazard_`, R's
  * `basehaz(centered = FALSE)`) and `means` (R's `survfit` default and `basehaz(centered = TRUE)`).
  */
class CoxBaselineReferenceSuite extends SparkSuite {

  private def rows(table: DataFrame): Array[Row] = table.orderBy("stratum", "time").collect()

  private def column(rows: Array[Row], name: String): Seq[Double] =
    rows.toSeq.map(_.getAs[Double](name))

  /** The fixtures with offsets and weights: one right-censored, one with entry times. */
  private val withOffsets = Seq("rc-stratified-weights-offset", "lt-weights-offset")

  /** R's value at each of our rows, looked up by stratum and time. */
  private def atRows(
      own: Array[Row],
      times: Array[Double],
      strata: Array[Double],
      values: Array[Double]
  ) = {
    val lookup = strata.zip(times).zip(values).toMap
    own.map(row => lookup((row.getAs[Long]("stratum").toDouble, row.getAs[Double]("time"))))
  }

  test("raw is the default and unchanged; an unknown reference is refused") {
    val name = "lt-weights-offset"
    val fit = CoxPH.fit(frame(spark, name), spec(name), tight(Ties.Efron))
    def values(table: DataFrame): Seq[Double] =
      rows(table).toSeq.flatMap(r =>
        Seq(
          r.getAs[Double]("hazard_increment"),
          r.getAs[Double]("cumulative_hazard"),
          r.getAs[Double]("survival")
        )
      )
    assertEquals(
      bits(values(CoxPH.baseline(frame(spark, name), fit, BaselineReference.Raw))),
      bits(values(CoxPH.baseline(frame(spark, name), fit)))
    )
    val e = intercept[IllegalArgumentException](BaselineReference.fromName("centered"))
    assert(e.getMessage.contains("mean_offset"), e.getMessage)
  }

  for (name <- withOffsets; ties <- Methods)
    test(
      s"mean_offset is pprof_py's public baseline and R's basehaz(centered = FALSE) (T-base): $name, ${ties.name}"
    ) {
      val fit = CoxPH.fit(frame(spark, name), spec(name), tight(ties))
      val pyBeta = doubles(reference(name, "pprof_py", ties).get("tight"), "coef")
      val atPy =
        fit.copy(coefficients = fit.coefficients.zip(pyBeta).map { case (c, b) =>
          c.copy(estimate = b)
        })
      val py = reference(name, "pprof_py", ties).get("baseline")
      val ours = rows(CoxPH.baseline(frame(spark, name), atPy, BaselineReference.MeanOffset))
      check(
        "T-base",
        s"$name ${ties.name} vs pprof_py's public baseline",
        column(ours, "cumulative_hazard"),
        doubles(py, "public_cumulative_hazard")
      )
      val own = rows(CoxPH.baseline(frame(spark, name), fit, BaselineReference.MeanOffset))
      val r = reference(name, "r_survival", ties).get("tight")
      val times = doubles(r, "basehaz_time")
      val strata =
        if (r.has("basehaz_stratum")) doubles(r, "basehaz_stratum")
        else Array.fill(times.length)(0.0)
      check(
        "T-base",
        s"$name ${ties.name} vs R's basehaz(centered = FALSE)",
        column(own, "cumulative_hazard"),
        atRows(own, times, strata, doubles(r, "basehaz_hazard"))
      )
      own.foreach { row =>
        assertEquals(
          row.getAs[Double]("survival"),
          StrictMath.exp(-row.getAs[Double]("cumulative_hazard"))
        )
      }
    }

  for (name <- withOffsets)
    test(s"means follows R's rules for which covariates are centred and how: $name") {
      // A 0/1 and a -1/0/1 covariate, which R does not centre (nocenter).
      val df = frame(spark, name)
        .withColumn("binary", when(col("x1") > 0.0, 1.0).otherwise(0.0))
        .withColumn("ternary", signum(col("x2")))
      val s = spec(name).copy(features = spec(name).features ++ Seq("binary", "ternary"))
      val fit = CoxPH.fit(df, s, tight(Ties.Breslow))
      val point = CoxPH.referencePoint(df, fit)
      val data = df.select((Seq("weight", "offset") ++ s.features).map(col): _*).collect()
      val w = data.map(_.getDouble(0))
      val total = w.sum
      val offsetMean = w.zip(data.map(_.getDouble(1))).map { case (a, b) => a * b }.sum / total
      assert(math.abs(point.offsetMean - offsetMean) <= 1e-14 * (1.0 + math.abs(offsetMean)))
      s.features.indices.foreach { k =>
        val x = data.map(_.getDouble(2 + k))
        val zeroOne = x.forall(v => v == -1.0 || v == 0.0 || v == 1.0)
        assertEquals(point.centered(k), !zeroOne, s.features(k))
        val expected =
          if (zeroOne) 0.0
          else if (s.entry.isDefined) x.sum / x.length // agreg.fit: unweighted
          else w.zip(x).map { case (a, b) => a * b }.sum / total // coxph.fit: weighted
        assert(
          math.abs(point.means(k) - expected) <= 1e-13 * (1.0 + math.abs(expected)),
          s"${s.features(k)}: ${point.means(k)} against $expected"
        )
      }
      assert(!point.centered(s.features.indexOf("binary")))
      assert(!point.centered(s.features.indexOf("ternary")))
      val factor = point.factor(BaselineReference.Means, fit.estimates)
      val raw = rows(CoxPH.baseline(df, fit))
      val means = rows(CoxPH.baseline(df, fit, BaselineReference.Means))
      raw.zip(means).foreach { case (a, b) =>
        val expected = a.getAs[Double]("cumulative_hazard") * factor
        assert(math.abs(b.getAs[Double]("cumulative_hazard") - expected) <= 1e-15 * expected)
      }
    }

  for (name <- withOffsets; ties <- Methods)
    test(s"means is R's basehaz(centered = TRUE), with R's fit$$means: $name, ${ties.name}") {
      val r = reference(name, "r_survival", ties).get("tight")
      assume(
        r.has("basehaz_centered_hazard"),
        "the fixtures predate round 84: regenerate them with the fixtures workflow"
      )
      val fit = CoxPH.fit(frame(spark, name), spec(name), tight(ties))
      val point = CoxPH.referencePoint(frame(spark, name), fit)
      check("T-coef", s"$name ${ties.name} means vs R", point.means, doubles(r, "means"))
      val own = rows(CoxPH.baseline(frame(spark, name), fit, BaselineReference.Means))
      val times = doubles(r, "basehaz_time")
      val strata =
        if (r.has("basehaz_stratum")) doubles(r, "basehaz_stratum")
        else Array.fill(times.length)(0.0)
      check(
        "T-base",
        s"$name ${ties.name} vs R's basehaz(centered = TRUE)",
        column(own, "cumulative_hazard"),
        atRows(own, times, strata, doubles(r, "basehaz_centered_hazard"))
      )
    }
}
