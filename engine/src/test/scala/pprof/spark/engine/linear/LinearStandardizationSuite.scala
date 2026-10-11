package pprof.spark.engine.linear

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import pprof.spark.engine.logistic.EffectReference
import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** Standardized differences and their intervals against base R, pprof_py and R pprof on the slice 3b fixtures
  * (docs/spec/linear/provider-inference.md §9), one test per case. Split in two suites so each run stays within its
  * time under Spark Connect; the training data are required (API-3).
  */
abstract class LinearStandardizationChecks(cases: Seq[String]) extends SparkSuite {

  protected val spec = LinearSpec("y", Seq("x1", "x2", "x3"), "provider", Some("id"))

  protected def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"linear/$name/input.csv").toString)

  private def d(node: JsonNode): Array[Double] = Fixtures.doubles(node)

  /** Finite positions under the class; other positions equal (the same infinity). */
  private def check(
      cls: String,
      what: String,
      actual: Array[Double],
      expected: Array[Double]
  ): Unit = {
    assertEquals(actual.length, expected.length, what)
    val keep = actual.indices.filter(i =>
      java.lang.Double.isFinite(actual(i)) && java.lang.Double.isFinite(expected(i))
    )
    actual.indices
      .filterNot(keep.contains)
      .foreach(i =>
        assert(actual(i) == expected(i), s"$what[$i]: ${actual(i)} against ${expected(i)}")
      )
    if (keep.nonEmpty) {
      val ratio = Tolerances(cls).worstRatio(keep.map(actual).toArray, keep.map(expected).toArray)
      assert(ratio <= 1.0, s"$what: worst ratio $ratio under $cls")
    }
  }

  private def column(table: DataFrame, name: String): Array[Double] =
    table.select(name).collect().map(_.getDouble(0))

  for (name <- cases) {
    test(s"$name: standardized differences and SM intervals match base R, pprof_py and R pprof") {
      val df = input(name)
      val fit = LinearFE.fit(df, spec)
      val py = Fixtures.json(s"linear/$name/inference_pprof_py.json")
      val r = Fixtures.json(s"linear/$name/inference_r.json")
      val refs = py.get("measures").fieldNames()
      while (refs.hasNext) {
        val ref = refs.next()
        val m = LinearStandardization.measures(
          df,
          fit,
          reference = if (ref == "median") EffectReference.Median else EffectReference.Mean
        )
        val expected = py.get("measures").get(ref)
        for (name <- Seq("indirect_difference", "observed", "expected")) {
          val ours = column(m("indirect"), name)
          check(
            "T-meas",
            s"$ref indirect $name vs base R",
            ours,
            d(r.get(s"measures_${ref}_indirect_${name.replace("indirect_", "")}"))
          )
          check(
            "T-meas",
            s"$ref indirect $name vs pprof_py",
            ours,
            d(expected.get("indirect").get(name))
          )
        }
        check(
          "T-meas",
          s"$ref direct_difference vs base R",
          column(m("direct"), "direct_difference"),
          d(r.get(s"measures_${ref}_direct_difference"))
        )
        check(
          "T-meas",
          s"$ref direct expected vs base R",
          column(m("direct"), "expected"),
          d(r.get(s"measures_${ref}_direct_expected"))
        )
        check(
          "T-meas",
          s"$ref direct observed vs base R",
          column(m("direct"), "observed").take(1),
          d(r.get(s"measures_${ref}_direct_observed"))
        )
        check(
          "T-meas",
          s"$ref direct_difference vs pprof_py",
          column(m("direct"), "direct_difference"),
          d(expected.get("direct").get("direct_difference"))
        )
        if (ref == "median") {
          check(
            "T-meas",
            "R pprof indirect difference",
            column(m("indirect"), "indirect_difference"),
            d(r.get("rpprof_indirect_difference"))
          )
          check(
            "T-meas",
            "R pprof indirect observed",
            column(m("indirect"), "observed"),
            d(r.get("rpprof_indirect_observed"))
          )
          check(
            "T-meas",
            "R pprof indirect expected",
            column(m("indirect"), "expected"),
            d(r.get("rpprof_indirect_expected"))
          )
          check(
            "T-meas",
            "R pprof direct difference",
            column(m("direct"), "direct_difference"),
            d(r.get("rpprof_direct_difference"))
          )
          check(
            "T-meas",
            "R pprof direct expected",
            column(m("direct"), "expected"),
            d(r.get("rpprof_direct_expected"))
          )
        }
      }
      val keys = py.get("intervals").fieldNames()
      while (keys.hasNext) {
        val key = keys.next()
        if (key.startsWith("sm_")) {
          val alternative = key.stripPrefix("sm_")
          val ci = LinearStandardization.intervals(
            df,
            fit,
            kinds = Seq("indirect", "direct"),
            alternative = alternative
          )
          for (kind <- Seq("indirect", "direct"); side <- Seq("lower", "upper")) {
            val ours = column(ci(kind), side)
            check("T-meas", s"$key $kind $side vs base R", ours, d(r.get(s"${key}_${kind}_$side")))
            check(
              "T-meas",
              s"$key $kind $side vs pprof_py",
              ours,
              d(py.get("intervals").get(key).get(kind).get(side))
            )
          }
        }
      }
    }
  }
}

class LinearStandardizationSuite
    extends LinearStandardizationChecks(Seq("lin-base", "lin-singletons", "lin-shifted"))

class LinearStandardizationMoreSuite
    extends LinearStandardizationChecks(Seq("lin-text", "lin-many", "lin-extreme")) {

  test(
    "measures refuse data other than the fit's training data (API-3), and a subset keeps the full rows"
  ) {
    val df = input("lin-base")
    val fit = LinearFE.fit(df, spec)
    val refused = intercept[IllegalArgumentException](
      LinearStandardization.measures(df.withColumn("y", col("y") + 1.0), fit)
    )
    assert(refused.getMessage.contains("API-3"), refused.getMessage)
    val all = LinearStandardization.measures(df, fit)("indirect").collect()
    val subset =
      LinearStandardization.measures(df, fit, providers = Some(Seq(1, 2, 3)))("indirect").collect()
    assertEquals(subset.toSeq, all.take(3).toSeq)
  }
}
