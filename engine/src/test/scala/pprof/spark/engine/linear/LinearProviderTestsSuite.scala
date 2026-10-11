package pprof.spark.engine.linear

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.DataFrame
import pprof.spark.engine.logistic.EffectReference
import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** Linear provider tests and effect intervals against base R and pprof_py on the slice 3b fixtures
  * (docs/spec/linear/provider-inference.md §9), with X-035's extreme provider tested.
  */
class LinearProviderTestsSuite extends SparkSuite {

  private val cases =
    Seq("lin-base", "lin-singletons", "lin-shifted", "lin-text", "lin-many", "lin-extreme")
  private val spec = LinearSpec("y", Seq("x1", "x2", "x3"), "provider", Some("id"))
  private val variants: Map[String, (EffectReference, String, Option[Double])] = Map(
    "two_sided" -> ((EffectReference.Median, "two_sided", None)),
    "greater" -> ((EffectReference.Median, "greater", None)),
    "less" -> ((EffectReference.Median, "less", None)),
    "mean" -> ((EffectReference.Mean, "two_sided", None)),
    "number" -> ((EffectReference.Value(0.25), "two_sided", None)),
    "critical" -> ((EffectReference.Median, "two_sided", Some(2.5)))
  )

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"linear/$name/input.csv").toString)

  private def d(node: JsonNode): Array[Double] = Fixtures.doubles(node)

  /** Finite positions under the class; other positions equal (the same infinity or both NaN). */
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
    actual.indices.filterNot(keep.contains).foreach { i =>
      assert(
        actual(i) == expected(i) || (actual(i).isNaN && expected(i).isNaN),
        s"$what[$i]: ${actual(i)} against ${expected(i)}"
      )
    }
    if (keep.nonEmpty) {
      val ratio = Tolerances(cls).worstRatio(keep.map(actual).toArray, keep.map(expected).toArray)
      assert(ratio <= 1.0, s"$what: worst ratio $ratio under $cls")
    }
  }

  /** §8.4's T-p. */
  private def checkP(what: String, actual: Array[Double], expected: Array[Double]): Unit = {
    val threshold = Tolerances.number("T-p.log10.threshold")
    val atol = Tolerances.number("T-p.log10.atol")
    actual.zip(expected).zipWithIndex.foreach { case ((a, e), i) =>
      if (e >= threshold) assert(Tolerances("T-test").accepts(a, e), s"$what[$i]: $a against $e")
      else if (e == 0.0) assert(a < 1e-300, s"$what[$i]: $a against 0")
      else
        assert(
          math.abs(math.log10(a) - math.log10(e)) <= atol,
          s"$what[$i]: log10 of $a against $e"
        )
    }
  }

  /** Flags equal, except within T-flag's boundary band around the critical value. */
  private def checkFlags(
      what: String,
      actual: Array[Double],
      expected: Array[Double],
      z: Array[Double],
      c: Double
  ): Unit = {
    val band = Tolerances.number("T-flag.boundary.rtol") * c
    actual.indices.foreach { i =>
      assert(
        actual(i) == expected(i) || math.abs(math.abs(z(i)) - c) <= band,
        s"$what[$i]: ${actual(i)} against ${expected(i)}"
      )
    }
  }

  private def columns(table: DataFrame, names: String*): Seq[Array[Double]] = {
    val rows = table.collect()
    names.map { name =>
      val i = table.schema.fieldIndex(name)
      rows.map(row => if (row.isNullAt(i)) Double.NaN else row.get(i).toString.toDouble)
    }
  }

  for (name <- cases) {
    test(s"$name: provider tests and effect intervals match base R and pprof_py") {
      val fit = LinearFE.fit(input(name), spec)
      val py = Fixtures.json(s"linear/$name/inference_pprof_py.json")
      val r = Fixtures.json(s"linear/$name/inference_r.json")
      check(
        "T-coef",
        "median",
        Array(LinearProviderTests.reference(fit, EffectReference.Median)),
        d(r.get("ref_median"))
      )
      check(
        "T-coef",
        "mean",
        Array(LinearProviderTests.reference(fit, EffectReference.Mean)),
        d(r.get("ref_mean"))
      )
      val tests = py.get("tests").fieldNames()
      while (tests.hasNext) {
        val v = tests.next()
        val (reference, alternative, critical) = variants(v)
        val table = LinearProviderTests.test(fit, reference, alternative, 0.95, critical)
        val got1 = columns(table, "z_raw", "p_value", "flag", "ci_lower", "ci_upper")
        val (z, p, flag, lower, upper) = (got1(0), got1(1), got1(2), got1(3), got1(4))
        check("T-test", s"$v z vs base R", z, d(r.get(s"test_${v}_z")))
        checkP(s"$v p vs base R", p, d(r.get(s"test_${v}_p")))
        val c = critical.getOrElse(
          if (alternative == "two_sided") 1.959963984540054 else 1.6448536269514722
        )
        checkFlags(s"$v flags vs base R", flag, d(r.get(s"test_${v}_flag")), z, c)
        check("T-coef", s"$v lower vs base R", lower, d(r.get(s"test_${v}_lower")))
        check("T-coef", s"$v upper vs base R", upper, d(r.get(s"test_${v}_upper")))
        val pyZ = d(py.get("tests").get(v).get("z_raw"))
        val defined =
          pyZ.indices.filterNot(i => pyZ(i).isNaN) // X-035: pprof_py leaves such providers untested
        check("T-test", s"$v z vs pprof_py", defined.map(z).toArray, defined.map(pyZ).toArray)
        assertEquals(
          pyZ.length - defined.size,
          if (name == "lin-extreme") 1 else 0,
          s"$v: providers untested by pprof_py"
        )
        assert(
          z.forall(v => java.lang.Double.isFinite(v)) && flag.forall(f => !f.isNaN),
          s"$v: every provider tested"
        )
      }
      if (py.has("subset")) {
        val keys = (0 until 5).map(i => py.get("providers").get(i).asText())
        val subset = LinearProviderTests.test(fit, providers = Some(keys))
        val full = LinearProviderTests.test(fit)
        assertEquals(subset.collect().toSeq, full.collect().take(5).toSeq)
      }
      val got2 = columns(LinearProviderTests.gammaIntervals(fit), "lower", "upper")
      val (lower, upper) = (got2(0), got2(1))
      check("T-coef", "gamma interval lower", lower, d(r.get("gamma_lower")))
      check("T-coef", "gamma interval upper", upper, d(r.get("gamma_upper")))
    }
  }
}
