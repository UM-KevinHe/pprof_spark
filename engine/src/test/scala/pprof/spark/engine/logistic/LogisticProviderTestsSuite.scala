package pprof.spark.engine.logistic

import org.apache.spark.sql.DataFrame
import pprof.spark.engine.logistic.LogisticFixtures._
import pprof.spark.numerics.PoissonBinomial
import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** Slice 2c against pprof_py's `test`, R pprof's `test.logis_fe` and mpmath
  * (docs/spec/logistic/provider-tests.md §7); one test per fixture case, split over two suites.
  */
abstract class LogisticProviderTestsBase(cases: Seq[String]) extends SparkSuite {

  /** z at which an upper tail is 1e-7: Φ̄⁻¹(1e-7). */
  private val UpperReliable = 5.199337582605575

  private def column(table: DataFrame, name: String): Array[Double] =
    table
      .select(name)
      .collect()
      .map(r => if (r.isNullAt(0)) Double.NaN else r.getAs[Number](0).doubleValue)

  private def flags(table: DataFrame): Seq[Int] =
    table.select("flag").collect().map(_.getInt(0)).toSeq

  /** Flags must match, except where z lies within T-flag of the critical value (reported, §8.4). */
  private def checkFlags(
      what: String,
      mine: Seq[Int],
      theirs: Seq[Int],
      z: Array[Double],
      c: Double
  ): Unit =
    mine.indices.filter(i => mine(i) != theirs(i)).foreach { i =>
      assert(
        math.abs(math.abs(z(i)) - c) <= 1e-8 * c,
        s"$what: flag of provider $i differs away from the threshold"
      )
    }

  for (name <- cases) {
    test(s"$name: exact, score, Wald and bootstrap tests match pprof_py and R") {
      val df = frame(spark, name)
      val fit = LogisticFE.fit(df, spec(name))
      val py = json(name, "pprof_py").get("provider_tests")
      val r = json(name, "r_logistic")
      val c2 = 1.959963984540054
      val c1 = 1.6448536269514722
      def run(method: String, alternative: String) =
        LogisticProviderTests.test(df, fit, method, EffectReference.Median, alternative, seed = 7L)
      Seq(
        ("exact_two_sided_median", "poibin_exact", "two_sided", c2, "r_tests_exact_two_sided"),
        ("exact_greater_median", "poibin_exact", "greater", c1, "r_tests_exact_greater"),
        ("exact_less_median", "poibin_exact", "less", c1, "r_tests_exact_less"),
        ("score_two_sided_median", "score", "two_sided", c2, "r_tests_score_two_sided"),
        ("wald_two_sided_median", "wald", "two_sided", c2, "")
      ).foreach { case (key, method, alternative, c, rKey) =>
        val table = run(method, alternative)
        val ref = py.get(key)
        val z = column(table, "z_raw")
        val refZ = doubles(ref, "z")
        // pprof_py's and R's exact upper tails are 1 − cdf + pmf/2, unreliable below 1e-7 (X-024): compare
        // z where the upper tail is at least 1e-7; the algorithm itself is checked against mpmath.
        val reliable = refZ.indices.filter { i =>
          method != "poibin_exact" || (alternative match {
            case "greater" => math.abs(refZ(i)) <= UpperReliable
            case _         => refZ(i) <= UpperReliable
          })
        }
        assert(
          refZ.indices.filterNot(reliable.contains).forall(i => math.abs(z(i)) > UpperReliable),
          s"$key: X-024 region"
        )
        check("T-test", s"$key z", reliable.map(z), reliable.map(refZ).toArray)
        checkPValues(key, column(table, "p_value").toSeq, doubles(ref, "p"))
        checkFlags(key, flags(table), ints(ref, "flag").map(_.toInt), z, c)
        if (method != "score") {
          val sides = (if (alternative != "less") Seq("ci_lower")
                       else Nil) ++ (if (alternative != "greater") Seq("ci_upper") else Nil)
          sides.foreach(side =>
            check("T-coef", s"$key $side", column(table, side).toSeq, doubles(ref, side))
          )
        }
        if (rKey.nonEmpty && r.has(rKey)) {
          val theirs = doubles(r.get(rKey), "z")
          // R reads exact tails as 1 − cdf or cdf, so its near-one tails are unreliable both ways.
          val finite = theirs.indices.filter { i =>
            theirs(i).isFinite && reliable.contains(i) &&
            (method != "poibin_exact" || alternative == "less" || math.abs(
              refZ(i)
            ) <= UpperReliable)
          }
          check("T-test", s"$key z vs R", finite.map(z), finite.map(theirs).toArray)
          assert(
            theirs.indices
              .filterNot(finite.contains)
              .forall(i => math.signum(z(i)) == math.signum(theirs(i)) && math.abs(z(i)) > 37.0)
          )
          checkFlags(
            s"$key vs R",
            flags(table),
            doubles(r.get(rKey), "flag").map(_.toInt).toSeq,
            z,
            c
          )
        }
      }
      val exactP = column(run("poibin_exact", "two_sided"), "p_value")
      val bootstrap = run("bootstrap_exact", "two_sided")
      val bootP = column(bootstrap, "p_value")
      exactP.indices.foreach { i =>
        val tail = exactP(i) / 2.0 // two-sided p is twice the smaller tail
        val se = 2.0 * math.sqrt(tail * (1.0 - tail) / 10000.0)
        assert(
          math.abs(bootP(i) - exactP(i)) <= 4.0 * se + 1e-4,
          s"bootstrap p of provider $i: ${bootP(i)} vs exact ${exactP(i)}"
        )
      }
      val reshuffled = LogisticProviderTests.test(
        frame(spark, name, reverse = true).repartition(5),
        fit,
        "bootstrap_exact",
        seed = 7L
      )
      assertEquals(
        column(reshuffled, "z_raw").toSeq.map(java.lang.Double.doubleToRawLongBits),
        column(bootstrap, "z_raw").toSeq.map(java.lang.Double.doubleToRawLongBits)
      )
      assertEquals(bootstrap.columns.toSeq, "provider" +: LogisticProviderTests.Columns)
    }
  }
}

class LogisticProviderTestsSuite extends LogisticProviderTestsBase(Cases.take(3)) {

  test("Poisson-binomial tails match mpmath at 60 digits in both directions (X-024)") {
    val tails = Fixtures.json("logistic/poibin-tails.json")
    tails.properties().forEach { vector =>
      val probabilities = Fixtures.doubles(vector.getValue.get("probabilities"))
      val rows = vector.getValue.get("tails")
      (0 until rows.size).foreach { i =>
        val o = rows.get(i).get("observed").asInt
        val t = PoissonBinomial.tails(probabilities, Array.fill(probabilities.length)(1), o)
        val exact = Fixtures.doubles(rows.get(i).get("mpmath"))
        Seq(t.upperMid, t.lowerMid, t.atLeast, t.atMost).zip(exact).foreach { case (a, e) =>
          assert(
            Tolerances("T-test").worstRatio(Array(a), Array(e)) <= 1.0,
            s"${vector.getKey} o = $o: $a vs $e"
          )
        }
      }
    }
  }

  test("the mean and a fixed reference effect, and a subset of providers") {
    val name = "lfe-base"
    val df = frame(spark, name)
    val fit = LogisticFE.fit(df, spec(name))
    val py = json(name, "pprof_py").get("provider_tests")
    Seq(
      ("exact_two_sided_mean", "poibin_exact", EffectReference.Mean),
      ("exact_two_sided_value", "poibin_exact", EffectReference.Value(-0.5)),
      ("score_two_sided_mean", "score", EffectReference.Mean)
    ).foreach { case (key, method, reference) =>
      val table = LogisticProviderTests.test(df, fit, method, reference)
      check(
        "T-coef",
        s"$key null value",
        Seq(table.select("null_value").head().getDouble(0)),
        doubles(py.get(key), "null_value")
      )
      check(
        "T-test",
        s"$key z",
        table.select("z_raw").collect().map(_.getDouble(0)).toSeq,
        doubles(py.get(key), "z")
      )
    }
    val subset = LogisticProviderTests.test(df, fit, providers = Some(Seq(2L, 5L)))
    assertEquals(subset.select("provider").collect().map(_.getLong(0)).toSeq, Seq(2L, 5L))
    intercept[IllegalArgumentException](LogisticProviderTests.test(df, fit, "midp"))
  }
}

class LogisticProviderTestsMoreSuite extends LogisticProviderTestsBase(Cases.drop(3))
