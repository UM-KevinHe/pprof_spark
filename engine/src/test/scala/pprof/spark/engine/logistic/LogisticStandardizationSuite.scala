package pprof.spark.engine.logistic

import org.apache.spark.sql.DataFrame
import pprof.spark.engine.logistic.LogisticFixtures._
import pprof.spark.testkit.{SparkSuite, Tolerances}

/** Slice 2d against pprof_py and R pprof (docs/spec/logistic/standardization.md §7), split over two suites. */
abstract class LogisticStandardizationBase(cases: Seq[String]) extends SparkSuite {

  private def column(table: DataFrame, name: String): Array[Double] =
    table
      .select(name)
      .collect()
      .map(r => if (r.isNullAt(0)) Double.NaN else r.getAs[Number](0).doubleValue)

  for (name <- cases) {
    test(s"$name: measures, standard errors and tests match pprof_py and R") {
      val df = frame(spark, name)
      val fit = LogisticFE.fit(df, spec(name))
      val ref = json(name, "pprof_py").get("standardization")
      Seq[(String, EffectReference, Double)](
        ("median", EffectReference.Median, 0.0),
        ("mean", EffectReference.Mean, 0.0),
        ("value", EffectReference.Value(-0.5), 0.0),
        ("extreme", EffectReference.Median, 50.0)
      ).foreach { case (label, reference, extreme) =>
        val tables =
          LogisticStandardization.measures(df, fit, reference = reference, extremeTrials = extreme)
        Seq(
          "indirect" -> Seq("indirect_ratio", "indirect_rate", "observed", "expected"),
          "direct" -> Seq("direct_ratio", "direct_rate", "observed", "expected", "n_pop")
        ).foreach { case (kind, columns) =>
          columns.foreach { c =>
            check(
              "T-meas",
              s"$label $kind $c",
              column(tables(kind), c).toSeq,
              doubles(ref.get("measures").get(label).get(kind), c)
            )
          }
        }
      }
      val measures = ref.get("measure")
      measures.fieldNames().forEachRemaining { key =>
        val measure =
          LogisticStandardization.Measures.filter(m => key.startsWith(m + "_")).maxBy(_.length)
        val rest = key.substring(measure.length + 1)
        val variance = rest.substring(0, rest.lastIndexOf('_'))
        val indirect = rest.substring(rest.lastIndexOf('_') + 1)
        val m = LogisticStandardization.measure(
          df,
          fit,
          measure,
          EffectReference.Median,
          variance,
          indirect
        )
        val r = measures.get(key)
        check("T-meas", s"$key estimate", m.estimate.toSeq, doubles(r, "estimate"))
        check("T-meas", s"$key se", m.se.toSeq, doubles(r, "se"))
        check("T-meas", s"$key reference", Seq(m.referenceValue), doubles(r, "reference_value"))
      }
      val tests = ref.get("tests")
      tests.fieldNames().forEachRemaining { key =>
        val split = key.lastIndexOf('_')
        val (measure, transform) = (key.substring(0, split), key.substring(split + 1))
        val table = LogisticStandardization.test(df, fit, measure, transform = transform)
        val r = tests.get(key)
        val z = column(table, "z_raw")
        val expected = doubles(r, "z")
        val finite = expected.indices.filter(i => expected(i).isFinite)
        assertEquals(
          z.indices.filterNot(i => z(i).isFinite),
          expected.indices.filterNot(i => expected(i).isFinite),
          s"$key untested"
        )
        check("T-test", s"$key z", finite.map(z), finite.map(expected).toArray)
        checkPValues(key, finite.map(column(table, "p_value")), finite.map(doubles(r, "p")).toArray)
        Seq("ci_lower", "ci_upper").foreach { side =>
          check(
            "T-coef",
            s"$key $side",
            finite.map(column(table, side)),
            finite.map(doubles(r, side)).toArray
          )
        }
        val flags = table
          .select("flag")
          .collect()
          .map(row => if (row.isNullAt(0)) None else Some(row.getInt(0)))
          .toSeq
        val theirs = (0 until r.get("flag").size)
          .map(i => if (r.get("flag").get(i).isNull) None else Some(r.get("flag").get(i).asInt))
        flags.indices.filter(i => flags(i) != theirs(i)).foreach { i =>
          assert(
            math.abs(math.abs(z(i)) - 1.959963984540054) <= 1e-8 * 1.96,
            s"$key flag $i differs away from the threshold"
          )
        }
      }
      val r = json(name, "r_logistic")
      if (r.has("r_sm")) {
        val tables = LogisticStandardization.measures(df, fit)
        Seq(
          "indirect_ratio" -> "indirect",
          "indirect_rate" -> "indirect",
          "direct_ratio" -> "direct",
          "direct_rate" -> "direct"
        )
          .foreach { case (c, kind) =>
            check("T-meas", s"$c vs R", column(tables(kind), c).toSeq, doubles(r.get("r_sm"), c))
          }
      }
    }
  }
}

class LogisticStandardizationSuite extends LogisticStandardizationBase(Cases.take(3)) {

  test("binned direct sums equal the exact sums within T-part, and the inputs fail clearly") {
    val name = "lfe-many"
    val df = frame(spark, name)
    val fit = LogisticFE.fit(df, spec(name))
    val binned = LogisticStandardization.measure(df, fit, "direct_rate")
    val exact = LogisticStandardization.measure(df, fit, "direct_rate", method = "exact")
    assert(Tolerances("T-part.parameters").worstRatio(binned.estimate, exact.estimate) <= 1.0)
    assert(Tolerances("T-part.parameters").worstRatio(binned.se, exact.se) <= 1.0)
    intercept[IllegalArgumentException](
      LogisticStandardization.measure(df, fit, "indirect_ratio", variance = "robust")
    )
    intercept[IllegalArgumentException](
      LogisticStandardization.measure(df, fit, "direct_rate", variance = "robust")
    )
    intercept[IllegalArgumentException](LogisticStandardization.measure(df, fit, "odds"))
    intercept[IllegalArgumentException](
      LogisticStandardization.test(df, fit, "direct_rate", transform = "probit")
    )
  }
}

class LogisticStandardizationMoreSuite extends LogisticStandardizationBase(Cases.drop(3))
