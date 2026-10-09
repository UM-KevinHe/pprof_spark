package pprof.spark.engine.logistic

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType
import pprof.spark.engine.layout.GroupKey
import pprof.spark.engine.logistic.LogisticFixtures.{check, checkPValues}
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** Slice 2f-4a against pprof_py's stage 3 inference (docs/spec/logistic/three-stage-pipeline.md §8). */
abstract class ThreeStagePipelineBase(cases: Seq[String]) extends SparkSuite {

  protected def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"three-stage/$name/input.csv").toString)

  protected def texts(node: JsonNode): Seq[String] = (0 until node.size).map(node.get(_).asText())

  protected def spec(name: String): ThreeStageSpec =
    ThreeStageSpec(
      "Y",
      texts(Fixtures.json(s"three-stage/$name/case.json").get("features")),
      "fac",
      "hosp",
      Some("rid")
    )

  protected def d(node: JsonNode): Array[Double] = Fixtures.doubles(node)

  private def column(df: DataFrame, name: String): Array[Double] =
    df.orderBy("fac")
      .select(name)
      .collect()
      .map(r => if (r.isNullAt(0)) Double.NaN else r.getAs[Number](0).doubleValue)

  /** z where pprof_py's tail is reliable (X-024): its `poibin_exact` upper tails come from 1 − cdf, and a
    * one-sided tail near 1 is a rounded sum of the distribution (1 − 3e-16 where it is exactly 1), so z there
    * is noise while pprof_spark reads the tail's complement.
    */
  private def reliable(method: String, alternative: String, z: Array[Double]): Seq[Int] =
    z.indices.filter { i =>
      (method, alternative) match {
        case ("poibin_exact", "greater") => math.abs(z(i)) <= 5.199
        case ("poibin_exact", _)         => z(i) <= 5.199
        case ("exact", "greater")        => z(i) >= -5.199
        case ("exact", "less")           => z(i) <= 5.199
        case _                           => true
      }
    }

  for (name <- cases) {
    test(
      s"$name: stage 3's tests, measures and intervals match pprof_py at its own stage outputs"
    ) {
      val py = Fixtures.json(s"three-stage/$name/pprof_py.json")
      val inf = py.get("inference")
      val beta = d(py.get("stage2").get("beta"))
      val offset = spec(name).features.zip(beta).foldLeft(lit(0.0)) { case (acc, (f, b)) =>
        acc + col(f).cast(DoubleType) * lit(b)
      }
      val records =
        ThreeStage.prepare(input(name), spec(name)).data.withColumn("stage1_offset", offset)
      val cells = ThreeStage.compress(records, spec(name))
      assertEquals(
        cells.providers.map(k => GroupKey.value(k).toString),
        texts(inf.get("providers")).toVector
      )
      val stage3 = ThreeStageStage3(
        cells.providers,
        cells.clusters,
        d(inf.get("gamma")),
        Double.NaN,
        true,
        false,
        0,
        0.0,
        Double.NaN,
        d(inf.get("alpha_mean")),
        d(inf.get("alpha_var")),
        Array.empty,
        cells.cells.length,
        cells.bins,
        ThreeStageStage3Options()
      )
      Seq(
        ("exact_two_sided", "exact", "two_sided"),
        ("exact_greater", "exact", "greater"),
        ("exact_less", "exact", "less"),
        ("poibin_exact", "poibin_exact", "two_sided")
      ).foreach { case (key, method, alternative) =>
        val table =
          ThreeStagePipeline.test(records, spec(name), stage3, method, alternative = alternative)
        val ref = inf.get("tests").get(key)
        val z = column(table, "z_raw")
        val keep = reliable(method, alternative, d(ref.get("z")))
        check("T-test", s"$key z", keep.map(z), keep.map(d(ref.get("z"))).toArray)
        assert(
          z.indices.filterNot(keep.contains).forall(i => math.abs(z(i)) > 5.199),
          s"$key: X-024 region"
        )
        checkPValues(key, column(table, "p_value").toSeq, d(ref.get("p")))
        val flags = table.orderBy("fac").select("flag").collect().map(_.getInt(0)).toSeq
        val theirs = (0 until ref.get("flag").size).map(i => ref.get("flag").get(i).asInt)
        flags.indices.filter(i => flags(i) != theirs(i)).foreach { i =>
          val c = if (alternative == "two_sided") 1.959963984540054 else 1.6448536269514722
          assert(
            math.abs(math.abs(z(i)) - c) <= 1e-8 * c,
            s"$key flag $i differs away from the threshold"
          )
        }
        val sides = (if (alternative != "less") Seq("ci_lower")
                     else Nil) ++ (if (alternative != "greater") Seq("ci_upper") else Nil)
        sides.foreach(side =>
          check("T-coef", s"$key $side", column(table, side).toSeq, d(ref.get(side)))
        )
      }
      val resampled = column(
        ThreeStagePipeline.test(records, spec(name), stage3, "resampling", seed = 3L),
        "p_value"
      )
      val theirs = d(inf.get("tests").get("resampling").get("p"))
      resampled.indices.foreach { i =>
        val t = theirs(i) / 2.0
        val se = 2.0 * math.sqrt(t * (1.0 - t) / 10000.0)
        assert(
          math.abs(resampled(i) - theirs(i)) <= 4.0 * math.sqrt(2.0) * se + 2e-4,
          s"resampling p $i: ${resampled(i)} vs ${theirs(i)}"
        )
      }
      Seq[(String, EffectReference)](
        "median" -> EffectReference.Median,
        "mean" -> EffectReference.Mean,
        "value" -> EffectReference.Value(-1.0)
      )
        .foreach { case (label, reference) =>
          val tables =
            ThreeStagePipeline.measures(records, cells, spec(name), stage3, reference = reference)
          Seq("indirect", "direct").foreach { kind =>
            Seq(s"${kind}_ratio", s"${kind}_rate", "observed", "expected").foreach { c =>
              check(
                "T-meas",
                s"$label $kind $c",
                column(tables(kind), c).toSeq,
                d(inf.get("measures").get(label).get(kind).get(c))
              )
            }
          }
        }
      val gamma =
        ThreeStagePipeline.intervals(records, cells, spec(name), stage3, "gamma")("gamma_ci")
      Seq("gamma_lower", "gamma_upper").foreach(c =>
        check("T-coef", c, column(gamma, c).toSeq, d(inf.get("intervals").get("gamma").get(c)))
      )
      val sm = ThreeStagePipeline.intervals(
        records,
        cells,
        spec(name),
        stage3,
        "SM",
        Seq("indirect", "direct")
      )
      sm.foreach { case (key, table) =>
        val ref = inf.get("intervals").get(key)
        ref.fieldNames().forEachRemaining { c =>
          if (c.startsWith("ci_"))
            check("T-meas", s"$key $c", column(table, c).toSeq, d(ref.get(c)))
        }
      }
    }
  }
}

class ThreeStagePipelineSuite extends ThreeStagePipelineBase(Seq("ts-golden")) {

  test("the pipeline end to end: stage 3 and its exact test within T-opt of pprof_py's") {
    val name = "ts-golden"
    val inf = Fixtures.json(s"three-stage/$name/pprof_py.json").get("inference")
    val fit = ThreeStagePipeline.fit(input(name), spec(name))
    check("T-opt", "stage 3 effects", fit.stage3.gamma.toSeq, d(inf.get("gamma")))
    check("T-opt", "posterior means", fit.stage3.alphaMean.toSeq, d(inf.get("alpha_mean")))
    val table = ThreeStagePipeline.test(fit.preparation.data, spec(name), fit.stage3)
    val z = table.orderBy("fac").select("z_raw").collect().map(_.getDouble(0)).toSeq
    check("T-opt", "exact test z", z, d(inf.get("tests").get("exact_two_sided").get("z")))
  }
}

class ThreeStagePipelineMoreSuite extends ThreeStagePipelineBase(Seq("ts-synthetic", "ts-shuffled"))
