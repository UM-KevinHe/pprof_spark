package pprof.spark.engine.logistic

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType
import pprof.spark.engine.logistic.LogisticFixtures.check
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** `sigma_sensitivity` against pprof_py (docs/spec/logistic/three-stage-pipeline.md §4, §8). */
abstract class ThreeStageSensitivityBase(cases: Seq[String]) extends SparkSuite {

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"three-stage/$name/input.csv").toString)

  private def texts(node: JsonNode): Seq[String] = (0 until node.size).map(node.get(_).asText())

  private def spec(name: String): ThreeStageSpec =
    ThreeStageSpec(
      "Y",
      texts(Fixtures.json(s"three-stage/$name/case.json").get("features")),
      "fac",
      "hosp",
      Some("rid")
    )

  private def d(node: JsonNode): Array[Double] = Fixtures.doubles(node)

  private def ints(node: JsonNode): Seq[Int] = (0 until node.size).map(node.get(_).asInt)

  for (name <- cases) {
    test(
      s"$name: sigma_c's profile interval and stage 3 at its ends match pprof_py (sigma = 0 as the limit)"
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
      val stage2 = ThreeStage.stage2(cells, ThreeStageStage2Options())
      val (lo90, hi90) =
        ThreeStagePipeline.profileInterval(cells, stage2, 0.9, ThreeStageStage2Options())
      check(
        "T-opt",
        "0.9 profile interval",
        Seq(stage2.sigmaCluster, lo90, hi90),
        d(inf.get("profile").get("0.9"))
      )
      val s = ThreeStagePipeline.sigmaSensitivity(records, cells, spec(name), stage2)
      check(
        "T-opt",
        "0.95 profile interval",
        Seq(s.estimate, s.lower, s.upper),
        d(inf.get("profile").get("0.95"))
      )
      val flags = s.flags.orderBy("fac").collect()
      val sensitivity = inf.get("sensitivity")
      if (sensitivity.has("sigma")) {
        check(
          "T-opt",
          "sensitivity sigmas",
          Seq(s.lower, s.estimate, s.upper),
          d(sensitivity.get("sigma"))
        )
        Seq("lower", "estimate", "upper").foreach { at =>
          assertEquals(
            flags.map(_.getAs[Int](at)).toSeq,
            ints(sensitivity.get("flags").get(at)),
            s"$name flags at $at"
          )
        }
        assertEquals(
          flags.map(_.getAs[Boolean]("stable")).toSeq,
          (0 until sensitivity.get("stable").size).map(sensitivity.get("stable").get(_).asBoolean)
        )
      } else {
        assertEquals(s.lower, 0.0, "the interval reaches 0 (X-005)")
        assert(
          s.fits("lower").converged && s.fits("lower").gamma.forall(v => !v.isNaN && !v.isInfinite)
        )
        check(
          "T-opt",
          "stage 3 at the upper limit",
          s.fits("upper").gamma.toSeq,
          d(sensitivity.get("upper_gamma"))
        )
        assertEquals(
          flags.map(_.getAs[Int]("upper")).toSeq,
          ints(sensitivity.get("upper_flags")),
          s"$name flags at the upper limit"
        )
      }
    }
  }
}

class ThreeStageSensitivitySuite extends ThreeStageSensitivityBase(Seq("ts-golden"))

class ThreeStageSensitivityMoreSuite
    extends ThreeStageSensitivityBase(Seq("ts-synthetic", "ts-shuffled"))
