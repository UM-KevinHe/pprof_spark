package pprof.spark.engine.logistic

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}
import pprof.spark.engine.data.{InputProblem, InvalidInputException}
import pprof.spark.engine.layout.GroupKey
import pprof.spark.engine.logistic.LogisticFixtures.check
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** Slice 2f-1 against pprof_py's preparation and stage 1 and R (docs/spec/logistic/three-stage-preparation.md §7). */
class ThreeStageSuite extends SparkSuite {

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"three-stage/$name/input.csv").toString)

  private def texts(node: com.fasterxml.jackson.databind.JsonNode): Seq[String] =
    (0 until node.size).map(node.get(_).asText())

  private def spec(name: String): ThreeStageSpec =
    ThreeStageSpec(
      "Y",
      texts(Fixtures.json(s"three-stage/$name/case.json").get("features")),
      "fac",
      "hosp",
      Some("rid")
    )

  private def bits(values: Seq[Double]): Seq[Long] =
    values.map(java.lang.Double.doubleToRawLongBits)

  for (name <- Fixtures.cases("three-stage")) {
    test(s"$name: preparation and stage 1 match pprof_py and R") {
      val prep = ThreeStage.prepare(input(name), spec(name))
      val py = Fixtures.json(s"three-stage/$name/pprof_py.json")
      val p = py.get("preparation")
      assertEquals(prep.data.count(), p.get("kept").asLong)
      assertEquals(
        Seq(prep.providers, prep.clusters, prep.cells, prep.includedCells),
        Seq("providers", "clusters", "cells", "included_cells").map(p.get(_).asLong)
      )
      val excluded =
        prep.excluded.map(e => GroupKey.value(e._1).toString -> e._2).sortBy(_._1).toList
      assertEquals(excluded.map(_._1), texts(p.get("excluded")).sorted.toList)
      assertEquals(excluded.map(_._2.toString), texts(p.get("excluded_records")).toList)
      val rows = prep.data
        .orderBy("rid")
        .select("rid", "y_adj", "cell", "included", "stage1_offset")
        .collect()
        .toSeq
      assertEquals(rows.map(_.getAs[Number](0).longValue.toString), texts(p.get("rid")))
      check("T-meas", "y_adj", rows.map(_.getDouble(1)), Fixtures.doubles(p.get("y_adj")))
      assertEquals(rows.map(_.getString(2)), texts(p.get("cell")))
      assertEquals(rows.map(_.getInt(3).toString), texts(p.get("included")))
      val s1 = py.get("stage1").get("default")
      check("T-coef", "beta", prep.stage1.estimates, Fixtures.doubles(s1.get("beta")))
      check("T-var", "covariance", prep.stage1.covariance, Fixtures.doubles(s1.get("var_beta")))
      assertEquals(
        prep.stage1.providers.keys.toSeq.map(k => GroupKey.value(k).toString),
        texts(s1.get("cells"))
      )
      check(
        "T-coef",
        "cell effects",
        prep.stage1.providers.gamma.toSeq,
        Fixtures.doubles(s1.get("gamma"))
      )
      check("T-coef", "offset", rows.map(_.getDouble(4)), Fixtures.doubles(s1.get("offset")))
      val tight = ThreeStage.prepare(
        input(name),
        spec(name),
        ThreeStageOptions(stage1 = LogisticOptions(screen = false, tol = 1e-13))
      )
      check(
        "T-coef",
        "tight beta",
        tight.stage1.estimates,
        Fixtures.doubles(py.get("stage1").get("tight").get("beta"))
      )
      val r = Fixtures.json(s"three-stage/$name/r_threestage.json")
      check(
        "T-coef",
        "tight beta vs R glm",
        tight.stage1.estimates,
        Fixtures.doubles(r.get("glm_stage1").get("beta"))
      )
      if (r.has("stage23"))
        check(
          "T-coef",
          "tight beta vs R's stage 1",
          tight.stage1.estimates,
          Fixtures.doubles(r.get("stage23").get("beta"))
        )
    }
  }

  test(
    "results are bitwise invariant to row order and partitioning; the cutoff boundary is strict"
  ) {
    val name = "ts-synthetic"
    val a = ThreeStage.prepare(input(name), spec(name))
    val b = ThreeStage.prepare(input(name).orderBy(col("rid").desc).repartition(7), spec(name))
    assertEquals(bits(b.stage1.estimates), bits(a.stage1.estimates))
    def yAdj(p: ThreeStagePreparation) =
      p.data.orderBy("rid").select("y_adj").collect().map(_.getDouble(0)).toSeq
    assertEquals(bits(yAdj(b)), bits(yAdj(a)))
    val excluded = a.excluded.map(e => GroupKey.value(e._1).toString)
    assert(excluded.contains("F28") && !excluded.contains("F29"), excluded.toString)
  }

  test("invalid inputs fail with counts, reserved names and empty steps clearly") {
    val name = "ts-synthetic"
    val df = input(name)
    val broken = df.withColumn(
      "Y",
      when(col("rid") <= 2, lit(null)).when(col("rid") === 3, lit(2)).otherwise(col("Y"))
    )
    val failure = intercept[InvalidInputException](ThreeStage.prepare(broken, spec(name)))
    assert(
      failure.problems.contains(InputProblem.InvalidValues("Y", 2L, 0L)),
      failure.problems.toString
    )
    assert(
      failure.problems.contains(InputProblem.NonBinaryValues("Y", 1L)),
      failure.problems.toString
    )
    val reserved = intercept[InvalidInputException](
      ThreeStage.prepare(df.withColumn("stage1_offset", lit(0.0)), spec(name))
    )
    assertEquals(reserved.problems, Seq(InputProblem.ReservedColumn("stage1_offset")))
    intercept[IllegalArgumentException](
      ThreeStage.prepare(df, spec(name), ThreeStageOptions(cutoff = 100L))
    )
  }
}
