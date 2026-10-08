package pprof.spark.engine.logistic

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType
import pprof.spark.engine.layout.GroupKey
import pprof.spark.engine.logistic.LogisticFixtures.check
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** Slice 2f-3 against pprof_py's stage 2 and lme4 (docs/spec/logistic/three-stage-stage2.md §7). */
class ThreeStageStage2Suite extends SparkSuite {

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

  /** 2f-1's prepared records with the offset from pprof_py's β, so stage 2 sees pprof_py's data. */
  private def cells(name: String, reference: JsonNode): ThreeStageCells = {
    val beta = d(reference.get("beta"))
    val offset = spec(name).features.zip(beta).foldLeft(lit(0.0)) { case (acc, (f, b)) =>
      acc + col(f).cast(DoubleType) * lit(b)
    }
    ThreeStage.compress(
      ThreeStage.prepare(input(name), spec(name)).data.withColumn("stage1_offset", offset),
      spec(name)
    )
  }

  for (name <- Seq("ts-golden", "ts-synthetic", "ts-shuffled")) {
    test(
      s"$name: the Laplace deviance at fixed parameters and the optimum match pprof_py (and glmer)"
    ) {
      val ref = Fixtures.json(s"three-stage/$name/pprof_py.json").get("stage2")
      val c = cells(name, ref)
      val tight = ref.get("tight")
      assertEquals(
        c.providers.map(k => GroupKey.value(k).toString),
        texts(tight.get("providers")).toVector
      )
      assertEquals(
        c.clusters.map(k => GroupKey.value(k).toString),
        texts(tight.get("clusters")).toVector
      )
      Seq("optimum", "fixed", "boundary").foreach { at =>
        val f = ref.get("function").get(at)
        val sigma = d(f.get("sigma"))
        val (deviance, u) = ThreeStage.laplace(c, sigma(0), sigma(1), d(f.get("intercept"))(0))
        check("T-fn", s"deviance at $at", Seq(deviance), d(f.get("deviance")))
        check("T-coef", s"u at $at", u.toSeq, d(f.get("u")))
      }
      val fit = ThreeStage.stage2(c, ThreeStageStage2Options())
      assert(fit.converged, s"projected gradient ${fit.projectedGradient}")
      check("T-opt", "sigmas", Seq(fit.sigmaProvider, fit.sigmaCluster), d(tight.get("sigma")))
      check("T-opt", "intercept", Seq(fit.intercept), d(tight.get("intercept")))
      check("T-opt", "provider BLUPs", fit.blupProviders.toSeq, d(tight.get("blup_providers")))
      check("T-opt", "cluster BLUPs", fit.blupClusters.toSeq, d(tight.get("blup_clusters")))
      val theirs = d(tight.get("deviance"))(0)
      assert(
        fit.deviance <= theirs + 1e-9 * math.abs(theirs),
        s"deviance ${fit.deviance} above pprof_py's $theirs"
      )
      val glmer = Fixtures.json(s"three-stage/$name/r_threestage.json").get("glmer_stage2")
      if (d(glmer.get("saturated"))(0) == 0.0) {
        check(
          "T-opt",
          "sigmas vs glmer",
          Seq(fit.sigmaProvider, fit.sigmaCluster),
          d(glmer.get("sigma"))
        )
        check(
          "T-opt",
          "provider BLUPs vs glmer",
          fit.blupProviders.toSeq,
          d(glmer.get("blup_providers"))
        )
      }
      assertEquals(fit.start.toSeq, fit.blupProviders.toSeq.map(_ + fit.intercept))
    }
  }

  test("stage 2 is bitwise invariant to row order and partitioning") {
    val name = "ts-golden"
    val ref = Fixtures.json(s"three-stage/$name/pprof_py.json").get("stage2")
    val beta = d(ref.get("beta"))
    val offset = spec(name).features.zip(beta).foldLeft(lit(0.0)) { case (acc, (f, b)) =>
      acc + col(f).cast(DoubleType) * lit(b)
    }
    val prepared =
      ThreeStage.prepare(input(name), spec(name)).data.withColumn("stage1_offset", offset)
    val a = ThreeStage.stage2(prepared, spec(name))
    val b = ThreeStage.stage2(prepared.orderBy(col("rid").desc).repartition(5), spec(name))
    def bits(v: Seq[Double]) = v.map(java.lang.Double.doubleToRawLongBits)
    assertEquals(
      bits(Seq(b.sigmaProvider, b.sigmaCluster, b.intercept)),
      bits(Seq(a.sigmaProvider, a.sigmaCluster, a.intercept))
    )
  }
}
