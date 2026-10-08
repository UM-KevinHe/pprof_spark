package pprof.spark.engine.logistic

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType
import pprof.spark.engine.layout.GroupKey
import pprof.spark.engine.logistic.LogisticFixtures.check
import pprof.spark.numerics.GaussHermite
import pprof.spark.testkit.{Fixtures, SparkSuite}

/** Slice 2f-2 against pprof_py's stage 3 (docs/spec/logistic/three-stage-stage3.md §9). */
class ThreeStageStage3Suite extends SparkSuite {

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

  /** 2f-1's prepared records with the offset from pprof_py's β, so stage 3 sees pprof_py's inputs. */
  private def prepared(name: String, reference: JsonNode): DataFrame = {
    val beta = d(reference.get("beta"))
    val offset = spec(name).features.zip(beta).foldLeft(lit(0.0)) { case (acc, (f, b)) =>
      acc + col(f).cast(DoubleType) * lit(b)
    }
    ThreeStage.prepare(input(name), spec(name)).data.withColumn("stage1_offset", offset)
  }

  for (name <- Seq("ts-golden", "ts-synthetic")) {
    test(
      s"$name: stage 3 matches pprof_py at fixed effects, in lockstep, converged and as sigma -> 0"
    ) {
      val ref = Fixtures.json(s"three-stage/$name/pprof_py.json").get("stage3")
      val cells = ThreeStage.compress(prepared(name, ref), spec(name))
      assertEquals(
        cells.providers.map(k => GroupKey.value(k).toString),
        texts(ref.get("providers")).toVector
      )
      assertEquals(
        cells.clusters.map(k => GroupKey.value(k).toString),
        texts(ref.get("clusters")).toVector
      )
      val sigma = d(ref.get("sigma"))(0)
      val start = d(ref.get("start"))
      Seq("start", "fixed", "tight").foreach { at =>
        val f = ref.get("function").get(at)
        val (loglik, score) = ThreeStage.marginal(cells, sigma, d(f.get("gamma")))
        check("T-fn", s"log-likelihood at $at", Seq(loglik), d(f.get("loglik")))
        // At the tight fit both scores are rounding noise around zero, so a relative comparison is
        // meaningless there: check optimality instead (as the calibration does).
        if (at == "tight")
          assert(
            score.forall(v => math.abs(v) <= 1e-9),
            s"score at the tight fit: ${score.map(math.abs).max}"
          )
        else check("T-fn", s"score at $at", score.toSeq, d(f.get("score")))
      }
      (1 to 3).foreach { k =>
        val r =
          ThreeStage.stage3(cells, sigma, start, ThreeStageStage3Options(maxIter = k, tol = 1e-14))
        check("T-iter", s"iterate $k", r.gamma.toSeq, d(ref.get("iterates").get(k - 1)))
      }
      val tight = ThreeStage.stage3(cells, sigma, start, ThreeStageStage3Options(tol = 1e-10))
      val t = ref.get("tight")
      assert(tight.converged)
      check("T-coef", "tight gamma", tight.gamma.toSeq, d(t.get("gamma")))
      check("T-fn", "tight log-likelihood", Seq(tight.loglik), d(t.get("loglik")))
      check("T-meas", "posterior means", tight.alphaMean.toSeq, d(t.get("alpha_mean")))
      check("T-meas", "posterior variances", tight.alphaVar.toSeq, d(t.get("alpha_var")))
      val default = ThreeStage.stage3(cells, sigma, start, ThreeStageStage3Options())
      check("T-coef", "default gamma", default.gamma.toSeq, d(ref.get("default").get("gamma")))
      val limit = d(ref.get("zero_sigma_limit"))
      val small = ThreeStage.stage3(cells, 1e-4, limit, ThreeStageStage3Options(tol = 1e-10))
      check("T-coef", "sigma 1e-4", small.gamma.toSeq, d(ref.get("small_sigma").get("gamma")))
      val zero = ThreeStage.stage3(cells, 0.0, limit, ThreeStageStage3Options(tol = 1e-12))
      assert(zero.converged)
      check("T-coef", "the sigma = 0 limit (X-005)", zero.gamma.toSeq, limit)
    }
  }

  test("Gauss–Hermite rules match mpmath at 50 digits") {
    val rules = Fixtures.json("three-stage/gauss-hermite.json")
    rules.fieldNames().forEachRemaining { n =>
      val rule = GaussHermite.rule(n.toInt)
      check("T-fn", s"$n nodes", rule.nodes.toSeq, d(rules.get(n).get("nodes")))
      check("T-fn", s"$n weights", rule.weights.toSeq, d(rules.get(n).get("weights")))
    }
  }

  test("compression and stage 3 are bitwise invariant to row order and partitioning") {
    val name = "ts-synthetic"
    val ref = Fixtures.json(s"three-stage/$name/pprof_py.json").get("stage3")
    val a = prepared(name, ref)
    val b = a.orderBy(col("rid").desc).repartition(5)
    val start = d(ref.get("start"))
    val sigma = d(ref.get("sigma"))(0)
    val fa = ThreeStage.stage3(a, spec(name), sigma, start)
    val fb = ThreeStage.stage3(b, spec(name), sigma, start)
    assertEquals(
      fb.gamma.toSeq.map(java.lang.Double.doubleToRawLongBits),
      fa.gamma.toSeq.map(java.lang.Double.doubleToRawLongBits)
    )
    intercept[IllegalArgumentException](ThreeStage.stage3(a, spec(name), -1.0, start))
    intercept[IllegalArgumentException](ThreeStage.stage3(a, spec(name), sigma, start.drop(1)))
  }
}
