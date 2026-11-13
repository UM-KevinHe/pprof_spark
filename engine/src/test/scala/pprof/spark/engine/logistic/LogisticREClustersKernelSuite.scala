package pprof.spark.engine.logistic

import scala.io.Source

import com.fasterxml.jackson.databind.JsonNode
import pprof.spark.numerics.kernels.{LogisticREClusters => Kernel}
import pprof.spark.testkit.{Fixtures, Tolerances}

/** The logistic cluster-factor kernel (spec 4d-2, D-72) on every `fixtures/logistic-re-clusters` case, its rows built
  * here without Spark: σ̂, β̂, the BLUPs and the posterior SEs against pprof_py's and glmer's tightened fits (T-opt),
  * the log-likelihood (T-fn), and D-59's optimality check.
  */
class LogisticREClustersKernelSuite extends munit.FunSuite {

  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(5, "min")

  private def listed(node: JsonNode): Array[Double] =
    if (node.isArray) Fixtures.doubles(node) else Array(java.lang.Double.parseDouble(node.asText()))

  private def rows(name: String): (Kernel.Rows, Seq[String], Seq[Vector[String]]) = {
    val c = Fixtures.json(s"logistic-re-clusters/$name/case.json")
    val clusters =
      (0 until c.get("cluster_vars").size).map(i => c.get("cluster_vars").get(i).asText())
    val lines = Source
      .fromFile(Fixtures.root.resolve(s"logistic-re-clusters/$name/input.csv").toFile)
      .getLines()
      .toVector
    val header = lines.head.split(",").toVector
    val data = lines.tail.map(_.split(",", -1).toVector)
    def col(n: String): Option[Int] = Some(header.indexOf(n)).filter(_ >= 0)
    val factors = "provider" +: clusters
    val keys = factors.map { f =>
      val raw = data.map(r => r(col(f).get)).distinct
      if (raw.forall(_.matches("-?\\d+"))) raw.sortBy(_.toLong) else raw.sorted
    }
    val n = data.size
    val x =
      data.flatMap(r => Seq(1.0, r(col("x1").get).toDouble, r(col("x2").get).toDouble)).toArray
    val index = factors.indices
      .map(k => data.map(r => keys(k).indexOf(r(col(factors(k)).get))).toArray)
      .toArray
    (
      Kernel.Rows(
        n,
        3,
        x,
        data.map(r => r(col("y").get).toDouble).toArray,
        data.map(r => col("o").fold(0.0)(i => r(i).toDouble)).toArray,
        index,
        keys.map(_.size).toArray
      ),
      factors,
      keys
    )
  }

  private def check(
      cls: String,
      what: String,
      actual: Seq[Double],
      expected: Array[Double]
  ): Unit = {
    val value = Tolerances(cls).worstRatio(actual.toArray, expected)
    assert(
      value <= 1.0,
      s"$what: $value of $cls\n  ours   ${actual.take(6).mkString(", ")}\n  theirs ${expected.take(6).mkString(", ")}"
    )
  }

  for (name <- Seq("lgc-one", "lgc-two", "lgc-text", "lgc-offset", "lgc-nested", "lgc-boundary")) {
    test(s"$name: the two-stage fit against pprof_py and glmer, and the optimality check") {
      val (r, factors, keys) = rows(name)
      val ybar = r.y.sum / r.n
      val fit = Kernel.fit(r, Array(StrictMath.log(ybar / (1.0 - ybar)), 0.0, 0.0))
      val py = Fixtures.json(s"logistic-re-clusters/$name/pprof_py.json").get("tight")
      val gl = Fixtures.json(s"logistic-re-clusters/$name/r_lme4.json")
      for ((who, ref) <- Seq("pprof_py" -> py, "glmer" -> gl)) {
        check("T-opt", s"$name sigma vs $who", fit.sigma.toSeq, listed(ref.get("sigma")))
        check("T-opt", s"$name beta vs $who", fit.beta.toSeq, listed(ref.get("beta")))
        check("T-fn", s"$name log-likelihood vs $who", Seq(fit.logLik), listed(ref.get("loglik")))
        val offsets = r.levels.scanLeft(0)(_ + _)
        factors.indices.foreach { k =>
          val re = ref.get("random_effects").get(factors(k))
          val theirKeys = (0 until re.get("keys").size).map(i => re.get("keys").get(i).asText())
          val byKeyBlup = theirKeys.zip(listed(re.get("blup"))).toMap
          val byKeySe = theirKeys.zip(listed(re.get("se"))).toMap
          val ours = (0 until r.levels(k)).map(l => fit.blups(offsets(k) + l))
          val se =
            (0 until r.levels(k)).map(l => StrictMath.sqrt(fit.posteriorVariance(offsets(k) + l)))
          check("T-opt", s"$name ${factors(k)} BLUPs vs $who", ours, keys(k).map(byKeyBlup).toArray)
          check(
            "T-opt",
            s"$name ${factors(k)} posterior SEs vs $who",
            se,
            keys(k).map(byKeySe).toArray
          )
        }
        val there = Kernel
          .laplace(r, listed(ref.get("sigma")), listed(ref.get("beta")), new Array[Double](r.q))
          .deviance
        assert(
          fit.deviance <= there + 1e-10 * math.abs(there),
          s"$name: ${fit.deviance} > $there at $who's optimum"
        )
      }
    }
  }
}
