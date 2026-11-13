package pprof.spark.engine.linear

import scala.io.Source

import pprof.spark.numerics.NeumaierSum
import pprof.spark.numerics.kernels.{LinearREClusters => Kernel}
import pprof.spark.testkit.{Fixtures, Tolerances}

/** The cluster-factor kernel (spec 4d-1, D-70) on every `fixtures/linear-re-clusters` case, from sufficient statistics
  * built here without Spark: the criterion at pprof_py's tightened θ against its objective (T-fn), and the search's
  * optimum against pprof_py's and lme4's (T-opt), REML and ML.
  */
class LinearREClustersKernelSuite extends munit.FunSuite {

  /** A list of hex doubles, or one (R's unboxed scalars). */
  private def listed(node: com.fasterxml.jackson.databind.JsonNode): Array[Double] =
    if (node.isArray) Fixtures.doubles(node) else Array(java.lang.Double.parseDouble(node.asText()))

  /** The sufficient statistics of a case's input, levels in sorted key order (numbers numerically, text as text). */
  private def data(name: String): (Kernel.Data, Seq[String], Seq[Vector[String]]) = {
    val c = Fixtures.json(s"linear-re-clusters/$name/case.json")
    val clusters =
      (0 until c.get("cluster_vars").size).map(i => c.get("cluster_vars").get(i).asText())
    val lines = Source
      .fromFile(Fixtures.root.resolve(s"linear-re-clusters/$name/input.csv").toFile)
      .getLines()
      .toVector
    val header = lines.head.split(",").toVector
    val rows = lines.tail.map(_.split(",", -1).toVector)
    def col(n: String): Option[Int] = Some(header.indexOf(n)).filter(_ >= 0)
    val factorNames = "provider" +: clusters
    val keys = factorNames.map { f =>
      val raw = rows.map(r => r(col(f).get)).distinct
      if (raw.forall(_.matches("-?\\d+"))) raw.sortBy(_.toLong) else raw.sorted
    }
    val index = keys.map(_.zipWithIndex.toMap)
    val p = 3
    val w = rows.map(r => col("w").fold(1.0)(i => r(i).toDouble))
    val o = rows.map(r => col("o").fold(0.0)(i => r(i).toDouble))
    val y = rows.map(r => r(col("y").get).toDouble)
    val x = rows.map(r => Array(1.0, r(col("x1").get).toDouble, r(col("x2").get).toDouble))
    val wsum = new NeumaierSum; val wy = new NeumaierSum
    rows.indices.foreach { i => wsum.add(w(i)); wy.add(w(i) * (y(i) - o(i))) }
    val centre = wy.value / wsum.value
    val yt = rows.indices.map(i => y(i) - o(i) - centre)
    val factors = factorNames.indices.map { k =>
      val levels = keys(k).size
      val weight = Array.fill(levels)(new NeumaierSum);
      val wx = Array.fill(levels * p)(new NeumaierSum);
      val wyl = Array.fill(levels)(new NeumaierSum)
      rows.indices.foreach { i =>
        val l = index(k)(rows(i)(col(factorNames(k)).get))
        weight(l).add(w(i)); wyl(l).add(w(i) * yt(i));
        (0 until p).foreach(j => wx(l * p + j).add(w(i) * x(i)(j)))
      }
      Kernel.Factor(levels, weight.map(_.value), wx.map(_.value), wyl.map(_.value))
    }.toArray
    val pairs = for (k <- factorNames.indices; l <- factorNames.indices if k < l) yield {
      val sums = scala.collection.mutable.TreeMap.empty[(Int, Int), NeumaierSum]
      rows.indices.foreach { i =>
        val key =
          (index(k)(rows(i)(col(factorNames(k)).get)), index(l)(rows(i)(col(factorNames(l)).get)))
        sums.getOrElseUpdate(key, new NeumaierSum).add(w(i))
      }
      Kernel.Pair(
        k,
        l,
        sums.toSeq.map(_._1._1).toArray,
        sums.toSeq.map(_._1._2).toArray,
        sums.toSeq.map(_._2.value).toArray
      )
    }
    val xx = Array.fill(p * p)(new NeumaierSum); val xy = Array.fill(p)(new NeumaierSum);
    val yy = new NeumaierSum; val lw = new NeumaierSum
    rows.indices.foreach { i =>
      (0 until p).foreach { a =>
        xy(a).add(w(i) * x(i)(a) * yt(i));
        (0 until p).foreach(b => xx(a * p + b).add(w(i) * x(i)(a) * x(i)(b)))
      }
      yy.add(w(i) * yt(i) * yt(i)); lw.add(StrictMath.log(w(i)))
    }
    (
      Kernel.Data(
        rows.size.toLong,
        p,
        factors,
        pairs.toArray,
        xx.map(_.value),
        xy.map(_.value),
        yy.value,
        lw.value,
        centre,
        0
      ),
      factorNames,
      keys
    )
  }

  private def check(
      cls: String,
      what: String,
      actual: Seq[Double],
      expected: Array[Double]
  ): Double = {
    val value = Tolerances(cls).worstRatio(actual.toArray, expected)
    assert(
      value <= 1.0,
      s"$what: $value of $cls\n  ours   ${actual.mkString(", ")}\n  theirs ${expected.mkString(", ")}"
    )
    value
  }

  for (
    name <- Seq(
      "lrc-one",
      "lrc-two",
      "lrc-text",
      "lrc-weights",
      "lrc-offset",
      "lrc-nested",
      "lrc-boundary"
    )
  ) {
    test(
      s"$name: the criterion at pprof_py's theta (T-fn) and the optimum against pprof_py and lme4 (T-opt)"
    ) {
      val (d, factorNames, keys) = data(name)
      val py = Fixtures.json(s"linear-re-clusters/$name/pprof_py.json")
      val r = Fixtures.json(s"linear-re-clusters/$name/r_lme4.json")
      for ((mode, reml) <- Seq("reml" -> true, "ml" -> false)) {
        val t = py.get(s"${mode}_tight")
        val theta = Fixtures.doubles(t.get("theta"))
        val at = Kernel.evaluate(d, theta, reml)
        check(
          "T-fn",
          s"$name $mode criterion at pprof_py's theta",
          Seq(at.criterion),
          Fixtures.doubles(t.get("objective"))
        )
        check(
          "T-opt",
          s"$name $mode beta at pprof_py's theta",
          at.beta.toSeq,
          Fixtures.doubles(t.get("beta"))
        )
        val fit = Kernel.fit(d, reml)
        // D-59's optimality check: the search's optimum is no worse than either reference's, by this kernel's criterion
        Seq("pprof_py" -> theta, "lme4" -> listed(r.get(mode).get("theta"))).foreach {
          case (who, other) =>
            val there = Kernel.evaluate(d, other, reml).criterion
            assert(
              fit.evaluation.criterion <= there + 1e-10 * math.abs(there),
              s"$name $mode: ${fit.evaluation.criterion} > $there at $who's optimum"
            )
        }
        check("T-opt", s"$name $mode theta vs pprof_py", fit.theta.toSeq, theta)
        val rr = r.get(mode)
        check("T-opt", s"$name $mode theta vs lme4", fit.theta.toSeq, listed(rr.get("theta")))
        check(
          "T-opt",
          s"$name $mode beta vs lme4",
          fit.evaluation.beta.toSeq,
          listed(rr.get("beta"))
        )
        check(
          "T-opt",
          s"$name $mode sigma vs lme4",
          Seq(math.sqrt(fit.evaluation.sigma2)),
          listed(rr.get("sigma"))
        )
        check(
          "T-fn",
          s"$name $mode criterion vs lme4",
          Seq(fit.evaluation.criterion),
          listed(rr.get("criterion"))
        )
        val se = (0 until d.p).map(j =>
          math.sqrt(fit.evaluation.sigma2 * fit.evaluation.sInverse(j * d.p + j))
        )
        check("T-opt", s"$name $mode standard errors vs lme4", se, listed(rr.get("se")))
        // the BLUPs, factor by factor, against lme4's by key
        val offsets = d.factors.map(_.levels).scanLeft(0)(_ + _)
        factorNames.indices.foreach { k =>
          val ours =
            (0 until d.factors(k).levels).map(l => fit.theta(k) * fit.evaluation.u(offsets(k) + l))
          val re = rr.get("random_effects").get(factorNames(k))
          val theirKeys = (0 until re.get("keys").size).map(i => re.get("keys").get(i).asText())
          val theirs = listed(re.get("blup"))
          val byKey = theirKeys.zip(theirs).toMap
          check(
            "T-opt",
            s"$name $mode BLUPs of ${factorNames(k)}",
            ours,
            keys(k).map(byKey).toArray
          )
        }
      }
    }
  }
}
