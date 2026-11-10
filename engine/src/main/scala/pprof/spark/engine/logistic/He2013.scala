package pprof.spark.engine.logistic

import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.types.{DoubleType, LongType}
import pprof.spark.engine.backend.DriverGuards
import pprof.spark.engine.data.Validation
import pprof.spark.engine.layout.GroupKey
import pprof.spark.numerics.{GaussHermite, NeumaierSum}

/** One provider × cluster cell's records in canonical order: their offsets xᵢᵀβ and outcomes. */
final case class He2013Cell(provider: Int, cluster: Int, offset: Array[Double], y: Array[Double])

/** One cell's sums in a pass of the he2013 iteration. */
final case class He2013Partial(provider: Int, cluster: Int, values: Array[Double])

/** pprof_py's `estimator="he2013"` (docs/spec/logistic/fe-random-cluster-he2013.md §2 to §4): He et al.'s (2013)
  * iteration step for step, on the records grouped in cells, each pass's cell sums added on the driver in cell order.
  */
private[logistic] object He2013 {

  private val InformationFloor = 1e-8

  private def expit(t: Double): Double =
    if (t >= 0.0) 1.0 / (1.0 + StrictMath.exp(-t))
    else {
      val e = StrictMath.exp(t)
      e / (1.0 + e)
    }

  private def log1pExp(t: Double): Double =
    if (t > 0.0) t + StrictMath.log1p(StrictMath.exp(-t)) else StrictMath.log1p(StrictMath.exp(t))

  def run(
      records: DataFrame,
      spec: LogisticFERandomClusterSpec,
      cells3: ThreeStageCells,
      sigma: Double,
      start: Array[Double],
      options: LogisticFERandomClusterOptions
  ): (ThreeStageStage3, Vector[String]) = {
    val m = cells3.providers.size
    val h = cells3.clusters.size
    val k = options.nNodes
    val providerIndex = cells3.providers.zipWithIndex.toMap
    val clusterIndex = cells3.clusters.zipWithIndex.toMap
    val ordered = spec.rowId.isDefined
    // keys as compress reads them: integral keys as Long, others as text
    def key(name: String) = {
      val c = Validation.column(name)
      if (records.schema(name).dataType == org.apache.spark.sql.types.StringType) c
      else c.cast(LongType)
    }
    val columns = Seq(
      key(spec.provider),
      key(spec.cluster),
      spec.rowId.fold(org.apache.spark.sql.functions.lit(0L))(r =>
        Validation.column(r).cast(LongType)
      ),
      org.apache.spark.sql.functions.col("stage1_offset").cast(DoubleType),
      org.apache.spark.sql.functions.col("y_adj").cast(DoubleType)
    )
    val cells = records
      .select(columns: _*)
      .groupByKey((r: Row) =>
        (providerIndex(GroupKey.of(r.get(0))), clusterIndex(GroupKey.of(r.get(1))))
      )(
        Encoders.tuple(Encoders.scalaInt, Encoders.scalaInt)
      )
      .mapGroups { (key: (Int, Int), rows: Iterator[Row]) =>
        val sorted = rows.map(r => (r.getLong(2), r.getDouble(3), r.getDouble(4))).toArray.sortBy {
          case (id, o, y) =>
            if (ordered) (id, 0L, 0L)
            else
              (0L, java.lang.Double.doubleToRawLongBits(o), java.lang.Double.doubleToRawLongBits(y))
        }
        He2013Cell(key._1, key._2, sorted.map(_._2), sorted.map(_._3))
      }(Encoders.product[He2013Cell])
      .persist(options.blocks.resolvedStorageLevel)
    try {
      val count = cells.count()
      DriverGuards.requireWithinBudget(
        s"$count cells' sums at $k nodes each iteration (OI-101)",
        8L * count * (k + 4L),
        options.blocks
      )
      val rule = GaussHermite.rule(k)
      val a = rule.nodes.map(t => StrictMath.sqrt(2.0) * sigma * t)
      val w = rule.weights.map(_ / StrictMath.sqrt(StrictMath.PI))
      def pass(f: He2013Cell => Array[Double]): Array[He2013Partial] =
        cells
          .map((c: He2013Cell) => He2013Partial(c.provider, c.cluster, f(c)))(
            Encoders.product[He2013Partial]
          )
          .collect()
          .sortBy(p => (p.provider, p.cluster))
      var gamma = start.clone()
      var mean = new Array[Double](h)
      var variance = new Array[Double](h)
      var iterations = 0
      var crit = 1.0
      var obj = 0.0
      var objNew = 1.0
      var objInit = Double.NaN
      var floored = 0
      var zeroDenominator = false
      var broke = false
      while (iterations <= options.maxIter && crit >= options.tol && !broke) {
        obj = objNew
        iterations += 1
        // E-step: each cluster's log-likelihood at the fixed nodes, then the posterior moments
        val g = gamma
        val nodes = a
        val e = pass { c =>
          Array.tabulate(nodes.length) { j =>
            val s = new NeumaierSum
            var i = 0
            while (i < c.y.length) {
              val q = expit(-(nodes(j) + g(c.provider) + c.offset(i)))
              s.add(StrictMath.log(c.y(i) * (1.0 - q) + (1.0 - c.y(i)) * q))
              i += 1
            }
            s.value
          }
        }
        val lkd = Array.fill(h)(Array.fill(k)(new NeumaierSum))
        e.foreach(p => (0 until k).foreach(j => lkd(p.cluster)(j).add(p.values(j))))
        mean = new Array[Double](h)
        variance = new Array[Double](h)
        (0 until h).foreach { c =>
          val values = lkd(c).map(_.value)
          val peak = values.max
          val scaled = values.map(v => StrictMath.exp(v - peak))
          val denominator = new NeumaierSum
          val first = new NeumaierSum
          val second = new NeumaierSum
          (0 until k).foreach { j =>
            denominator.add(scaled(j) * w(j))
            first.add(scaled(j) * a(j) * w(j))
            second.add(scaled(j) * a(j) * a(j) * w(j))
          }
          mean(c) = first.value / denominator.value
          variance(c) = second.value / denominator.value - mean(c) * mean(c)
        }
        // M-step: He et al.'s score and information for each provider, one Newton step, the bound
        val mm = mean
        val vv = variance
        val sums = pass { c =>
          val score = new NeumaierSum
          val info = new NeumaierSum
          var i = 0
          while (i < c.y.length) {
            val q = expit(-(mm(c.cluster) + g(c.provider) + c.offset(i)))
            val p = 1.0 - q
            val pq = p * q
            score.add(c.y(i) - p - 0.5 * vv(c.cluster) * pq * (q - p))
            info.add(pq + 0.5 * vv(c.cluster) * pq * (p * p + q * q - 4.0 * pq))
            i += 1
          }
          Array(score.value, info.value)
        }
        val score = Array.fill(m)(new NeumaierSum)
        val info = Array.fill(m)(new NeumaierSum)
        sums.foreach { p =>
          score(p.provider).add(p.values(0))
          info(p.provider).add(p.values(1))
        }
        val previous = gamma
        val updated = Array.tabulate(m) { j =>
          val information = info(j).value
          val floor = information <= InformationFloor
          if (floor) floored += 1
          previous(j) + score(j).value / (if (floor) InformationFloor else information)
        }
        val (low, high) =
          if (options.boundMode == "absolute") (-options.bound, options.bound)
          else {
            val sorted = updated.sorted
            val median =
              if (m % 2 == 1) sorted(m / 2) else 0.5 * (sorted(m / 2 - 1) + sorted(m / 2))
            (median - options.bound, median + options.bound)
          }
        gamma = updated.map(v => math.min(math.max(v, low), high))
        // The objective at the new γ with the same moments; its prior term is 0 at σ = 0 (X-049)
        val gn = gamma
        val prior = if (sigma == 0.0) 0.0 else 1.0 / (2.0 * sigma * sigma)
        val objective = new NeumaierSum
        pass { c =>
          val s = new NeumaierSum
          var i = 0
          while (i < c.y.length) {
            val eta = mm(c.cluster) + gn(c.provider) + c.offset(i)
            val q = expit(-eta)
            val p = 1.0 - q
            s.add(eta * c.y(i) - log1pExp(eta) - 0.5 * vv(c.cluster) * p * q)
            s.add(-(mm(c.cluster) * mm(c.cluster) + vv(c.cluster)) * prior)
            i += 1
          }
          Array(s.value)
        }.foreach(p => objective.add(p.values(0)))
        objNew = objective.value
        crit =
          if (options.convergenceCriterion == "max_delta_gamma")
            gamma.indices.map(j => math.abs(gamma(j) - previous(j))).max
          else if (iterations == 1) {
            objInit = objNew
            1.0
          } else {
            val numerator = objNew - obj
            val denominator = objNew - objInit
            if (denominator == 0.0) {
              if (numerator != 0.0) zeroDenominator = true
              if (numerator == 0.0) 0.0 else Double.PositiveInfinity
            } else math.abs(numerator / denominator)
          }
        if (!java.lang.Double.isFinite(objNew)) {
          crit = Double.PositiveInfinity
          broke = true
        }
      }
      val converged = crit < options.tol
      val warnings = Vector.newBuilder[String]
      if (floored > 0)
        warnings += s"Newton information was <= $InformationFloor for $floored provider-iteration(s) and was floored"
      if (zeroDenominator)
        warnings += "the relative convergence criterion met a zero denominator with a nonzero numerator"
      if (!converged)
        warnings += s"he2013 did not converge within maxIter = ${options.maxIter} (criterion $crit)"
      val (low, high) =
        if (options.boundMode == "absolute") (-options.bound, options.bound)
        else {
          val sorted = gamma.sorted
          val median = if (m % 2 == 1) sorted(m / 2) else 0.5 * (sorted(m / 2 - 1) + sorted(m / 2))
          (median - options.bound, median + options.bound)
        }
      val held = gamma.map(v => v <= low || v >= high)
      val loglik = ThreeStage.marginal(cells3, sigma, gamma, k)._1
      val fit = ThreeStageStage3(
        cells3.providers,
        cells3.clusters,
        gamma,
        sigma,
        converged,
        stalled = false,
        iterations,
        crit,
        loglik,
        mean,
        variance,
        held,
        cells3.cells.length,
        cells3.bins,
        options.stage3
      )
      (fit, warnings.result())
    } finally cells.unpersist()
  }
}
