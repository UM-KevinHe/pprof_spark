package pprof.spark.engine.cox

import pprof.spark.numerics.{NeumaierSum, NeumaierVector}
import pprof.spark.numerics.kernels.{
  CoxMeasures => MeasureKernels,
  CoxResiduals,
  CoxStratum,
  CoxTotals,
  Fingerprint,
  Moments
}

/** One block's log partial likelihood, score and packed information at one β. */
final case class CoxPartial(
    blockId: Int,
    value: Double,
    score: Array[Double],
    information: Array[Double]
)

/** One row of the baseline hazard table, keyed by the stratum's group index (Phase 1b §2). */
final case class CoxBaselineRow(
    groupIndex: Int,
    time: Double,
    increment: Double,
    cumulativeHazard: Double,
    survival: Double
)

/** One row's residuals (Phase 1c specification §1): martingale, score and dfbeta. */
final case class CoxResidualRow(
    rowId: Long,
    martingale: Double,
    score: Array[Double],
    dfbeta: Array[Double]
)

/** One row's weighted score residual wᵢUᵢ with its cluster and canonical position. */
final case class CoxScoreRow(cluster: String, blockId: Int, position: Int, wu: Array[Double])

/** One cluster's sum s_c = Σ wᵢUᵢ and its reduction bucket (Phase 1c specification §4). */
final case class CoxClusterSum(bucket: Int, cluster: String, sum: Array[Double])

/** A partial Σ s sᵀ (packed) over one block's rows or one bucket's clusters, with its count. */
final case class CoxRobustPartial(key: Int, units: Long, outer: Array[Double])

/** Per-time totals for the national baseline: Σ w by exit and by entry time, events at exit. */
final case class CoxTimeTotal(
    time: Double,
    blockId: Int,
    exitSum: Double,
    entrySum: Double,
    events: Long
)

/** One provider's observed and expected events, person-time and direct expectation (Phase 1d §1). */
final case class CoxProviderRow(
    groupIndex: Int,
    observed: Long,
    expected: Double,
    personTime: Double,
    directExpected: Double,
    indirectRatio: Double
)

/** One block's counts, column sums (for centering), and fingerprint (§6.10). */
final case class CoxBlockSummary(
    blockId: Int,
    rows: Long,
    events: Long,
    strataWithoutEvents: Int,
    columnSums: Array[Double],
    fingerprint: Long
)

/** Executor-side work on Cox blocks: pure functions of a block and small arguments. */
object CoxKernel {

  def summary(block: CoxBlock): CoxBlockSummary = {
    val p = block.p
    val values = new Array[Double](p + 6)
    var events = 0L
    var withoutEvents = 0
    var fingerprint = 0L
    var g = 0
    while (g < block.groupCount) {
      var any = false
      var r = block.groupStart(g)
      while (r < block.groupEnd(g)) {
        if (block.event(r)) {
          events += 1
          if (block.weight(r) > 0.0) any = true
        }
        values(0) = block.time(r)
        values(1) = if (block.event(r)) 1.0 else 0.0
        values(2) = block.weight(r)
        values(3) = block.offset(r)
        values(4) = block.entry(r)
        values(5) =
          if (block.cluster(r) == null) 0.0
          else scala.util.hashing.MurmurHash3.stringHash(block.cluster(r)).toDouble
        System.arraycopy(block.x, r * p, values, 6, p)
        fingerprint += Fingerprint.row(block.groupIndex(g), block.rowId(r), values, 0, p + 6)
        r += 1
      }
      if (!any) withoutEvents += 1
      g += 1
    }
    CoxBlockSummary(
      block.blockId,
      block.rowCount.toLong,
      events,
      withoutEvents,
      Moments.columnSums(block.x, p, 0, block.rowCount),
      fingerprint
    )
  }

  /** The contributions of the block's strata, added in stratum order (Cox specification §6). */
  def partial(
      block: CoxBlock,
      beta: Array[Double],
      center: Array[Double],
      efron: Boolean
  ): CoxPartial = {
    val totals = new CoxTotals(block.p)
    var g = 0
    while (g < block.groupCount) {
      CoxStratum.add(
        block.time,
        block.entry,
        block.entryOrder,
        block.event,
        block.weight,
        block.offset,
        block.x,
        block.p,
        block.groupStart(g),
        block.groupEnd(g),
        beta,
        center,
        efron,
        totals
      )
      g += 1
    }
    CoxPartial(block.blockId, totals.value.value, totals.score.values, totals.information.values)
  }

  /** The baseline hazard rows of the block's strata at `beta`: increments, their running Neumaier
    * total in ascending time, and exp(−total) (Phase 1b specification §2 and §6).
    */
  def baseline(block: CoxBlock, beta: Array[Double], efron: Boolean): Seq[CoxBaselineRow] = {
    val rows = Vector.newBuilder[CoxBaselineRow]
    var g = 0
    while (g < block.groupCount) {
      val (times, increments) = CoxStratum.baseline(
        block.time,
        block.entry,
        block.entryOrder,
        block.event,
        block.weight,
        block.offset,
        block.x,
        block.p,
        block.groupStart(g),
        block.groupEnd(g),
        beta,
        efron
      )
      val total = new NeumaierSum
      var k = 0
      while (k < times.length) {
        total.add(increments(k))
        val cumulative = total.value
        rows += CoxBaselineRow(
          block.groupIndex(g),
          times(k),
          increments(k),
          cumulative,
          StrictMath.exp(-cumulative)
        )
        k += 1
      }
      g += 1
    }
    rows.result()
  }

  /** The residuals of the block's rows at `beta`; dfbeta is wᵢ·Uᵢ·V with V the packed model-based
    * covariance (Phase 1c specification §1).
    */
  def residuals(
      block: CoxBlock,
      beta: Array[Double],
      center: Array[Double],
      efron: Boolean,
      covariance: Array[Double]
  ): Seq[CoxResidualRow] = {
    val p = block.p
    val rows = Vector.newBuilder[CoxResidualRow]
    var g = 0
    while (g < block.groupCount) {
      val from = block.groupStart(g)
      val until = block.groupEnd(g)
      val (martingale, score) = CoxResiduals.stratum(
        block.time,
        block.entry,
        block.entryOrder,
        block.event,
        block.weight,
        block.offset,
        block.x,
        p,
        from,
        until,
        beta,
        center,
        efron
      )
      var i = 0
      while (i < until - from) {
        val u = java.util.Arrays.copyOfRange(score, i * p, (i + 1) * p)
        val w = block.weight(from + i)
        val dfbeta = Array.tabulate(p) { j =>
          var s = 0.0
          var k = 0
          while (k < p) {
            s += w * u(k) * covariance(Moments.packedIndex(math.min(k, j), math.max(k, j), p))
            k += 1
          }
          s
        }
        rows += CoxResidualRow(block.rowId(from + i), martingale(i), u, dfbeta)
        i += 1
      }
      g += 1
    }
    rows.result()
  }

  /** Clusters fall into this many buckets by a fixed hash of their key (Phase 1c §4). */
  val Buckets: Int = 256

  private def residualsOf(
      block: CoxBlock,
      beta: Array[Double],
      center: Array[Double],
      efron: Boolean
  ) =
    (0 until block.groupCount).map { g =>
      val (_, score) = CoxResiduals.stratum(
        block.time,
        block.entry,
        block.entryOrder,
        block.event,
        block.weight,
        block.offset,
        block.x,
        block.p,
        block.groupStart(g),
        block.groupEnd(g),
        beta,
        center,
        efron
      )
      block.groupStart(g) -> score
    }

  /** Each row's wᵢUᵢ with its cluster key, for clustered robust variance. */
  def weightedScores(
      block: CoxBlock,
      beta: Array[Double],
      center: Array[Double],
      efron: Boolean
  ): Seq[CoxScoreRow] = {
    val p = block.p
    residualsOf(block, beta, center, efron).flatMap { case (from, score) =>
      (0 until score.length / p).map { i =>
        val r = from + i
        val w = block.weight(r)
        CoxScoreRow(
          block.cluster(r),
          block.blockId,
          r,
          Array.tabulate(p)(j => w * score(i * p + j))
        )
      }
    }
  }

  /** Σ (wᵢUᵢ)(wᵢUᵢ)ᵀ over the block's rows in canonical order: per-row robust variance. */
  def robustPerRow(
      block: CoxBlock,
      beta: Array[Double],
      center: Array[Double],
      efron: Boolean
  ): CoxRobustPartial = {
    val p = block.p
    val outer = new NeumaierVector(Moments.packedLength(p))
    residualsOf(block, beta, center, efron).foreach { case (from, score) =>
      (0 until score.length / p).foreach { i =>
        val w = block.weight(from + i)
        var k = 0
        var a = 0
        while (a < p) {
          var b = a
          while (b < p) {
            outer.addAt(k, (w * score(i * p + a)) * (w * score(i * p + b)))
            k += 1
            b += 1
          }
          a += 1
        }
      }
    }
    CoxRobustPartial(block.blockId, block.rowCount.toLong, outer.values)
  }

  /** A cluster's s_c: its rows' wᵢUᵢ summed in (block, position) order. */
  def clusterSum(cluster: String, rows: Iterator[CoxScoreRow], p: Int): CoxClusterSum = {
    val ordered = rows.toArray.sortBy(r => (r.blockId, r.position))
    val total = new NeumaierVector(p)
    ordered.foreach(r => total.add(r.wu))
    val bucket =
      java.lang.Math.floorMod(scala.util.hashing.MurmurHash3.stringHash(cluster), Buckets)
    CoxClusterSum(bucket, cluster, total.values)
  }

  /** Σ s_c s_cᵀ over a bucket's clusters in key order. */
  def bucketOuter(bucket: Int, sums: Iterator[CoxClusterSum], p: Int): CoxRobustPartial = {
    val ordered = sums.toArray.sortBy(_.cluster)
    val outer = new NeumaierVector(Moments.packedLength(p))
    ordered.foreach { c =>
      var k = 0
      var a = 0
      while (a < p) {
        var b = a
        while (b < p) {
          outer.addAt(k, c.sum(a) * c.sum(b))
          k += 1
          b += 1
        }
        a += 1
      }
    }
    CoxRobustPartial(bucket, ordered.length.toLong, outer.values)
  }

  /** The largest uncentered η = xᵀβ + o of the block's rows. */
  def maxEta(block: CoxBlock, beta: Array[Double]): Double = {
    var m = Double.NegativeInfinity
    var r = 0
    while (r < block.rowCount) {
      m = math.max(m, MeasureKernels.eta(block.x, block.p, r, beta, block.offset))
      r += 1
    }
    m
  }

  /** The block's per-time totals for the national baseline (Phase 1d §4). */
  def timeTotals(block: CoxBlock, beta: Array[Double], maxEta: Double): Seq[CoxTimeTotal] = {
    val (times, exits, entries, events) = MeasureKernels.timeTotals(
      block.time,
      block.entry,
      block.event,
      block.offset,
      block.x,
      block.p,
      0,
      block.rowCount,
      beta,
      maxEta
    )
    times.indices.map(i => CoxTimeTotal(times(i), block.blockId, exits(i), entries(i), events(i)))
  }

  /** One time's totals over all blocks, added in block order. */
  def combineTimeTotals(time: Double, parts: Iterator[CoxTimeTotal]): CoxTimeTotal = {
    val ordered = parts.toArray.sortBy(_.blockId)
    val exits = new NeumaierSum
    val entries = new NeumaierSum
    var events = 0L
    ordered.foreach { t =>
      exits.add(t.exitSum)
      entries.add(t.entrySum)
      events += t.events
    }
    CoxTimeTotal(time, -1, exits.value, entries.value, events)
  }

  /** Each provider's measures, for blocks laid out by provider (Phase 1d §1 and §4). */
  def providerMeasures(
      block: CoxBlock,
      beta: Array[Double],
      maxEta: Double,
      eventTimes: Array[Double],
      cumulative: Array[Double],
      riskSets: Array[Double]
  ): Seq[CoxProviderRow] =
    (0 until block.groupCount).map { g =>
      val (observed, expected, personTime, direct) = MeasureKernels.provider(
        block.time,
        block.entry,
        block.entryOrder,
        block.event,
        block.offset,
        block.x,
        block.p,
        block.groupStart(g),
        block.groupEnd(g),
        beta,
        maxEta,
        eventTimes,
        cumulative,
        riskSets
      )
      CoxProviderRow(
        block.groupIndex(g),
        observed,
        expected,
        personTime,
        direct,
        observed / expected
      )
    }
}
