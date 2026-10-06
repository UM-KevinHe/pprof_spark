package pprof.spark.engine.cox

import pprof.spark.numerics.kernels.{CoxStratum, CoxTotals, Fingerprint, Moments}

/** One block's log partial likelihood, score and packed information at one β. */
final case class CoxPartial(
    blockId: Int,
    value: Double,
    score: Array[Double],
    information: Array[Double]
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
    val values = new Array[Double](p + 5)
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
        System.arraycopy(block.x, r * p, values, 5, p)
        fingerprint += Fingerprint.row(block.groupIndex(g), block.rowId(r), values, 0, p + 5)
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
}
