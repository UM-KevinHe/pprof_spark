package pprof.spark.engine.cox

import pprof.spark.numerics.{NeumaierSum, NeumaierVector}
import pprof.spark.numerics.kernels.Moments

/** A reduction group's sums of block partials, in block order, by the driver's own arithmetic
  * (docs/spec/performance/step1-reduction.md §4): a Neumaier sum per quantity, so that a group of one block returns
  * that block's partial exactly.
  */
private[cox] final class EvaluationSum(p: Int) {
  private val value = new NeumaierSum
  private val score = new NeumaierVector(p)
  private val information = new NeumaierVector(Moments.packedLength(p))

  def add(part: CoxPartial): Unit = {
    value.add(part.value)
    score.add(part.score)
    information.add(part.information)
  }

  def partial(group: Int): CoxPartial =
    CoxPartial(group, value.value, score.values, information.values)
}

private[cox] final class SummarySum(p: Int) {
  private var rows = 0L
  private var events = 0L
  private var strataWithoutEvents = 0
  private var fingerprint = 0L
  private val columnSums = new NeumaierVector(p)

  def add(s: CoxBlockSummary): Unit = {
    rows += s.rows
    events += s.events
    strataWithoutEvents += s.strataWithoutEvents
    fingerprint += s.fingerprint
    columnSums.add(s.columnSums)
  }

  def summary(group: Int): CoxBlockSummary =
    CoxBlockSummary(group, rows, events, strataWithoutEvents, columnSums.values, fingerprint)
}

private[cox] final class RobustSum(p: Int) {
  private var units = 0L
  private val outer = new NeumaierVector(Moments.packedLength(p))

  def add(r: CoxRobustPartial): Unit = {
    units += r.units
    outer.add(r.outer)
  }

  def partial(group: Int): CoxRobustPartial = CoxRobustPartial(group, units, outer.values)
}
