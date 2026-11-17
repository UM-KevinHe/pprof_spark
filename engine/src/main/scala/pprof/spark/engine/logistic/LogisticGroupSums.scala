package pprof.spark.engine.logistic

import scala.collection.mutable.ArrayBuffer

import pprof.spark.numerics.{NeumaierSum, NeumaierVector}
import pprof.spark.numerics.kernels.Moments

/** A reduction group's sums of block partials, in block order, by the driver's own arithmetic
  * (docs/spec/performance/step1-reduction.md §4), so that a group of one block returns that block's partial exactly.
  */
private[logistic] final class SchurSum(p: Int) {
  private val packed = new NeumaierVector(Moments.packedLength(p))
  private val rhs = new NeumaierVector(p)
  private val term = new NeumaierSum
  private val loglik = new NeumaierSum

  def add(part: LogisticSchurPartial): Unit = {
    packed.add(part.packed)
    rhs.add(part.rhs)
    term.add(part.scoreTerm)
    loglik.add(part.loglik)
  }

  def partial(group: Int): LogisticSchurPartial =
    LogisticSchurPartial(group, packed.values, rhs.values, term.value, loglik.value)
}

private[logistic] final class VarianceSum(p: Int) {
  private val packed = new NeumaierVector(Moments.packedLength(p))
  private val loglik = new NeumaierSum

  def add(part: LogisticVariancePartial): Unit = {
    packed.add(part.packed)
    loglik.add(part.loglik)
  }

  def partial(group: Int): LogisticVariancePartial =
    LogisticVariancePartial(group, packed.values, loglik.value)
}

private[logistic] final class MeatSum(p: Int) {
  private val packed = new NeumaierVector(Moments.packedLength(p))
  private var clusters = 0L

  def add(part: LogisticMeatPartial): Unit = {
    packed.add(part.packed)
    clusters += part.clusters
  }

  def partial(group: Int): LogisticMeatPartial = LogisticMeatPartial(group, packed.values, clusters)
}

/** A group's summary: counts and plain sums as the driver adds them, weighted features by Neumaier sums, co-moments
  * merged from the group's first block on (a group of one block keeps that block's moments untouched), and the
  * per-provider values concatenated in block order.
  */
private[logistic] final class SummaryGroup(p: Int) {
  private var rows = 0L
  private var events = 0.0
  private var trials = 0.0
  private val weightedX = new NeumaierVector(p)
  private var moments: Option[Moments.Centred] = None
  private var fingerprint = 0L
  private val records = ArrayBuffer.empty[Long]
  private val providerEvents = ArrayBuffer.empty[Double]
  private val providerTrials = ArrayBuffer.empty[Double]
  private var maxOutcome = Double.NegativeInfinity
  private var zeroOutcomes = 0L

  def add(s: LogisticSummary): Unit = {
    rows += s.rows
    events += s.events
    trials += s.trials
    weightedX.add(s.weightedX)
    val centred = Moments.Centred(s.rows, s.featureMean, s.featureComoments)
    moments = Some(moments.fold(centred)(m => Moments.merge(m, centred)))
    fingerprint += s.fingerprint
    records ++= s.records
    providerEvents ++= s.providerEvents
    providerTrials ++= s.providerTrials
    maxOutcome = math.max(maxOutcome, s.maxOutcome)
    zeroOutcomes += s.zeroOutcomes
  }

  def summary(group: Int): LogisticSummary = {
    val m = moments.getOrElse(throw new IllegalStateException(s"group $group has no blocks"))
    LogisticSummary(
      group,
      rows,
      events,
      trials,
      weightedX.values,
      m.mean,
      m.comoments,
      fingerprint,
      records.toArray,
      providerEvents.toArray,
      providerTrials.toArray,
      maxOutcome,
      zeroOutcomes
    )
  }
}
