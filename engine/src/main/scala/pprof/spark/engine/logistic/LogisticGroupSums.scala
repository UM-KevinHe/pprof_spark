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

/** A reduction group's profile pass: pass A's sums as `SchurSum` adds them, and its blocks' intercepts in block order. */
private[logistic] final class ProfileSum(p: Int) {
  private val schur = new SchurSum(p)
  private val blocks = Vector.newBuilder[(Int, Array[Double], Array[Int])]
  private var innerSteps = 0L
  private var capped = 0

  def add(blockId: Int, part: pprof.spark.numerics.kernels.LogisticFE.ProfileBlock): Unit = {
    schur.add(
      LogisticSchurPartial(
        blockId,
        part.schur.packed,
        part.schur.rhs,
        part.schur.scoreTerm,
        part.schur.loglik
      )
    )
    blocks += ((blockId, part.gamma, part.held))
    innerSteps += part.innerSteps
    capped += part.capped
  }

  def partial(group: Int): LogisticProfilePartial = {
    val s = schur.partial(group)
    val parts = blocks.result()
    LogisticProfilePartial(
      group,
      s.packed,
      s.rhs,
      s.scoreTerm,
      s.loglik,
      parts.map(_._1).toArray,
      parts.flatMap(_._2).toArray,
      parts.flatMap(_._3).toArray,
      innerSteps,
      capped
    )
  }
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

/** A group's score-test terms, the centred S and the score, in block order (step2-chunks.md §4). */
private[logistic] final class ScoreSum(p: Int) {
  private val packed = new NeumaierVector(Moments.packedLength(p))
  private val score = new NeumaierVector(p)

  def add(part: LogisticScorePartial): Unit = {
    packed.add(part.packed)
    score.add(part.score)
  }

  def partial(group: Int): LogisticScorePartial =
    LogisticScorePartial(group, packed.values, score.values)
}

/** A group's exact direct sums at every effect, in block order (step2-chunks.md §4). */
private[logistic] final class DirectSum(effects: Int) {
  private val direct = new NeumaierVector(effects)
  private val variance = new NeumaierVector(effects)

  def add(part: LogisticDirectPartial): Unit = {
    direct.add(part.direct)
    variance.add(part.variance)
  }

  def partial(group: Int): LogisticDirectPartial =
    LogisticDirectPartial(group, direct.values, variance.values)
}
