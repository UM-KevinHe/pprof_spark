package pprof.spark.numerics

import pprof.spark.numerics.kernels.LogisticFE

/** Options of the SerBIN iteration (Phase 2a specification §5). */
final case class SerbinOptions(
    tol: Double = 1e-8,
    maxIter: Int = 10000,
    bound: Double = 10.0,
    backtrack: Boolean = true
) {
  require(tol > 0.0 && !tol.isInfinite, s"tol must be positive and finite, got $tol")
  require(maxIter >= 0, s"maxIter must be non-negative, got $maxIter")
  require(bound > 0.0 && !bound.isInfinite, s"bound must be positive and finite, got $bound")
}

/** The two passes of one Newton step over all providers, reduced in block order (§6, §11). */
trait SerbinPasses {

  /** S packed, r, Σ gⱼ²/hⱼ and ℓ at (γ, β). */
  def schur(gamma: Array[Double], beta: Array[Double]): LogisticFE.Schur

  /** Δγ for every provider, in provider order, and ℓ at each step length. */
  def trial(
      gamma: Array[Double],
      beta: Array[Double],
      deltaBeta: Array[Double],
      steps: Array[Double]
  ): LogisticFE.Trial
}

/** One Newton step: ℓ at its start, the accepted step length, ‖Δβ‖∞ of the accepted step and the
  * number of step lengths evaluated.
  */
final case class SerbinStep(
    iteration: Int,
    loglik: Double,
    step: Double,
    betaChange: Double,
    evaluated: Int
)

/** The outcome: estimates, steps taken, the final criterion, the last step length and Newton β
  * step, and whether the line search shortened a step that was not yet below tol (pprof_py's
  * second warning).
  */
final case class SerbinResult(
    gamma: Array[Double],
    beta: Array[Double],
    iterations: Int,
    criterion: Double,
    lastStep: Double,
    lastNewtonBetaStep: Double,
    converged: Boolean,
    shortened: Boolean,
    trace: Vector[SerbinStep]
)

/** SerBIN (Wu, Yang, Kang and He, 2022) with pprof_py v0.7.0's step control, bound and stopping rule
  * (Phase 2a specification §5 and §6).
  */
object Serbin {

  val Armijo: Double = 0.01
  val Shrink: Double = 0.6
  val NoiseLevel: Double = 1e-12

  /** Step lengths evaluated per trial pass; the search accepts what the sequential search would. */
  val Ladder: Int = 8

  /** The predicted gain is below the log-likelihood's rounding level (pprof_py C3). */
  def belowNoise(predicted: Double, loglik: Double): Boolean =
    0.0 < predicted && predicted <= NoiseLevel * (1.0 + math.abs(loglik))

  /** numpy's median: the middle value, or the mean of the two middle values. */
  def median(values: Array[Double]): Double = {
    require(values.nonEmpty, "the median needs at least one value")
    val sorted = values.clone()
    java.util.Arrays.sort(sorted)
    val m = sorted.length
    if (m % 2 == 1) sorted(m / 2) else (sorted(m / 2 - 1) + sorted(m / 2)) / 2.0
  }

  /** Clips every value to [med − bound, med + bound], med the median of `values`. */
  def bounded(values: Array[Double], bound: Double): Array[Double] = {
    val med = median(values)
    val lower = med - bound
    val upper = med + bound
    values.map(v => math.min(math.max(v, lower), upper))
  }

  def fit(
      gamma0: Array[Double],
      beta0: Array[Double],
      passes: SerbinPasses,
      options: SerbinOptions,
      aliasTolerance: Double = Cholesky.DefaultTolerance
  ): SerbinResult = {
    val p = beta0.length
    var gamma = gamma0.clone()
    var beta = beta0.clone()
    var iteration = 0
    var criterion = Double.PositiveInfinity
    var lastStep = 1.0
    var lastNewton = 0.0
    val trace = Vector.newBuilder[SerbinStep]
    // pprof_py's loops: with backtracking `iter <= max_iter and crit >= tol`, without it
    // `iter < max_iter and crit > tol`.
    def running: Boolean =
      if (options.backtrack) iteration <= options.maxIter && criterion >= options.tol
      else iteration < options.maxIter && criterion > options.tol
    while (running) {
      iteration += 1
      val schur = passes.schur(gamma, beta)
      val factor = Cholesky.factor(schur.packed, p, aliasTolerance)
      if (!factor.isFullRank) throw new AliasedException(factor.aliased)
      val deltaBeta = factor.solve(schur.rhs)
      var lambda = schur.scoreTerm
      var j = 0
      while (j < p) {
        lambda += schur.rhs(j) * deltaBeta(j)
        j += 1
      }
      var step = 1.0
      var evaluated = 0
      val deltaGamma =
        if (!options.backtrack)
          passes.trial(gamma, beta, deltaBeta, Array.emptyDoubleArray).deltaGamma
        else {
          var accepted = false
          var next = 1.0
          var found: Array[Double] = null
          while (!accepted) {
            val steps = new Array[Double](Ladder)
            var i = 0
            while (i < Ladder) {
              steps(i) = next
              next *= Shrink
              i += 1
            }
            val trial = passes.trial(gamma, beta, deltaBeta, steps)
            if (found == null) found = trial.deltaGamma
            i = 0
            while (!accepted && i < Ladder) {
              val predicted = Armijo * steps(i) * lambda
              val change = trial.loglik(i) - schur.loglik
              evaluated += 1
              if (!(change < predicted && !belowNoise(predicted, schur.loglik))) {
                accepted = true
                step = steps(i)
              }
              i += 1
            }
          }
          found
        }
      gamma =
        bounded(Array.tabulate(gamma.length)(k => gamma(k) + step * deltaGamma(k)), options.bound)
      val updated = Array.tabulate(p)(k => beta(k) + step * deltaBeta(k))
      criterion = 0.0
      j = 0
      while (j < p) {
        criterion = math.max(criterion, math.abs(beta(j) - updated(j)))
        j += 1
      }
      beta = updated
      lastStep = step
      lastNewton = deltaBeta.foldLeft(0.0)((m, v) => math.max(m, math.abs(v)))
      trace += SerbinStep(iteration, schur.loglik, step, criterion, evaluated)
    }
    val converged =
      if (options.backtrack) criterion < options.tol else criterion <= options.tol
    SerbinResult(
      gamma,
      beta,
      iteration,
      criterion,
      lastStep,
      lastNewton,
      converged,
      converged && options.backtrack && lastStep < 1.0 && lastNewton >= options.tol,
      trace.result()
    )
  }
}

/** The passes over blocks held in memory, reduced in the given order: the engine's arithmetic
  * without Spark, for kernel tests and lockstep parity (§9.2).
  */
final class InMemorySerbinPasses(blocks: Seq[LogisticFE.Rows]) extends SerbinPasses {
  private val offsets = blocks.scanLeft(0)(_ + _.providers).toVector

  private def slice(gamma: Array[Double], b: Int): Array[Double] =
    java.util.Arrays.copyOfRange(gamma, offsets(b), offsets(b + 1))

  def schur(gamma: Array[Double], beta: Array[Double]): LogisticFE.Schur = {
    val parts = blocks.indices.map(b => LogisticFE.schur(blocks(b), slice(gamma, b), beta))
    val p = beta.length
    val s = new NeumaierVector(kernels.Moments.packedLength(p))
    val r = new NeumaierVector(p)
    val term = new NeumaierSum
    val loglik = new NeumaierSum
    parts.foreach { part =>
      s.add(part.packed)
      r.add(part.rhs)
      term.add(part.scoreTerm)
      loglik.add(part.loglik)
    }
    LogisticFE.Schur(s.values, r.values, term.value, loglik.value)
  }

  def trial(
      gamma: Array[Double],
      beta: Array[Double],
      deltaBeta: Array[Double],
      steps: Array[Double]
  ): LogisticFE.Trial = {
    val parts =
      blocks.indices.map(b => LogisticFE.trial(blocks(b), slice(gamma, b), beta, deltaBeta, steps))
    val sums = Array.fill(steps.length)(new NeumaierSum)
    parts.foreach(part => part.loglik.indices.foreach(t => sums(t).add(part.loglik(t))))
    LogisticFE.Trial(parts.flatMap(_.deltaGamma).toArray, sums.map(_.value))
  }
}
