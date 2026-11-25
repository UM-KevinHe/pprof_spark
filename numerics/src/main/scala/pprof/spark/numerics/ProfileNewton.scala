package pprof.spark.numerics

import pprof.spark.numerics.kernels.LogisticFE

/** One profile-likelihood pass over all providers (docs/spec/logistic/profile-newton.md §3, D-83): pass A's sums at the
  * solved intercepts, reduced in block order; the intercepts and which of them the bound held (−1, 0, +1), in
  * provider order; the inner steps and capped solves.
  */
final case class ProfilePass(
    schur: LogisticFE.Schur,
    gamma: Array[Double],
    held: Array[Int],
    innerSteps: Long,
    capped: Int
)

/** The pass at β: each provider's intercept solved within [median − bound, median + bound] from `gamma`. */
trait ProfilePasses {
  def solve(gamma: Array[Double], beta: Array[Double], median: Double, bound: Double): ProfilePass
}

/** SerBIN's result type, with the profile method's inner Newton steps and capped solves over the whole fit (§7). */
final case class ProfileResult(result: SerbinResult, innerSteps: Long, capped: Int)

/** Profile-likelihood Newton through SerBIN's reduced system (§3): Δβ = S⁻¹r from the accepted pass, trials at
  * β + vΔβ with SerBIN's Armijo test on ℓₚ (λ = rᵀΔβ) and its stopping rule; the median of the accepted pass's
  * intercepts bounds the next passes.
  */
object ProfileNewton {

  /** The fit's intercepts: the last pass's, with those the bound held set to the median of the last pass's intercepts
    * ∓ the bound, as SerBIN's clip leaves them (§12, round 199). Held intercepts lie at the extremes, so the median is
    * the same before and after.
    */
  def settle(pass: ProfilePass, bound: Double): Array[Double] = {
    val median = Serbin.median(pass.gamma)
    Array.tabulate(pass.gamma.length) { k =>
      if (pass.held(k) < 0) median - bound
      else if (pass.held(k) > 0) median + bound
      else pass.gamma(k)
    }
  }

  /** Shrinks of one trial before its last is accepted: SerBIN's ladder ends because λ > 0, which a zero r would not
    * give (§3, round 199).
    */
  val MaxShrinks: Int = 60

  def fit(
      gamma0: Array[Double],
      beta0: Array[Double],
      passes: ProfilePasses,
      options: SerbinOptions,
      aliasTolerance: Double = Cholesky.DefaultTolerance
  ): ProfileResult = {
    val p = beta0.length
    var beta = beta0.clone()
    var current = passes.solve(gamma0, beta, Serbin.median(gamma0), options.bound)
    var innerSteps = current.innerSteps
    var capped = current.capped
    var iteration = 0
    var criterion = Double.PositiveInfinity
    var lastStep = 1.0
    var lastNewton = 0.0
    val trace = Vector.newBuilder[SerbinStep]
    def running: Boolean =
      if (options.backtrack) iteration <= options.maxIter && criterion >= options.tol
      else iteration < options.maxIter && criterion > options.tol
    while (running) {
      iteration += 1
      val schur = current.schur
      val factor = Cholesky.factor(schur.packed, p, aliasTolerance)
      if (!factor.isFullRank) throw new AliasedException(factor.aliased)
      val deltaBeta = factor.solve(schur.rhs)
      var lambda = 0.0
      var j = 0
      while (j < p) {
        lambda += schur.rhs(j) * deltaBeta(j)
        j += 1
      }
      val median = Serbin.median(current.gamma)
      var step = 1.0
      var shrinks = 0
      var evaluated = 0
      var accepted: ProfilePass = null
      while (accepted == null) {
        val trial = Array.tabulate(p)(k => beta(k) + step * deltaBeta(k))
        val pass = passes.solve(current.gamma, trial, median, options.bound)
        evaluated += 1
        innerSteps += pass.innerSteps
        capped += pass.capped
        val predicted = Serbin.Armijo * step * lambda
        val change = pass.schur.loglik - schur.loglik
        if (
          !options.backtrack || shrinks >= MaxShrinks ||
          !(change < predicted && !Serbin.belowNoise(predicted, schur.loglik))
        ) accepted = pass
        else {
          step *= Serbin.Shrink
          shrinks += 1
        }
      }
      val updated = Array.tabulate(p)(k => beta(k) + step * deltaBeta(k))
      criterion = 0.0
      j = 0
      while (j < p) {
        criterion = math.max(criterion, math.abs(beta(j) - updated(j)))
        j += 1
      }
      beta = updated
      current = accepted
      lastStep = step
      lastNewton = deltaBeta.foldLeft(0.0)((m, v) => math.max(m, math.abs(v)))
      trace += SerbinStep(iteration, schur.loglik, step, criterion, evaluated)
    }
    val converged =
      if (options.backtrack) criterion < options.tol else criterion <= options.tol
    ProfileResult(
      SerbinResult(
        settle(current, options.bound),
        beta,
        iteration,
        criterion,
        lastStep,
        lastNewton,
        converged,
        converged && options.backtrack && lastStep < 1.0 && lastNewton >= options.tol,
        trace.result()
      ),
      innerSteps,
      capped
    )
  }
}

/** The profile pass over blocks held in memory, reduced in the given order: the engine's arithmetic without Spark,
  * for kernel tests.
  */
final class InMemoryProfilePasses(blocks: Seq[LogisticFE.RowsView]) extends ProfilePasses {
  private val offsets = blocks.scanLeft(0)(_ + _.providers).toVector

  /** Passes made, for tests that count them. */
  var count: Int = 0

  def solve(
      gamma: Array[Double],
      beta: Array[Double],
      median: Double,
      bound: Double
  ): ProfilePass = {
    count += 1
    val parts = blocks.indices.map { b =>
      LogisticFE.profile(
        blocks(b),
        java.util.Arrays.copyOfRange(gamma, offsets(b), offsets(b + 1)),
        beta,
        median,
        bound
      )
    }
    val p = beta.length
    val s = new NeumaierVector(kernels.Moments.packedLength(p))
    val r = new NeumaierVector(p)
    val term = new NeumaierSum
    val loglik = new NeumaierSum
    parts.foreach { part =>
      s.add(part.schur.packed)
      r.add(part.schur.rhs)
      term.add(part.schur.scoreTerm)
      loglik.add(part.schur.loglik)
    }
    ProfilePass(
      LogisticFE.Schur(s.values, r.values, term.value, loglik.value),
      parts.flatMap(_.gamma).toArray,
      parts.flatMap(_.held).toArray,
      parts.map(_.innerSteps).sum,
      parts.map(_.capped).sum
    )
  }
}
