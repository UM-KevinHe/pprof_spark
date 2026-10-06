package pprof.spark.numerics

/** One evaluation of a concave objective: its value, gradient and information (the negative
  * Hessian) as a packed upper triangle (§6.8).
  */
final case class Evaluation(value: Double, gradient: Array[Double], information: Array[Double])

/** The state after one Newton iteration, kept for lockstep parity (§9.2) and diagnostics. */
final case class IterationRecord(iteration: Int, beta: Vector[Double], value: Double, halvings: Int)

final case class NewtonOptions(
    maxIterations: Int = 20,
    eps: Double = 1e-9,
    maxHalvings: Int = 20,
    aliasTolerance: Double = Cholesky.DefaultTolerance
) {
  require(maxIterations >= 1, s"maxIterations must be at least 1, got $maxIterations")
  require(eps > 0.0, s"eps must be positive, got $eps")
  require(maxHalvings >= 0, s"maxHalvings must be non-negative, got $maxHalvings")
  require(aliasTolerance >= 0.0, s"aliasTolerance must be non-negative, got $aliasTolerance")
}

final case class NewtonResult(
    beta: Vector[Double],
    evaluation: Evaluation,
    initial: Evaluation,
    iterations: Int,
    halvings: Int,
    converged: Boolean,
    message: String,
    log: Vector[IterationRecord]
)

/** The information matrix is singular in these columns (X-011). */
final class AliasedException(val columns: Vector[Int])
    extends IllegalArgumentException(
      s"the information matrix is singular: columns ${columns.mkString(", ")} are aliased"
    )

/** Newton–Raphson maximization with the Cox specification's step control (§5, X-010).
  *
  * Each iteration solves I·Δ = U by Cholesky. The full step is accepted at once when its value is
  * finite and its relative change, |new − old| / |old| (|old| = 0 counts as 1), is below `eps`:
  * convergence is tested before any halving, as R does. Otherwise the step is halved from the
  * current point while the value is not finite or lower than the current one, at most
  * `maxHalvings` times, and convergence is tested on the accepted point, as pprof_py does.
  */
object Newton {

  def maximize(
      evaluate: Array[Double] => Evaluation,
      start: Array[Double],
      options: NewtonOptions
  ): NewtonResult = {
    val p = start.length
    require(p >= 1, "at least one parameter is required")
    var beta = start.clone()
    var current = evaluate(beta.clone())
    val initial = current
    if (!java.lang.Double.isFinite(current.value))
      throw new ArithmeticException("the objective is not finite at the starting values")
    var iteration = 0
    var totalHalvings = 0
    var converged = false
    val log = Vector.newBuilder[IterationRecord]
    while (!converged && iteration < options.maxIterations) {
      iteration += 1
      val factor = Cholesky.factor(current.information, p, options.aliasTolerance)
      if (!factor.isFullRank) throw new AliasedException(factor.aliased)
      var step = factor.solve(current.gradient)
      var candidate = plus(beta, step)
      var next = evaluate(candidate.clone())
      var halvings = 0
      if (finite(next) && relativeChange(next.value, current.value) < options.eps) converged = true
      else {
        while ((!finite(next) || next.value < current.value) && halvings < options.maxHalvings) {
          step = step.map(_ / 2.0)
          candidate = plus(beta, step)
          next = evaluate(candidate.clone())
          halvings += 1
        }
        if (!finite(next))
          throw new ArithmeticException(
            s"the objective is not finite after $halvings step halvings in iteration $iteration"
          )
        converged = relativeChange(next.value, current.value) < options.eps
      }
      beta = candidate
      current = next
      totalHalvings += halvings
      log += IterationRecord(iteration, beta.toVector, current.value, halvings)
    }
    val message =
      if (converged) "converged"
      else
        s"reached ${options.maxIterations} iterations without a relative change in the " +
          s"log-likelihood below eps = ${options.eps}"
    NewtonResult(
      beta.toVector,
      current,
      initial,
      iteration,
      totalHalvings,
      converged,
      message,
      log.result()
    )
  }

  /** |next − current| / |current|, with |current| = 0 counting as 1 (pprof_py's formula). */
  def relativeChange(next: Double, current: Double): Double = {
    val scale = if (current != 0.0) math.abs(current) else 1.0
    math.abs(next - current) / scale
  }

  private def finite(e: Evaluation): Boolean = java.lang.Double.isFinite(e.value)

  private def plus(a: Array[Double], b: Array[Double]): Array[Double] =
    Array.tabulate(a.length)(i => a(i) + b(i))
}
