package pprof.spark.bench.synthetic

/** A model's generating process (scale-test plan §4). The parameters are recorded in every manifest. */
sealed abstract class Model extends Product with Serializable {
  def name: String
  def parameters: Seq[(String, Any)]
}

object Model {

  /** Cox: a Weibull baseline of shape `shape`, with each provider's stratum scale λⱼ = `scale`·exp(`spread`·Zⱼ).
    * The event time is T = (−log U / (λⱼ·e^η))^(1/shape) years, censoring is uniform on (0, `horizon`) and
    * administrative at `horizon`, and time = ⌈365·min(T, C, horizon)⌉ days, an event when T ≤ min(C, horizon).
    */
  final case class Cox(
      scale: Double = 0.1,
      shape: Double = 1.2,
      spread: Double = 0.3,
      horizon: Double = 5.0
  ) extends Model {
    require(
      scale > 0.0 && shape > 0.0 && spread >= 0.0 && horizon > 0.0,
      s"invalid Cox parameters: $this"
    )
    def name: String = "cox"
    def parameters: Seq[(String, Any)] =
      Seq("scale" -> scale, "shape" -> shape, "spread" -> spread, "horizon_years" -> horizon)
  }

  /** Logistic fixed effects: γⱼ = `gammaMean` + `gammaSd`·Zⱼ. A row has one trial, or, when `maxTrials` exceeds 1,
    * a number of trials uniform on 1 to `maxTrials`; each trial succeeds with probability σ(γⱼ + xᵀβ).
    */
  final case class Logistic(gammaMean: Double = -1.0, gammaSd: Double = 0.5, maxTrials: Int = 1)
      extends Model {
    require(gammaSd >= 0.0 && maxTrials >= 1, s"invalid logistic parameters: $this")
    def name: String = "logistic"
    def parameters: Seq[(String, Any)] =
      Seq("gamma_mean" -> gammaMean, "gamma_sd" -> gammaSd, "max_trials" -> maxTrials)
  }

  /** Linear fixed effects: y = γⱼ + xᵀβ + `sigma`·ε, with γⱼ = `gammaMean` + `gammaSd`·Zⱼ and ε standard normal. */
  final case class Linear(gammaMean: Double = 0.0, gammaSd: Double = 0.5, sigma: Double = 1.0)
      extends Model {
    require(gammaSd >= 0.0 && sigma > 0.0, s"invalid linear parameters: $this")
    def name: String = "linear"
    def parameters: Seq[(String, Any)] =
      Seq("gamma_mean" -> gammaMean, "gamma_sd" -> gammaSd, "sigma" -> sigma)
  }
}

/** A synthetic data set (scale-test plan §4): n rows among m providers, with p features. Odd-numbered features
  * are binary with P(1) = 0.3, even-numbered ones standard normal on a 1/64 grid, and the true coefficients are
  * βₖ = (−1)ᵏ·0.5/√p, so the variance of η stays bounded as p grows.
  */
final case class Design(
    model: Model,
    n: Long,
    m: Int,
    p: Int,
    sizes: Sizes = Sizes.Equal,
    seed: Long = 79L
) {
  require(p >= 1, s"at least one feature is required, got $p")
  require(n >= m.toLong && m >= 1, s"$n rows cannot give each of $m providers a row")

  def features: Seq[String] = (1 to p).map(k => s"x$k")

  def beta: Vector[Double] =
    (1 to p).map(k => (if (k % 2 == 0) 0.5 else -0.5) / StrictMath.sqrt(p.toDouble)).toVector
}
