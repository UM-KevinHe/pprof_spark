package pprof.spark.engine.logistic

import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.layout.GroupKey
import pprof.spark.engine.metadata.SoftwareInfo
import pprof.spark.engine.skeleton.LayoutSummary
import pprof.spark.numerics.{Cholesky, SerbinOptions, SerbinStep}

/** Column roles of the logistic fixed-effect model (Phase 2a specification §4): an outcome of
  * events, optional binomial trials (1 per row without them), the features and the provider.
  */
final case class LogisticSpec(
    outcome: String,
    features: Seq[String],
    provider: String,
    trials: Option[String] = None,
    rowId: Option[String] = None
)

/** Options of the fit (specification §5); the defaults are pprof_py v0.7.0's. */
final case class LogisticOptions(
    tol: Double = 1e-8,
    maxIter: Int = 10000,
    bound: Double = 10.0,
    backtrack: Boolean = true,
    screen: Boolean = true,
    minRecords: Long = 10L,
    confidenceLevel: Double = 0.95,
    maxProvidersOnDriver: Int = LogisticOptions.DefaultMaxProvidersOnDriver,
    aliasTolerance: Double = Cholesky.DefaultTolerance,
    blocks: BlockOptions = BlockOptions()
) {
  require(minRecords >= 0L, s"minRecords must be non-negative, got $minRecords")
  require(
    confidenceLevel > 0.0 && confidenceLevel < 1.0,
    s"confidenceLevel must lie strictly between 0 and 1, got $confidenceLevel"
  )
  require(maxProvidersOnDriver > 0, "maxProvidersOnDriver must be positive")

  def serbin: SerbinOptions = SerbinOptions(tol, maxIter, bound, backtrack)
}

object LogisticOptions {

  /** γ, its variances and the provider counts are held on the driver up to this many providers
    * (specification §11; OI-50).
    */
  val DefaultMaxProvidersOnDriver: Int = 10000000
}

/** One covariate's Wald summary. */
final case class LogisticCoefficient(
    feature: String,
    estimate: Double,
    standardError: Double,
    z: Double,
    pValue: Double,
    lower: Double,
    upper: Double
)

/** The fitted providers, in key order: effects, their two variances and counts (specification §8).
  * m-scale, held on the driver under `LogisticOptions.maxProvidersOnDriver`.
  */
final class LogisticProviders(
    val keys: Array[GroupKey],
    val gamma: Array[Double],
    val varGamma: Array[Double],
    val varCaseMix: Array[Double],
    val records: Array[Long],
    val events: Array[Double],
    val trials: Array[Double],
    val atBound: Array[Boolean]
) extends Serializable {
  def size: Int = keys.length
  def zeroEvents(k: Int): Boolean = events(k) <= 0.0
  def allEvents(k: Int): Boolean = trials(k) > 0.0 && events(k) >= trials(k)
}

/** A fitted logistic fixed-effect model (specification §8). Holds no SparkSession and no reference
  * to the training data (ARCH-4).
  */
final case class LogisticFit(
    coefficients: Vector[LogisticCoefficient],
    covariance: Vector[Double],
    providers: LogisticProviders,
    excluded: Vector[(GroupKey, Long)],
    loglik: Double,
    aic: Double,
    bic: Double,
    iterations: Int,
    converged: Boolean,
    criterion: Double,
    lastStep: Double,
    rows: Long,
    trials: Double,
    events: Double,
    warnings: Vector[String],
    trace: Vector[SerbinStep],
    spec: LogisticSpec,
    options: LogisticOptions,
    software: SoftwareInfo,
    layout: LayoutSummary,
    fingerprint: Long,
    status: String = "experimental"
) {
  def estimates: Vector[Double] = coefficients.map(_.estimate)
  def standardErrors: Vector[Double] = coefficients.map(_.standardError)
  def degenerateProviders: Int =
    providers.keys.indices.count(k => providers.zeroEvents(k) || providers.allEvents(k))
}

/** Features aliased in the Schur complement: collinear, or constant within every provider (X-019). */
final class LogisticAliasingException(val features: Seq[String])
    extends IllegalArgumentException(
      s"the features ${features.mkString(", ")} are aliased: collinear with other features or " +
        "constant within every provider, so the provider effects absorb them (X-019)"
    )
