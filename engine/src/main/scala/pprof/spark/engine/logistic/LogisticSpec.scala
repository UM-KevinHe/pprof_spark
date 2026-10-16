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
    rowId: Option[String] = None,
    cluster: Option[String] = None
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
    correlationThreshold: Double = 0.9,
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
  require(
    correlationThreshold >= 0.0 && correlationThreshold <= 1.0,
    s"correlationThreshold must lie in [0, 1], got $correlationThreshold"
  )

  def serbin: SerbinOptions = SerbinOptions(tol, maxIter, bound, backtrack)
}

object LogisticOptions {

  /** γ, its variances and the provider counts are held on the driver up to this many providers
    * (specification §11; OI-50).
    */
  val DefaultMaxProvidersOnDriver: Int = 10000000
}

/** One covariate's test (Phase 2b §6): `method` is `wald`, `wald-robust`, `lr` or `score`; the interval is
  * Wald-based, as pprof_py's.
  */
final case class LogisticTest(
    feature: String,
    estimate: Double,
    standardError: Double,
    statistic: Double,
    pValue: Double,
    lower: Double,
    upper: Double,
    method: String
)

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
    val atBound: Array[Boolean],
    val robustVarCaseMix: Array[Double] = Array.emptyDoubleArray,
    val robustVarFixedBeta: Array[Double] = Array.emptyDoubleArray
) extends Serializable {
  def size: Int = keys.length
  def zeroEvents(k: Int): Boolean = events(k) <= 0.0
  def allEvents(k: Int): Boolean = trials(k) > 0.0 && events(k) >= trials(k)
}

/** Providers added to a fit after fitting (pprof_py's `add_providers`; specification §8a): each with a
  * fixed effect, its standard error and, for providers not in the fit, a group size.
  */
final class LogisticAddedProviders(
    val keys: Array[GroupKey],
    val gamma: Array[Double],
    val seGamma: Array[Double],
    val sizes: Array[Long]
) extends Serializable {
  def size: Int = keys.length
}

object LogisticAddedProviders {
  val empty: LogisticAddedProviders =
    new LogisticAddedProviders(
      Array.empty,
      Array.emptyDoubleArray,
      Array.emptyDoubleArray,
      Array.emptyLongArray
    )
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
    status: String = "experimental",
    robustCovariance: Option[Vector[Double]] = None,
    clusters: Long = 0L,
    auc: Option[Double] = None,
    added: LogisticAddedProviders = LogisticAddedProviders.empty
) {

  /** This fit with providers added after fitting (pprof_py's `add_providers`; specification §8a,
    * production finding L1). Each given provider takes the effect `gamma` and, for every variance of
    * its effect, `seGamma` squared; one not in the fit is added with `groupSizes` records, which weight
    * the mean reference, while a fitted one keeps its records and its data-based statistics. A later
    * call replaces earlier values. The estimation (β, the likelihood, the AUC) never involves them.
    */
  def withProviders(
      keys: Seq[GroupKey],
      gamma: Seq[Double],
      seGamma: Seq[Double],
      groupSizes: Seq[Long]
  ): LogisticFit = {
    val n = keys.length
    require(
      gamma.length == n && seGamma.length == n && groupSizes.length == n,
      "keys, gamma, seGamma and groupSizes must have the same length"
    )
    require(keys.distinct.length == n, "a provider may be added once per call")
    require(gamma.forall(java.lang.Double.isFinite), "gamma must be finite")
    require(
      seGamma.forall(s => java.lang.Double.isFinite(s) && s > 0.0),
      "seGamma must be positive and finite"
    )
    require(groupSizes.forall(_ >= 0L), "groupSizes must not be negative")
    val text = providers.keys.headOption.exists(_.isInstanceOf[GroupKey.Text])
    require(
      keys.forall(_.isInstanceOf[GroupKey.Text] == text),
      s"the added providers' keys must be ${if (text) "text" else "integral"}, as the fit's"
    )
    val previous = added.keys.zipWithIndex.toMap
    val k = added.keys.toBuffer
    val g = added.gamma.toBuffer
    val s = added.seGamma.toBuffer
    val z = added.sizes.toBuffer
    keys.indices.foreach { i =>
      previous.get(keys(i)) match {
        case Some(j) =>
          g(j) = gamma(i)
          s(j) = seGamma(i)
        case None =>
          k += keys(i)
          g += gamma(i)
          s += seGamma(i)
          z += groupSizes(i)
      }
    }
    copy(added = new LogisticAddedProviders(k.toArray, g.toArray, s.toArray, z.toArray))
  }

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
