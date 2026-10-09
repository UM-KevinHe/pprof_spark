package pprof.spark.engine.linear

import org.apache.spark.sql.DataFrame
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.metadata.SoftwareInfo
import pprof.spark.engine.skeleton.LayoutSummary

/** Column roles of the linear fixed-effect model (docs/spec/linear/fixed-effect-estimation.md §5). */
final case class LinearSpec(
    outcome: String,
    features: Seq[String],
    provider: String,
    rowId: Option[String] = None
)

/** `varianceOption`: `complete` (the exact Var(γ̂ⱼ), the default) or `simplified` (σ̂²/nⱼ), as pprof_py's
  * `gamma_var_option` (§3).
  */
final case class LinearOptions(
    varianceOption: String = "complete",
    blocks: BlockOptions = BlockOptions()
) {
  require(
    LinearOptions.VarianceOptions.contains(varianceOption),
    s"varianceOption must be complete or simplified, got $varianceOption"
  )
}

object LinearOptions {
  val VarianceOptions: Seq[String] = Seq("complete", "simplified")
}

final case class LinearCoefficient(feature: String, estimate: Double, standardError: Double)

/** One feature's t test and interval (§4). */
final case class LinearTest(
    feature: String,
    estimate: Double,
    standardError: Double,
    statistic: Double,
    pValue: Double,
    lower: Double,
    upper: Double
)

/** The fitted model (§8). `covariance` is σ̂²V, packed; `providers` is the distributed provider table (key,
  * `records`, `gamma`, `variance`, `se`), persisted.
  */
final case class LinearFit(
    coefficients: Vector[LinearCoefficient],
    covariance: Vector[Double],
    providers: DataFrame,
    sigma: Double,
    rss: Double,
    tss: Double,
    rows: Long,
    providerCount: Int,
    degreesOfFreedom: Long,
    loglik: Double,
    aic: Double,
    bic: Double,
    condition: Double,
    warnings: Vector[String],
    spec: LinearSpec,
    options: LinearOptions,
    software: SoftwareInfo,
    layout: LayoutSummary,
    fingerprint: Long,
    status: String = "experimental"
) {
  def estimates: Vector[Double] = coefficients.map(_.estimate)
  def standardErrors: Vector[Double] = coefficients.map(_.standardError)

  /** R² on the training data: 1 − RSS / Σ(y − ȳ)², pprof_py's `score` on the rows fitted. */
  def r2: Double = 1.0 - rss / tss
}

/** Aliased features (X-032): the within cross-product is singular. */
final class LinearAliasingException(val features: Seq[String])
    extends IllegalArgumentException(
      s"the features ${features.mkString(", ")} are aliased: collinear with other features after centring " +
        "within providers, or constant within every provider, so the provider effects absorb them (X-032)"
    )
