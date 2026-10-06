package pprof.spark.engine.cox

import org.apache.spark.sql.{DataFrame, Encoders}

import pprof.spark.engine.backend.{BlockOptions, DriverGuards, OrderedReduction}
import pprof.spark.engine.layout.{GroupSizes, LayoutPlan}
import pprof.spark.engine.metadata.SoftwareInfo
import pprof.spark.engine.skeleton.LayoutSummary
import pprof.spark.numerics.{
  AliasedException,
  Cholesky,
  Evaluation,
  IterationRecord,
  NeumaierSum,
  Newton,
  NewtonOptions,
  Normal
}
import pprof.spark.numerics.kernels.Moments

/** The tie method (§7.2 and addendum §2). Breslow is the default (D-06). */
sealed abstract class Ties(val name: String) extends Product with Serializable

object Ties {
  case object Breslow extends Ties("breslow")
  case object Efron extends Ties("efron")

  val all: Seq[Ties] = Seq(Breslow, Efron)

  def fromName(name: String): Ties =
    all
      .find(_.name == name)
      .getOrElse(
        throw new IllegalArgumentException(
          s"unknown tie method $name; expected one of ${all.map(_.name).mkString(", ")}"
        )
      )
}

/** Fit options (Cox specification §5); defaults are pprof_py's and R's. */
final case class CoxOptions(
    ties: Ties = Ties.Breslow,
    maxIterations: Int = 20,
    eps: Double = 1e-9,
    maxHalvings: Int = 20,
    aliasTolerance: Double = Cholesky.DefaultTolerance,
    confidenceLevel: Double = 0.95,
    maxStratumRows: Long = 1000000L,
    blocks: BlockOptions = BlockOptions()
) {
  require(
    confidenceLevel > 0.0 && confidenceLevel < 1.0,
    s"confidenceLevel must lie in (0, 1), got $confidenceLevel"
  )
  require(maxStratumRows >= 1, s"maxStratumRows must be positive, got $maxStratumRows")

  val newton: NewtonOptions = NewtonOptions(maxIterations, eps, maxHalvings, aliasTolerance)
}

/** One coefficient with its Wald inference (Cox specification §7). */
final case class CoxCoefficient(
    feature: String,
    estimate: Double,
    standardError: Double,
    z: Double,
    pValue: Double,
    lower: Double,
    upper: Double
)

/** A fitted Cox model (Cox specification §8): estimates, model-based covariance and the information
  * matrix I(β̂) as packed upper triangles, fit diagnostics, counts, reproducibility metadata
  * (§6.10), and the feature status (NN-12). [[CoxFitIO]] saves and loads it bit for bit.
  */
final case class CoxFit(
    coefficients: Vector[CoxCoefficient],
    covariance: Vector[Double],
    information: Vector[Double],
    logLikelihood: Double,
    logLikelihoodNull: Double,
    iterations: Int,
    halvings: Int,
    converged: Boolean,
    message: String,
    observations: Long,
    events: Long,
    strata: Int,
    strataWithoutEvents: Int,
    warnings: Vector[String],
    iterationLog: Vector[IterationRecord],
    spec: CoxSpec,
    options: CoxOptions,
    software: SoftwareInfo,
    layout: LayoutSummary,
    fingerprint: Long,
    featureStatus: String = CoxPH.FeatureStatus
) {
  def estimates: Vector[Double] = coefficients.map(_.estimate)
  def standardErrors: Vector[Double] = coefficients.map(_.standardError)
  def ties: String = options.ties.name
}

/** Covariates that are constant or linear combinations of others (X-011). */
final class CoxAliasingException(val features: Seq[String])
    extends IllegalArgumentException(
      s"covariates ${features.mkString(", ")} are aliased: each is constant or a linear " +
        "combination of the others, so its coefficient is not identifiable"
    )

/** Stratified Cox regression for right-censored data with Breslow or Efron ties, case weights,
  * offsets and model-based variance (docs/spec/cox/first-slice.md, efron-weights-offsets.md).
  */
object CoxPH {

  /** Every Cox feature is Experimental until it passes the parity gate, scale test included (NN-12,
    * §9.8).
    */
  val FeatureStatus: String = "experimental"

  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  def fit(df: DataFrame, spec: CoxSpec, options: CoxOptions = CoxOptions()): CoxFit =
    withWorkingSet(df, spec, options) { prepared =>
      val p = spec.features.size
      val result =
        try
          Newton.maximize(
            beta => evaluate(prepared, beta, options.ties),
            new Array[Double](p),
            options.newton
          )
        catch {
          case e: AliasedException => throw new CoxAliasingException(e.columns.map(spec.features))
        }
      val factor = Cholesky.factor(result.evaluation.information, p, options.aliasTolerance)
      if (!factor.isFullRank) throw new CoxAliasingException(factor.aliased.map(spec.features))
      val covariance = factor.inversePacked
      val quantile = Normal.upperQuantile((1.0 - options.confidenceLevel) / 2.0)
      val coefficients = spec.features.indices.map { j =>
        val estimate = result.beta(j)
        val se = math.sqrt(covariance(Moments.packedIndex(j, j, p)))
        val z = estimate / se
        CoxCoefficient(
          spec.features(j),
          estimate,
          se,
          z,
          Normal.twoSidedPValue(z),
          estimate - quantile * se,
          estimate + quantile * se
        )
      }.toVector
      val warnings =
        if (result.converged) Vector.empty[String]
        else Vector(s"the Cox fit did not converge: ${result.message}")
      warnings.foreach(w => log.warn(w))
      CoxFit(
        coefficients,
        covariance.toVector,
        result.evaluation.information.toVector,
        result.evaluation.value,
        result.initial.value,
        result.iterations,
        result.halvings,
        result.converged,
        result.message,
        prepared.rows,
        prepared.events,
        prepared.plan.groupCount,
        prepared.strataWithoutEvents,
        warnings,
        result.log,
        spec,
        options,
        SoftwareInfo.capture(df.sparkSession),
        LayoutSummary.of(prepared.plan),
        prepared.fingerprint
      )
    }

  /** Log partial likelihood, score and information at `beta`: one pass, for function-level
    * parity (§9.2).
    */
  private[cox] def evaluateAt(
      df: DataFrame,
      spec: CoxSpec,
      beta: Array[Double],
      options: CoxOptions = CoxOptions()
  ): Evaluation =
    withWorkingSet(df, spec, options)(prepared => evaluate(prepared, beta, options.ties))

  private final case class Prepared(
      plan: LayoutPlan,
      workingSet: CoxWorkingSet,
      center: Array[Double],
      rows: Long,
      events: Long,
      strataWithoutEvents: Int,
      fingerprint: Long
  )

  private def withWorkingSet[A](df: DataFrame, spec: CoxSpec, options: CoxOptions)(
      body: Prepared => A
  ): A = {
    val input = CoxValidation.validate(df, spec)
    val p = spec.features.size
    val plan = LayoutPlan.create(
      GroupSizes.collectGroupSizes(input.frame, options.blocks),
      options.blocks.targetRowsPerBlock(p + 2)
    )
    plan.placements.find(_.rows > options.maxStratumRows).foreach { g =>
      throw new IllegalArgumentException(
        s"stratum ${g.groupIndex} has ${g.rows} rows, more than maxStratumRows = " +
          s"${options.maxStratumRows}; a stratum must fit in one block until TimeRange layouts " +
          "arrive in Phase 1b"
      )
    }
    DriverGuards.requireWithinBudget(
      s"${plan.blockCount} block partials",
      plan.blockCount.toLong * 8L * (2L + p + Moments.packedLength(p)),
      options.blocks
    )
    val workingSet = CoxWorkingSet.build(input, plan, options.blocks.resolvedStorageLevel)
    try {
      val summaries = workingSet.blocks
        .map((block: CoxBlock) => CoxKernel.summary(block))(Encoders.product[CoxBlockSummary])
        .collect()
        .sortBy(_.blockId)
      val rows = summaries.iterator.map(_.rows).sum
      val events = summaries.iterator.map(_.events).sum
      if (rows != input.rowCount || events != input.eventCount)
        throw new IllegalStateException(
          s"the blocks hold $rows rows and $events events; the input has ${input.rowCount} and " +
            s"${input.eventCount}"
        )
      val sums = OrderedReduction.reduce(summaries.toSeq.map(s => s.blockId -> s.columnSums), p)
      body(
        Prepared(
          plan,
          workingSet,
          sums.map(_ / rows.toDouble),
          rows,
          events,
          summaries.iterator.map(_.strataWithoutEvents).sum,
          summaries.iterator.map(_.fingerprint).sum
        )
      )
    } finally workingSet.release()
  }

  /** One pass: block partials combined in block order (ADR-0003). */
  private def evaluate(prepared: Prepared, beta: Array[Double], ties: Ties): Evaluation = {
    val p = prepared.center.length
    val b = beta.clone()
    val c = prepared.center.clone()
    val efron = ties == Ties.Efron
    val partials = prepared.workingSet.blocks
      .map((block: CoxBlock) => CoxKernel.partial(block, b, c, efron))(Encoders.product[CoxPartial])
      .collect()
      .sortBy(_.blockId)
      .toSeq
    val value = new NeumaierSum
    partials.foreach(part => value.add(part.value))
    Evaluation(
      value.value,
      OrderedReduction.reduce(partials.map(part => part.blockId -> part.score), p),
      OrderedReduction.reduce(
        partials.map(part => part.blockId -> part.information),
        Moments.packedLength(p)
      )
    )
  }
}
