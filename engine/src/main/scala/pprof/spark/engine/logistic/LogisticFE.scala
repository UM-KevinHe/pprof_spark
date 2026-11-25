package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, exp, lit}
import org.apache.spark.sql.types.{
  BooleanType,
  DoubleType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.slf4j.LoggerFactory
import pprof.spark.engine.backend.Storage._
import pprof.spark.engine.backend.{BlockOptions, DriverLimitExceededException, OrderedReduction}
import pprof.spark.engine.data.{InputProblem, InvalidInputException, Validation}
import pprof.spark.engine.layout.{GroupFold, GroupKey, GroupSizes, LayoutPlan, ReductionGroups}
import pprof.spark.engine.metadata.SoftwareInfo
import pprof.spark.engine.skeleton.LayoutSummary
import pprof.spark.numerics.{
  AliasedException,
  Cholesky,
  NeumaierSum,
  NeumaierVector,
  Normal,
  ProfileNewton,
  ProfilePass,
  ProfilePasses,
  Serbin,
  SerbinPasses
}
import pprof.spark.numerics.kernels.{Fingerprint, Moments, LogisticFE => Kernel}
import pprof.spark.engine.backend.Operations._

/** The logistic fixed-effect provider model, fitted by SerBIN on ProviderLocal blocks (Phase 2a
  * specification, docs/spec/logistic/fixed-effect-estimation.md). Executors compute the block
  * partials; the driver holds β, γ and the p×p system and reduces partials in block order (§11).
  * Connect-compatible (PLAT-2).
  */
object LogisticFE {

  private val log = LoggerFactory.getLogger(getClass)

  /** Columns of [[providerTable]] (specification §8). */
  val ProviderColumns: Seq[String] = Seq(
    "provider",
    "records",
    "trials",
    "events",
    "gamma",
    "var_gamma",
    "var_case_mix",
    "zero_events",
    "all_events",
    "at_bound",
    "robust_var_case_mix",
    "robust_var_fixed_beta",
    "added"
  )

  /** The margin of `at_bound` below the bound (pprof_py's `at_bound(tol = 0.1)`). */
  val AtBoundMargin: Double = 0.1

  def fit(
      df: DataFrame,
      spec: LogisticSpec,
      options: LogisticOptions = LogisticOptions()
  ): LogisticFit = {
    val input = LogisticValidation.validate(df, spec)
    val p = spec.features.size
    val sizes = GroupSizes.collectGroupSizes(input.frame, options.blocks).sortBy(_._1)
    val (fitted, excluded) =
      if (options.screen) sizes.partition(_._2 > options.minRecords)
      else (sizes, Vector.empty[(GroupKey, Long)])
    if (fitted.isEmpty)
      throw new InvalidInputException(
        Seq(InputProblem.NoProvidersFitted(spec.provider, options.minRecords))
      )
    if (fitted.size > options.maxProvidersOnDriver)
      throw new DriverLimitExceededException(
        s"${fitted.size} providers exceed maxProvidersOnDriver = ${options.maxProvidersOnDriver}; " +
          "the provider effects are held on the driver (Phase 2a specification §11, OI-50)"
      )
    // Blocks stay at the target size; their partials are reduced in groups (docs/spec/performance/step1-reduction.md).
    val (plan, groups) = layout(fitted, p, options.blocks)
    val m = plan.groupCount
    val keys = new Array[GroupKey](m)
    plan.placements.foreach(g => keys(g.groupIndex) = g.key)
    val providersOfBlock = providersOf(plan)
    val workingSet =
      LogisticWorkingSet.build(input, plan, groups, options.blocks)
    try {
      val summaries = summariesOf(workingSet.records, groups, p)
      val rows = summaries.iterator.map(_.rows).sum
      if (rows != plan.rowCount)
        throw new IllegalStateException(
          s"the blocks hold $rows rows; the plan has ${plan.rowCount}"
        )
      val records = new Array[Long](m)
      val providerEvents = new Array[Double](m)
      val providerTrials = new Array[Double](m)
      summaries.foreach { s =>
        var offset = 0
        groups.blocksOf(s.blockId).foreach { b =>
          val ids = providersOfBlock(b)
          ids.indices.foreach { i =>
            records(ids(i)) = s.records(offset + i)
            providerEvents(ids(i)) = s.providerEvents(offset + i)
            providerTrials(ids(i)) = s.providerTrials(offset + i)
          }
          offset += ids.length
        }
      }
      val events = summaries.iterator.map(_.events).sum
      val trials = summaries.iterator.map(_.trials).sum
      if (events <= 0.0 || events >= trials)
        throw new InvalidInputException(
          Seq(InputProblem.NoOutcomeVariation(events.toLong, trials.toLong))
        )
      val xbar = OrderedReduction
        .reduce(summaries.toSeq.map(s => s.blockId -> s.weightedX), p)
        .map(_ / trials)
      val yBar = events / trials
      val correlations = Moments.correlations(
        summaries
          .map(s => Moments.Centred(s.rows, s.featureMean, s.featureComoments))
          .reduceLeft(Moments.merge)
      )
      val correlated = for {
        i <- 0 until p
        j <- i + 1 until p
        if math.abs(correlations(Moments.packedIndex(i, j, p))) > options.correlationThreshold
      } yield s"(${spec.features(i)}, ${spec.features(j)})"
      val start = Array.fill(m)(StrictMath.log(yBar / (1.0 - yBar)))
      // SerBIN, or the profile-likelihood Newton through its reduced system (D-83).
      val (result, innerSteps, cappedSolves) =
        try
          if (options.newton == LogisticOptions.ProfileMethod) {
            val profile = ProfileNewton.fit(
              start,
              new Array[Double](p),
              new SparkProfilePasses(workingSet.records, providersOfBlock, p, workingSet.groups),
              options.serbin,
              options.aliasTolerance
            )
            (profile.result, profile.innerSteps, profile.capped)
          } else
            (
              Serbin.fit(
                start,
                new Array[Double](p),
                new SparkPasses(workingSet.records, providersOfBlock, p, workingSet.groups),
                options.serbin,
                options.aliasTolerance
              ),
              0L,
              0
            )
        catch {
          case e: AliasedException =>
            throw new LogisticAliasingException(e.columns.map(spec.features))
        }
      val gamma = result.gamma
      val beta = result.beta
      val effects = LogisticBlocks.effects(providersOfBlock, gamma)
      val (varianceSchur, loglik) = variancePass(workingSet.records, effects, groups, p, beta)
      val factor = Cholesky.factor(varianceSchur, p, options.aliasTolerance)
      if (!factor.isFullRank) throw new LogisticAliasingException(factor.aliased.map(spec.features))
      val covariance = factor.inversePacked
      val varGamma = new Array[Double](m)
      val varCaseMix = new Array[Double](m)
      workingSet.records
        .mapPartitions((records: Iterator[LogisticChunk]) =>
          LogisticBlocks.assemble(records).map { block =>
            val v =
              Kernel.providerVariances(block.rows, effects(block.blockId), beta, covariance, xbar)
            LogisticProviderPartial(block.blockId, v.varGamma, v.varCaseMix)
          }
        )(Encoders.product[LogisticProviderPartial])
        .collectAs("logistic.provider-variance")
        .foreach { part =>
          val ids = providersOfBlock(part.blockId)
          ids.indices.foreach { i =>
            varGamma(ids(i)) = part.varGamma(i)
            varCaseMix(ids(i)) = part.varCaseMix(i)
          }
        }
      val robust = spec.cluster.map { _ =>
        val meatParts = workingSet.records
          .mapPartitions((records: Iterator[LogisticChunk]) =>
            GroupFold(
              LogisticBlocks.assemble(records),
              (block: LogisticBlock) => block.blockId,
              groups
            )(new MeatSum(p)) { (sum, block) =>
              val meat = Kernel.robustMeat(block.rows, effects(block.blockId), beta)
              sum.add(LogisticMeatPartial(block.blockId, meat.packed, meat.clusters))
            }.map { case (group, sum) => sum.partial(group) }
          )(Encoders.product[LogisticMeatPartial])
          .collectAs("logistic.robust")
          .sortBy(_.blockId)
        val meat = OrderedReduction.reduce(
          meatParts.toSeq.map(m => m.blockId -> m.packed),
          Moments.packedLength(p)
        )
        val caseMix = new Array[Double](m)
        val fixedBeta = new Array[Double](m)
        workingSet.records
          .mapPartitions((records: Iterator[LogisticChunk]) =>
            LogisticBlocks.assemble(records).map { block =>
              val v = Kernel.robustProviders(
                block.rows,
                effects(block.blockId),
                beta,
                meat,
                covariance,
                xbar
              )
              LogisticRobustPartial(block.blockId, v.caseMix, v.fixedBeta)
            }
          )(Encoders.product[LogisticRobustPartial])
          .collectAs("logistic.robust")
          .foreach { part =>
            val ids = providersOfBlock(part.blockId)
            ids.indices.foreach { i =>
              caseMix(ids(i)) = part.caseMix(i)
              fixedBeta(ids(i)) = part.fixedBeta(i)
            }
          }
        (
          Kernel.sandwich(covariance, meat, p).toVector,
          meatParts.iterator.map(_.clusters).sum,
          caseMix,
          fixedBeta
        )
      }
      val binaryOutcomes =
        summaries.forall(_.maxOutcome <= 1.0) && summaries.exists(_.zeroOutcomes > 0L)
      val auc =
        if (binaryOutcomes)
          Some(areaUnderCurve(workingSet.records, providersOfBlock, gamma, beta, options.blocks))
        else None
      val median = Serbin.median(gamma)
      val degenerate =
        Array.tabulate(m)(k => providerEvents(k) <= 0.0 || providerEvents(k) >= providerTrials(k))
      val atBound =
        Array.tabulate(m)(k =>
          degenerate(k) || math.abs(gamma(k) - median) >= options.bound - AtBoundMargin
        )
      val quantile = Normal.upperQuantile((1.0 - options.confidenceLevel) / 2.0)
      val coefficients = spec.features.indices.map { j =>
        val se = math.sqrt(covariance(Moments.packedIndex(j, j, p)))
        val z = beta(j) / se
        LogisticCoefficient(
          spec.features(j),
          beta(j),
          se,
          z,
          Normal.twoSidedPValue(z),
          beta(j) - quantile * se,
          beta(j) + quantile * se
        )
      }.toVector
      val parameters = (p + m).toDouble
      val warnings = Vector.newBuilder[String]
      if (correlated.nonEmpty)
        warnings += s"features ${correlated.mkString(", ")} have an absolute correlation above " +
          s"${options.correlationThreshold} on the fitted rows (X-021)"
      if (excluded.nonEmpty)
        warnings += s"${excluded.size} out of ${sizes.size} providers have at most ${options.minRecords} " +
          "records and were not fitted"
      val method =
        if (options.newton == LogisticOptions.ProfileMethod) "profile-likelihood Newton"
        else "SerBIN"
      if (!result.converged)
        warnings += s"the $method fit did not converge in ${result.iterations} steps (criterion " +
          s"${result.criterion}, tol ${options.tol})"
      else if (result.shortened)
        warnings += s"the line search shortened the last step to ${result.lastStep} while the Newton " +
          s"beta step was ${result.lastNewtonBetaStep} (tol ${options.tol}); the estimates may not be " +
          "at the maximum"
      if (cappedSolves > 0)
        warnings += s"$cappedSolves inner solves of provider intercepts reached ${Kernel.ProfileMaxSteps} steps; " +
          "their last values were used (D-83 §8)"
      val degenerateCount = degenerate.count(identity)
      if (degenerateCount > 0)
        warnings += s"$degenerateCount providers have no events or only events; their effects have no " +
          "finite estimate and are held at the bound (X-018)"
      val allWarnings = warnings.result()
      allWarnings.foreach(w => log.warn(w))
      LogisticFit(
        coefficients,
        covariance.toVector,
        new LogisticProviders(
          keys,
          gamma,
          varGamma,
          varCaseMix,
          records,
          providerEvents,
          providerTrials,
          atBound,
          robust.map(_._3).getOrElse(Array.emptyDoubleArray),
          robust.map(_._4).getOrElse(Array.emptyDoubleArray)
        ),
        excluded,
        loglik,
        2.0 * parameters + -2.0 * loglik,
        parameters * StrictMath.log(rows.toDouble) + -2.0 * loglik,
        result.iterations,
        result.converged,
        result.criterion,
        result.lastStep,
        rows,
        trials,
        events,
        allWarnings,
        result.trace,
        spec,
        options,
        SoftwareInfo.capture(df.sparkSession),
        LayoutSummary.of(plan, groups),
        summaries.iterator.map(_.fingerprint).sum,
        robustCovariance = robust.map(_._1),
        clusters = robust.map(_._2).getOrElse(0L),
        auc = auc,
        innerSteps = innerSteps,
        cappedSolves = cappedSolves
      )
    } finally workingSet.release()
  }

  /** The providers as a table with [[ProviderColumns]], those added after fitting included (§8a): rows built on the driver, where the
    * fit holds them (specification §8 and §11).
    */
  def providerTable(spark: SparkSession, fit: LogisticFit): DataFrame = {
    val providers = fit.providers
    val view = LogisticProviderView.of(fit)
    val keyIsText = providers.keys.headOption.exists(_.isInstanceOf[GroupKey.Text])
    val schema = StructType(
      Seq(
        StructField("provider", if (keyIsText) StringType else LongType, nullable = false),
        StructField("records", LongType, nullable = false),
        StructField("trials", DoubleType, nullable = false),
        StructField("events", DoubleType, nullable = false),
        StructField("gamma", DoubleType, nullable = false),
        StructField("var_gamma", DoubleType, nullable = false),
        StructField("var_case_mix", DoubleType, nullable = false),
        StructField("zero_events", BooleanType, nullable = false),
        StructField("all_events", BooleanType, nullable = false),
        StructField("at_bound", BooleanType, nullable = false),
        StructField("robust_var_case_mix", DoubleType, nullable = true),
        StructField("robust_var_fixed_beta", DoubleType, nullable = true),
        StructField("added", BooleanType, nullable = false)
      )
    )
    // The view's providers: fitted ones keep their data-based counts and flags; one added after
    // fitting has its group size as records, no trials or events, and no flags (§8a).
    val rows = (0 until view.size).map { v =>
      val k = view.fitted(v)
      Row(
        GroupKey.value(view.keys(v)),
        view.records(v),
        if (k >= 0) providers.trials(k) else 0.0,
        if (k >= 0) providers.events(k) else 0.0,
        view.gamma(v),
        view.varGamma(v),
        view.varCaseMix(v),
        k >= 0 && providers.zeroEvents(k),
        k >= 0 && providers.allEvents(k),
        k >= 0 && providers.atBound(k),
        if (view.robustVarCaseMix.isEmpty) null else view.robustVarCaseMix(v),
        if (view.robustVarFixedBeta.isEmpty) null else view.robustVarFixedBeta(v),
        view.added(v)
      )
    }
    spark.createDataFrame(rows.asJava, schema)
  }

  /** Wald tests of βⱼ = `nullValue` from the fit (Phase 2b §4): `alternative` is `two_sided`, `less` or
    * `greater`; standard errors model-based, or cluster-robust when `robust` (the fit needs a cluster
    * column).
    */
  def waldTests(
      fit: LogisticFit,
      nullValue: Double = 0.0,
      alternative: String = "two_sided",
      level: Double = 0.95,
      robust: Boolean = false
  ): Vector[LogisticTest] = {
    require(level > 0.0 && level < 1.0, s"level must lie strictly between 0 and 1, got $level")
    val covariance =
      if (robust)
        fit.robustCovariance.getOrElse(
          throw new IllegalArgumentException("robust Wald tests need a fit with a cluster column")
        )
      else fit.covariance
    val p = fit.coefficients.size
    val alpha = 1.0 - level
    fit.coefficients.indices.map { j =>
      val estimate = fit.coefficients(j).estimate
      val se = math.sqrt(covariance(Moments.packedIndex(j, j, p)))
      val z = (estimate - nullValue) / se
      val (pValue, lower, upper) = alternative match {
        case "two_sided" =>
          val q = Normal.upperQuantile(alpha / 2.0)
          (Normal.twoSidedPValue(z), estimate - q * se, estimate + q * se)
        case "less" =>
          (
            Normal.upperTail(-z),
            Double.NegativeInfinity,
            estimate + Normal.upperQuantile(alpha) * se
          )
        case "greater" =>
          (
            Normal.upperTail(z),
            estimate - Normal.upperQuantile(alpha) * se,
            Double.PositiveInfinity
          )
        case other =>
          throw new IllegalArgumentException(
            s"alternative must be two_sided, less or greater, got $other"
          )
      }
      LogisticTest(
        fit.coefficients(j).feature,
        estimate,
        se,
        z,
        pValue,
        lower,
        upper,
        if (robust) "wald-robust" else "wald"
      )
    }.toVector
  }

  /** Likelihood-ratio (`lr`) or score (`score`) tests of βⱼ = 0 for `covariates` (all by default), by
    * refitting without each on the fit's working set (Phase 2b §5). The data must be the fit's
    * (fingerprint, API-3); intervals are Wald-based, as pprof_py's.
    */
  def covariateTests(
      df: DataFrame,
      fit: LogisticFit,
      method: String,
      covariates: Seq[String] = Seq.empty
  ): Vector[LogisticTest] = {
    require(method == "lr" || method == "score", s"method must be lr or score, got $method")
    val features = fit.spec.features
    val p = features.size
    if (p < 2)
      throw new IllegalArgumentException(
        "the likelihood-ratio and score tests refit without the covariate tested; they need at least two features"
      )
    val tested = if (covariates.isEmpty) features else covariates
    val unknown = tested.filterNot(features.contains)
    require(unknown.isEmpty, s"not features of the fit: ${unknown.mkString(", ")}")
    val input = LogisticValidation.validate(df, fit.spec)
    val providers = fit.providers
    val m = providers.size
    val (plan, groups) = layout(
      providers.keys.indices.map(k => providers.keys(k) -> providers.records(k)).toVector,
      p,
      fit.options.blocks
    )
    val providersOfBlock = providersOf(plan)
    val workingSet = LogisticWorkingSet.build(input, plan, groups, fit.options.blocks)
    try {
      val fingerprint = fingerprintOf(workingSet)
      if (fingerprint != fit.fingerprint)
        throw new IllegalArgumentException(
          "the data differ from the fit's (fingerprint); covariate tests need the training data (API-3)"
        )
      val yBar = fit.events / fit.trials
      val start = Array.fill(m)(StrictMath.log(yBar / (1.0 - yBar)))
      val quantile = Normal.upperQuantile((1.0 - fit.options.confidenceLevel) / 2.0)
      tested.map { name =>
        val j = features.indexOf(name)
        val reduced = workingSet.records
          .map((record: LogisticChunk) => dropColumn(record, j))(Encoders.product[LogisticChunk])
          .persistAt(fit.options.blocks)
        try {
          val passes = new SparkPasses(reduced, providersOfBlock, p - 1, workingSet.groups)
          val refit =
            try
              Serbin.fit(
                start,
                new Array[Double](p - 1),
                passes,
                fit.options.serbin,
                fit.options.aliasTolerance
              )
            catch {
              case e: AliasedException =>
                throw new LogisticAliasingException(e.columns.map(features.filterNot(_ == name)))
            }
          val effects = LogisticBlocks.effects(providersOfBlock, refit.gamma)
          val statistic =
            if (method == "lr") {
              val reducedBeta = refit.beta
              val sum = new NeumaierSum
              reduced
                .mapPartitions((records: Iterator[LogisticChunk]) =>
                  LogisticBlocks
                    .assemble(records)
                    .map(block =>
                      LogisticLoglikPartial(
                        block.blockId,
                        Kernel.loglik(block.rows, effects(block.blockId), reducedBeta)
                      )
                    )
                )(Encoders.product[LogisticLoglikPartial])
                .collectAs("logistic.tests")
                .sortBy(_.blockId)
                .foreach(part => sum.add(part.loglik))
              2.0 * (fit.loglik - sum.value)
            } else {
              val full = Array.tabulate(p)(k =>
                if (k < j) refit.beta(k) else if (k == j) 0.0 else refit.beta(k - 1)
              )
              val parts = workingSet.records
                .mapPartitions((records: Iterator[LogisticChunk]) =>
                  GroupFold(
                    LogisticBlocks.assemble(records),
                    (block: LogisticBlock) => block.blockId,
                    groups
                  )(new ScoreSum(p)) { (sum, block) =>
                    val t = Kernel.scoreTerms(block.rows, effects(block.blockId), full)
                    sum.add(LogisticScorePartial(block.blockId, t.packed, t.score))
                  }.map { case (group, sum) => sum.partial(group) }
                )(Encoders.product[LogisticScorePartial])
                .collectAs("logistic.tests")
                .sortBy(_.blockId)
                .toSeq
              val factor = Cholesky.factor(
                OrderedReduction.reduce(
                  parts.map(t => t.blockId -> t.packed),
                  Moments.packedLength(p)
                ),
                p,
                fit.options.aliasTolerance
              )
              if (!factor.isFullRank)
                throw new LogisticAliasingException(factor.aliased.map(features))
              val score = OrderedReduction.reduce(parts.map(t => t.blockId -> t.score), p)(j)
              score * score * factor.inversePacked(Moments.packedIndex(j, j, p))
            }
          val c = fit.coefficients(j)
          LogisticTest(
            name,
            c.estimate,
            c.standardError,
            statistic,
            if (statistic <= 0.0) 1.0 else Normal.twoSidedPValue(math.sqrt(statistic)),
            c.estimate - quantile * c.standardError,
            c.estimate + quantile * c.standardError,
            method
          )
        } finally {
          reduced.unpersist(blocking = false)
          ()
        }
      }.toVector
    } finally workingSet.release()
  }

  /** The working set of `df` laid out as the fit's, after checking that `df` is the fit's training data
    * (fingerprint, API-3); with each block's providers in key order. The caller releases it.
    */
  private[logistic] def trainingWorkingSet(
      df: DataFrame,
      fit: LogisticFit
  ): (LogisticWorkingSet, Array[Array[Int]]) = {
    val p = fit.spec.features.size
    val input = LogisticValidation.validate(df, fit.spec)
    val providers = fit.providers
    val (plan, groups) = layout(
      providers.keys.indices.map(k => providers.keys(k) -> providers.records(k)).toVector,
      p,
      fit.options.blocks
    )
    val providersOfBlock = providersOf(plan)
    val workingSet = LogisticWorkingSet.build(input, plan, groups, fit.options.blocks)
    val fingerprint =
      try fingerprintOf(workingSet)
      catch {
        case e: Throwable =>
          workingSet.release()
          throw e
      }
    if (fingerprint != fit.fingerprint) {
      workingSet.release()
      throw new IllegalArgumentException(
        "the data differ from the fit's (fingerprint); provider and covariate tests need the training data (API-3)"
      )
    }
    (workingSet, providersOfBlock)
  }

  /** The working set's block summaries, folded in groups and in block order (§6.10). */
  private def summariesOf(
      records: Dataset[LogisticChunk],
      groups: ReductionGroups,
      p: Int
  ): Array[LogisticSummary] =
    records
      .mapPartitions((chunks: Iterator[LogisticChunk]) =>
        GroupFold(
          LogisticBlocks.assemble(chunks),
          (block: LogisticBlock) => block.blockId,
          groups
        )(new SummaryGroup(p))((sum, block) => sum.add(summarize(block))).map { case (group, sum) =>
          sum.summary(group)
        }
      )(Encoders.product[LogisticSummary])
      .collectAs("logistic.summarize")
      .sortBy(_.blockId)

  /** The variance pass at (γ, β): the Schur complement at clipped π and the log-likelihood, reduced in block order
    * within groups (Phase 2a specification §8, step1-reduction.md §4).
    */
  private def variancePass(
      records: Dataset[LogisticChunk],
      effects: Array[Array[Double]],
      groups: ReductionGroups,
      p: Int,
      beta: Array[Double]
  ): (Array[Double], Double) = {
    val variance = records
      .mapPartitions((chunks: Iterator[LogisticChunk]) =>
        GroupFold(
          LogisticBlocks.assemble(chunks),
          (block: LogisticBlock) => block.blockId,
          groups
        )(new VarianceSum(p))((sum, block) =>
          sum.add(
            LogisticVariancePartial(
              block.blockId,
              Kernel.varianceSchur(block.rows, effects(block.blockId), beta),
              Kernel.loglik(block.rows, effects(block.blockId), beta)
            )
          )
        ).map { case (group, sum) => sum.partial(group) }
      )(Encoders.product[LogisticVariancePartial])
      .collectAs("logistic.variance")
      .sortBy(_.blockId)
    val loglik = new NeumaierSum
    variance.foreach(v => loglik.add(v.loglik))
    (
      OrderedReduction.reduce(
        variance.toSeq.map(v => v.blockId -> v.packed),
        Moments.packedLength(p)
      ),
      loglik.value
    )
  }

  /** Pass A and the variance pass at one fixed (γ, β) (D-79, step2-chunks.md §6). */
  private[logistic] final case class FixedPoint(
      schur: Array[Double],
      rhs: Array[Double],
      loglik: Double,
      scoreTerm: Double,
      varianceSchur: Array[Double],
      varianceLoglik: Double
  )

  /** The golden runs' fixed points (D-79): the data's merged co-moments, then pass A and the variance pass at SerBIN's
    * start, every γ at logit(ȳ) and β = 0, and at `gamma` and `beta`. The working set is laid out and summarized as
    * `fit` does it.
    */
  private[logistic] def fixedPoints(
      df: DataFrame,
      spec: LogisticSpec,
      options: LogisticOptions,
      gamma: Array[Double],
      beta: Array[Double]
  ): (Array[Double], FixedPoint, FixedPoint) = {
    val input = LogisticValidation.validate(df, spec)
    val p = spec.features.size
    val sizes = GroupSizes.collectGroupSizes(input.frame, options.blocks).sortBy(_._1)
    val fitted = if (options.screen) sizes.filter(_._2 > options.minRecords) else sizes
    val (plan, groups) = layout(fitted, p, options.blocks)
    val providersOfBlock = providersOf(plan)
    val workingSet = LogisticWorkingSet.build(input, plan, groups, options.blocks)
    try {
      val summaries = summariesOf(workingSet.records, groups, p)
      val events = summaries.iterator.map(_.events).sum
      val trials = summaries.iterator.map(_.trials).sum
      val yBar = events / trials
      val comoments = summaries
        .map(s => Moments.Centred(s.rows, s.featureMean, s.featureComoments))
        .reduceLeft(Moments.merge)
        .comoments
      val passes = new SparkPasses(workingSet.records, providersOfBlock, p, workingSet.groups)
      def at(g: Array[Double], b: Array[Double]): FixedPoint = {
        val s = passes.schur(g, b)
        val (variance, loglik) =
          variancePass(
            workingSet.records,
            LogisticBlocks.effects(providersOfBlock, g),
            groups,
            p,
            b
          )
        FixedPoint(s.packed, s.rhs, s.loglik, s.scoreTerm, variance, loglik)
      }
      (
        comoments,
        at(Array.fill(plan.groupCount)(StrictMath.log(yBar / (1.0 - yBar))), new Array[Double](p)),
        at(gamma, beta)
      )
    } finally workingSet.release()
  }

  /** The layout of a fit's working set and of every later call that rebuilds it: blocks at the target size, their
    * partials reduced in groups (docs/spec/performance/step1-reduction.md; step2-chunks.md §4).
    */
  private[logistic] def layout(
      providers: Seq[(GroupKey, Long)],
      p: Int,
      blocks: BlockOptions
  ): (LayoutPlan, ReductionGroups) = {
    val plan = LayoutPlan.create(providers, blocks.targetRowsPerBlock(p + 2))
    (plan, ReductionGroups.plan(plan.blockCount, 8L * (Moments.packedLength(p) + p + 2L), blocks))
  }

  /** Each block's providers, in key order. */
  private[logistic] def providersOf(plan: LayoutPlan): Array[Array[Int]] = {
    val lists = Array.fill(plan.blockCount)(Array.newBuilder[Int])
    plan.placements.sortBy(_.groupIndex).foreach(g => lists(g.blockId) += g.groupIndex)
    lists.map(_.result())
  }

  /** The data's fingerprint (§6.10) over a working set, one value per block. */
  private def fingerprintOf(workingSet: LogisticWorkingSet): Long =
    workingSet.records
      .mapPartitions((records: Iterator[LogisticChunk]) =>
        LogisticBlocks.assemble(records).map(block => fingerprint(block))
      )(Encoders.scalaLong)
      .collectAs("logistic.summarize")
      .sum

  /** The rows of `df` with `linear_predictor` and `probability` (Phase 2b §4): γ̂ⱼ + xᵀβ̂ and its inverse
    * logit, with Spark's StrictMath-based `exp`. Rows of providers not in the fit fail with counts
    * (X-022).
    */
  def predict(df: DataFrame, fit: LogisticFit): DataFrame = {
    val spark = df.sparkSession
    val missing = (fit.spec.features :+ fit.spec.provider).filterNot(df.columns.contains)
    if (missing.nonEmpty) throw new InvalidInputException(missing.map(InputProblem.MissingColumn))
    val providers = LogisticProviderView.of(fit)
    val text = providers.keys.headOption.exists(_.isInstanceOf[GroupKey.Text])
    val keyColumn = "__pprof_provider_key"
    val gammaColumn = "__pprof_gamma"
    val effects = spark.createDataFrame(
      (0 until providers.size)
        .map(k => Row(GroupKey.value(providers.keys(k)), providers.gamma(k)))
        .asJava,
      StructType(
        Seq(
          StructField(keyColumn, if (text) StringType else LongType, nullable = false),
          StructField(gammaColumn, DoubleType, nullable = false)
        )
      )
    )
    val provider = Validation.column(fit.spec.provider)
    val joined = df.join(
      broadcast(effects),
      (if (text) provider.cast(StringType) else provider.cast(LongType)) === col(keyColumn),
      "left"
    )
    val unknown = joined.filter(col(gammaColumn).isNull).countAs("logistic.predict")
    if (unknown > 0L)
      throw new InvalidInputException(
        Seq(InputProblem.UnknownProviders(fit.spec.provider, unknown))
      )
    val dot = fit.spec.features.zip(fit.estimates).foldLeft(lit(0.0)) { case (acc, (name, b)) =>
      acc + Validation.column(name).cast(DoubleType) * lit(b)
    }
    joined
      .withColumn("linear_predictor", col(gammaColumn) + dot)
      .withColumn("probability", lit(1.0) / (lit(1.0) + exp(-col("linear_predictor"))))
      .drop(keyColumn, gammaColumn)
  }

  /** The Mann–Whitney AUC of the fitted π against 0/1 outcomes, exact: one sort by π (equal values share
    * a partition), integer counts per partition, combined in partition order (Phase 2b §5).
    *
    * Leaner since round 173 (OI-119), with the same result to the bit: each task takes its blocks' effects
    * from `gamma` instead of a join that copied every cached block, and the scores are computed once and
    * kept at the fit's storage level for the sort, which reads them twice (its sample, then its shuffle).
    */
  private def areaUnderCurve(
      records: Dataset[LogisticChunk],
      providersOfBlock: Array[Array[Int]],
      gamma: Array[Double],
      beta: Array[Double],
      storage: BlockOptions
  ): Double = {
    val b = beta.clone()
    val gammaOfBlock = providersOfBlock.map(_.map(gamma(_)))
    val scores = records
      .mapPartitions((chunks: Iterator[LogisticChunk]) =>
        LogisticBlocks.assemble(chunks).flatMap { block =>
          val pi = Kernel.fitted(block.rows, gammaOfBlock(block.blockId), b)
          pi.indices.iterator.map(r => LogisticScore(pi(r), block.y(r) > 0.5))
        }
      )(Encoders.product[LogisticScore])
      .persistAt(storage)
    val parts =
      try
        scores
          .orderBy("pi")
          .mapPartitions((sorted: Iterator[LogisticScore]) => Iterator(aucPartial(sorted)))(
            Encoders.product[LogisticAucPartial]
          )
          .collectAs("logistic.auc")
      finally scores.unpersist(blocking = false)
    var negativesBefore = 0L
    var twiceNumerator = 0L
    var positives = 0L
    var negatives = 0L
    parts.foreach { part =>
      twiceNumerator += part.twiceNumerator + 2L * part.positives * negativesBefore
      negativesBefore += part.negatives
      positives += part.positives
      negatives += part.negatives
    }
    twiceNumerator.toDouble / (2.0 * positives.toDouble * negatives.toDouble)
  }

  private def aucPartial(scores: Iterator[LogisticScore]): LogisticAucPartial = {
    var positives = 0L
    var negatives = 0L
    var twice = 0L
    var below = 0L
    var current = Double.NaN
    var groupPositives = 0L
    var groupNegatives = 0L
    def flush(): Unit = {
      twice += groupPositives * (2L * below + groupNegatives)
      below += groupNegatives
      positives += groupPositives
      negatives += groupNegatives
      groupPositives = 0L
      groupNegatives = 0L
    }
    scores.foreach { s =>
      if (s.pi != current) {
        flush()
        current = s.pi
      }
      if (s.positive) groupPositives += 1L else groupNegatives += 1L
    }
    flush()
    LogisticAucPartial(positives, negatives, twice)
  }

  private def dropColumn(record: LogisticChunk, j: Int): LogisticChunk = {
    val p = record.p
    val rows = record.y.length
    val x = new Array[Double](rows * (p - 1))
    var r = 0
    while (r < rows) {
      var k = 0
      var c = 0
      while (k < p) {
        if (k != j) {
          x(r * (p - 1) + c) = record.x(r * p + k)
          c += 1
        }
        k += 1
      }
      r += 1
    }
    record.copy(p = p - 1, x = x)
  }

  private def summarize(block: LogisticBlock): LogisticSummary = {
    val p = block.p
    val rows = block.rows
    val providers = block.groupIndex.length
    val weighted = new NeumaierVector(p)
    val records = new Array[Long](providers)
    val providerEvents = new Array[Double](providers)
    val providerTrials = new Array[Double](providers)
    var events = 0.0
    var trials = 0.0
    var maxOutcome = 0.0
    var zeroOutcomes = 0L
    var g = 0
    while (g < providers) {
      val until = rows.end(g)
      var r = rows.start(g)
      while (r < until) {
        val xs = rows.xs(r)
        val base = rows.xBase(r)
        val y = rows.yAt(r)
        val n = rows.nAt(r)
        records(g) += 1L
        providerEvents(g) += y
        providerTrials(g) += n
        events += y
        trials += n
        maxOutcome = math.max(maxOutcome, y)
        if (y == 0.0) zeroOutcomes += 1L
        var j = 0
        while (j < p) {
          weighted.addAt(j, n * xs(base + j))
          j += 1
        }
        r += 1
      }
      g += 1
    }
    val moments = Moments.centredRows(block.rowCount, p, rows.xs, rows.xBase)
    LogisticSummary(
      block.blockId,
      block.rowCount.toLong,
      events,
      trials,
      weighted.values,
      moments.mean,
      moments.comoments,
      fingerprint(block),
      records,
      providerEvents,
      providerTrials,
      maxOutcome,
      zeroOutcomes
    )
  }

  /** The block's share of the data's fingerprint (§6.10): the wrapping sum of its rows' hashes. */
  private def fingerprint(block: LogisticBlock): Long = {
    val p = block.p
    val rows = block.rows
    val values = new Array[Double](p + 2)
    var sum = 0L
    var g = 0
    while (g < block.groupIndex.length) {
      val until = rows.end(g)
      var r = rows.start(g)
      while (r < until) {
        val xs = rows.xs(r)
        val base = rows.xBase(r)
        values(0) = rows.yAt(r)
        values(1) = rows.nAt(r)
        var j = 0
        while (j < p) {
          values(j + 2) = xs(base + j)
          j += 1
        }
        sum += Fingerprint.row(block.groupIndex(g), block.rowId(r), values, 0, p + 2)
        r += 1
      }
      g += 1
    }
    sum
  }

  /** Passes A and B over the persisted records: each block takes its providers' effects in the task (OI-105), and
    * partials are reduced in block order within groups (§6.8, step1-reduction.md §4).
    */
  private final class SparkPasses(
      records: Dataset[LogisticChunk],
      providersOfBlock: Array[Array[Int]],
      p: Int,
      groups: ReductionGroups
  ) extends SerbinPasses {

    def schur(gamma: Array[Double], beta: Array[Double]): Kernel.Schur = {
      val b = beta.clone()
      val effects = LogisticBlocks.effects(providersOfBlock, gamma)
      val (folds, width) = (groups, p)
      val parts = records
        .mapPartitions((chunks: Iterator[LogisticChunk]) =>
          GroupFold(
            LogisticBlocks.assemble(chunks),
            (block: LogisticBlock) => block.blockId,
            folds
          )(
            new SchurSum(width)
          ) { (sum, block) =>
            val s = Kernel.schur(block.rows, effects(block.blockId), b)
            sum.add(LogisticSchurPartial(block.blockId, s.packed, s.rhs, s.scoreTerm, s.loglik))
          }.map { case (group, sum) => sum.partial(group) }
        )(Encoders.product[LogisticSchurPartial])
        .collectAs("logistic.pass-a")
        .sortBy(_.blockId)
        .toSeq
      val term = new NeumaierSum
      val loglik = new NeumaierSum
      parts.foreach { part =>
        term.add(part.scoreTerm)
        loglik.add(part.loglik)
      }
      Kernel.Schur(
        OrderedReduction
          .reduce(parts.map(part => part.blockId -> part.packed), Moments.packedLength(p)),
        OrderedReduction.reduce(parts.map(part => part.blockId -> part.rhs), p),
        term.value,
        loglik.value
      )
    }

    def trial(
        gamma: Array[Double],
        beta: Array[Double],
        deltaBeta: Array[Double],
        steps: Array[Double]
    ): Kernel.Trial = {
      val b = beta.clone()
      val d = deltaBeta.clone()
      val v = steps.clone()
      val effects = LogisticBlocks.effects(providersOfBlock, gamma)
      val parts = records
        .mapPartitions((chunks: Iterator[LogisticChunk]) =>
          LogisticBlocks.assemble(chunks).map { block =>
            val t = Kernel.trial(block.rows, effects(block.blockId), b, d, v)
            LogisticTrialPartial(block.blockId, t.deltaGamma, t.loglik)
          }
        )(Encoders.product[LogisticTrialPartial])
        .collectAs("logistic.pass-b")
        .sortBy(_.blockId)
      val deltaGamma = new Array[Double](gamma.length)
      parts.foreach { part =>
        val ids = providersOfBlock(part.blockId)
        ids.indices.foreach(i => deltaGamma(ids(i)) = part.deltaGamma(i))
      }
      val sums = Array.fill(v.length)(new NeumaierSum)
      parts.foreach(part => part.loglik.indices.foreach(t => sums(t).add(part.loglik(t))))
      Kernel.Trial(deltaGamma, sums.map(_.value))
    }
  }

  /** The profile pass (docs/spec/logistic/profile-newton.md §6): per block, the intercepts solved in the task and pass
    * A's sums at them, folded in reduction groups as pass A's partials are; the intercepts come back in block order.
    */
  private final class SparkProfilePasses(
      records: Dataset[LogisticChunk],
      providersOfBlock: Array[Array[Int]],
      p: Int,
      groups: ReductionGroups
  ) extends ProfilePasses {

    def solve(
        gamma: Array[Double],
        beta: Array[Double],
        median: Double,
        bound: Double
    ): ProfilePass = {
      val b = beta.clone()
      val effects = LogisticBlocks.effects(providersOfBlock, gamma)
      val (folds, width, med, limit) = (groups, p, median, bound)
      val parts = records
        .mapPartitions((chunks: Iterator[LogisticChunk]) =>
          GroupFold(
            LogisticBlocks.assemble(chunks),
            (block: LogisticBlock) => block.blockId,
            folds
          )(
            new ProfileSum(width)
          ) { (sum, block) =>
            sum
              .add(block.blockId, Kernel.profile(block.rows, effects(block.blockId), b, med, limit))
          }.map { case (group, sum) => sum.partial(group) }
        )(Encoders.product[LogisticProfilePartial])
        .collectAs("logistic.profile")
        .sortBy(_.blockId)
        .toSeq
      val term = new NeumaierSum
      val loglik = new NeumaierSum
      parts.foreach { part =>
        term.add(part.scoreTerm)
        loglik.add(part.loglik)
      }
      val solved = new Array[Double](gamma.length)
      val held = new Array[Int](gamma.length)
      parts.foreach { part =>
        var offset = 0
        part.blocks.foreach { blockId =>
          val ids = providersOfBlock(blockId)
          ids.indices.foreach { i =>
            solved(ids(i)) = part.gamma(offset + i)
            held(ids(i)) = part.held(offset + i)
          }
          offset += ids.length
        }
      }
      ProfilePass(
        Kernel.Schur(
          OrderedReduction
            .reduce(parts.map(part => part.blockId -> part.packed), Moments.packedLength(p)),
          OrderedReduction.reduce(parts.map(part => part.blockId -> part.rhs), p),
          term.value,
          loglik.value
        ),
        solved,
        held,
        parts.map(_.innerSteps).sum,
        parts.map(_.capped).sum
      )
    }
  }
}
