package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, exp, lit}
import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.slf4j.LoggerFactory
import pprof.spark.engine.backend.{DriverGuards, DriverLimitExceededException, OrderedReduction}
import pprof.spark.engine.data.{InputProblem, InvalidInputException, Validation}
import pprof.spark.engine.layout.{GroupKey, GroupSizes, LayoutPlan}
import pprof.spark.engine.metadata.SoftwareInfo
import pprof.spark.engine.skeleton.LayoutSummary
import pprof.spark.numerics.{
  AliasedException,
  Cholesky,
  NeumaierSum,
  NeumaierVector,
  Normal,
  Serbin,
  SerbinPasses
}
import pprof.spark.numerics.kernels.{Fingerprint, Moments, LogisticFE => Kernel}

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
    "robust_var_fixed_beta"
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
    val plan = LayoutPlan.create(fitted, options.blocks.targetRowsPerBlock(p + 2))
    DriverGuards.requireWithinBudget(
      s"${plan.blockCount} block partials",
      plan.blockCount.toLong * 8L * (Moments.packedLength(p) + p + 2L),
      options.blocks
    )
    val m = plan.groupCount
    val keys = new Array[GroupKey](m)
    plan.placements.foreach(g => keys(g.groupIndex) = g.key)
    val providersOfBlock = {
      val lists = Array.fill(plan.blockCount)(Array.newBuilder[Int])
      plan.placements.sortBy(_.groupIndex).foreach(g => lists(g.blockId) += g.groupIndex)
      lists.map(_.result())
    }
    val workingSet = LogisticWorkingSet.build(input, plan, options.blocks.resolvedStorageLevel)
    try {
      val summaries = workingSet.blocks
        .map((block: LogisticBlock) => summarize(block))(Encoders.product[LogisticSummary])
        .collect()
        .sortBy(_.blockId)
      val rows = summaries.iterator.map(_.rows).sum
      if (rows != plan.rowCount)
        throw new IllegalStateException(
          s"the blocks hold $rows rows; the plan has ${plan.rowCount}"
        )
      val records = new Array[Long](m)
      val providerEvents = new Array[Double](m)
      val providerTrials = new Array[Double](m)
      summaries.foreach { s =>
        val ids = providersOfBlock(s.blockId)
        ids.indices.foreach { i =>
          records(ids(i)) = s.records(i)
          providerEvents(ids(i)) = s.providerEvents(i)
          providerTrials(ids(i)) = s.providerTrials(i)
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
      val passes = new SparkPasses(workingSet.blocks, providersOfBlock, p)
      val result =
        try
          Serbin.fit(
            Array.fill(m)(StrictMath.log(yBar / (1.0 - yBar))),
            new Array[Double](p),
            passes,
            options.serbin,
            options.aliasTolerance
          )
        catch {
          case e: AliasedException =>
            throw new LogisticAliasingException(e.columns.map(spec.features))
        }
      val gamma = result.gamma
      val beta = result.beta
      val variance = passes
        .withGamma(gamma)
        .map((block: LogisticBlock) =>
          LogisticVariancePartial(
            block.blockId,
            Kernel.varianceSchur(block.rows, block.gamma, beta),
            Kernel.loglik(block.rows, block.gamma, beta)
          )
        )(Encoders.product[LogisticVariancePartial])
        .collect()
        .sortBy(_.blockId)
      val factor = Cholesky.factor(
        OrderedReduction
          .reduce(variance.toSeq.map(v => v.blockId -> v.packed), Moments.packedLength(p)),
        p,
        options.aliasTolerance
      )
      if (!factor.isFullRank) throw new LogisticAliasingException(factor.aliased.map(spec.features))
      val covariance = factor.inversePacked
      val loglikSum = new NeumaierSum
      variance.foreach(v => loglikSum.add(v.loglik))
      val loglik = loglikSum.value
      val varGamma = new Array[Double](m)
      val varCaseMix = new Array[Double](m)
      passes
        .withGamma(gamma)
        .map { (block: LogisticBlock) =>
          val v = Kernel.providerVariances(block.rows, block.gamma, beta, covariance, xbar)
          LogisticProviderPartial(block.blockId, v.varGamma, v.varCaseMix)
        }(Encoders.product[LogisticProviderPartial])
        .collect()
        .foreach { part =>
          val ids = providersOfBlock(part.blockId)
          ids.indices.foreach { i =>
            varGamma(ids(i)) = part.varGamma(i)
            varCaseMix(ids(i)) = part.varCaseMix(i)
          }
        }
      val robust = spec.cluster.map { _ =>
        val meatParts = passes
          .withGamma(gamma)
          .map { (block: LogisticBlock) =>
            val meat = Kernel.robustMeat(block.rows, block.gamma, beta)
            LogisticMeatPartial(block.blockId, meat.packed, meat.clusters)
          }(Encoders.product[LogisticMeatPartial])
          .collect()
          .sortBy(_.blockId)
        val meat = OrderedReduction.reduce(
          meatParts.toSeq.map(m => m.blockId -> m.packed),
          Moments.packedLength(p)
        )
        val caseMix = new Array[Double](m)
        val fixedBeta = new Array[Double](m)
        passes
          .withGamma(gamma)
          .map { (block: LogisticBlock) =>
            val v = Kernel.robustProviders(block.rows, block.gamma, beta, meat, covariance, xbar)
            LogisticRobustPartial(block.blockId, v.caseMix, v.fixedBeta)
          }(Encoders.product[LogisticRobustPartial])
          .collect()
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
      val auc = if (binaryOutcomes) Some(areaUnderCurve(passes.withGamma(gamma), beta)) else None
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
      if (!result.converged)
        warnings += s"the SerBIN fit did not converge in ${result.iterations} steps (criterion " +
          s"${result.criterion}, tol ${options.tol})"
      else if (result.shortened)
        warnings += s"the line search shortened the last step to ${result.lastStep} while the Newton " +
          s"beta step was ${result.lastNewtonBetaStep} (tol ${options.tol}); the estimates may not be " +
          "at the maximum"
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
        LayoutSummary.of(plan),
        summaries.iterator.map(_.fingerprint).sum,
        robustCovariance = robust.map(_._1),
        clusters = robust.map(_._2).getOrElse(0L),
        auc = auc
      )
    } finally workingSet.release()
  }

  /** The fitted providers as a table with [[ProviderColumns]]: m rows built on the driver, where the
    * fit holds them (specification §8 and §11).
    */
  def providerTable(spark: SparkSession, fit: LogisticFit): DataFrame = {
    val providers = fit.providers
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
        StructField("robust_var_fixed_beta", DoubleType, nullable = true)
      )
    )
    val rows = (0 until providers.size).map { k =>
      Row(
        GroupKey.value(providers.keys(k)),
        providers.records(k),
        providers.trials(k),
        providers.events(k),
        providers.gamma(k),
        providers.varGamma(k),
        providers.varCaseMix(k),
        providers.zeroEvents(k),
        providers.allEvents(k),
        providers.atBound(k),
        if (providers.robustVarCaseMix.isEmpty) null else providers.robustVarCaseMix(k),
        if (providers.robustVarFixedBeta.isEmpty) null else providers.robustVarFixedBeta(k)
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
    val plan = LayoutPlan.create(
      providers.keys.indices.map(k => providers.keys(k) -> providers.records(k)).toVector,
      fit.options.blocks.targetRowsPerBlock(p + 2)
    )
    val providersOfBlock = {
      val lists = Array.fill(plan.blockCount)(Array.newBuilder[Int])
      plan.placements.sortBy(_.groupIndex).foreach(g => lists(g.blockId) += g.groupIndex)
      lists.map(_.result())
    }
    val workingSet = LogisticWorkingSet.build(input, plan, fit.options.blocks.resolvedStorageLevel)
    try {
      val fingerprint = workingSet.blocks
        .map((block: LogisticBlock) => summarize(block))(Encoders.product[LogisticSummary])
        .collect()
        .iterator
        .map(_.fingerprint)
        .sum
      if (fingerprint != fit.fingerprint)
        throw new IllegalArgumentException(
          "the data differ from the fit's (fingerprint); covariate tests need the training data (API-3)"
        )
      val yBar = fit.events / fit.trials
      val start = Array.fill(m)(StrictMath.log(yBar / (1.0 - yBar)))
      val quantile = Normal.upperQuantile((1.0 - fit.options.confidenceLevel) / 2.0)
      tested.map { name =>
        val j = features.indexOf(name)
        val reduced = workingSet.blocks
          .map((block: LogisticBlock) => dropColumn(block, j))(Encoders.product[LogisticBlock])
          .persist(fit.options.blocks.resolvedStorageLevel)
        try {
          val passes = new SparkPasses(reduced, providersOfBlock, p - 1)
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
          val statistic =
            if (method == "lr") {
              val reducedBeta = refit.beta
              val sum = new NeumaierSum
              passes
                .withGamma(refit.gamma)
                .map((block: LogisticBlock) =>
                  LogisticLoglikPartial(
                    block.blockId,
                    Kernel.loglik(block.rows, block.gamma, reducedBeta)
                  )
                )(Encoders.product[LogisticLoglikPartial])
                .collect()
                .sortBy(_.blockId)
                .foreach(part => sum.add(part.loglik))
              2.0 * (fit.loglik - sum.value)
            } else {
              val full = Array.tabulate(p)(k =>
                if (k < j) refit.beta(k) else if (k == j) 0.0 else refit.beta(k - 1)
              )
              val parts = new SparkPasses(workingSet.blocks, providersOfBlock, p)
                .withGamma(refit.gamma)
                .map { (block: LogisticBlock) =>
                  val t = Kernel.scoreTerms(block.rows, block.gamma, full)
                  LogisticScorePartial(block.blockId, t.packed, t.score)
                }(Encoders.product[LogisticScorePartial])
                .collect()
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

  /** The rows of `df` with `linear_predictor` and `probability` (Phase 2b §4): γ̂ⱼ + xᵀβ̂ and its inverse
    * logit, with Spark's StrictMath-based `exp`. Rows of providers not in the fit fail with counts
    * (X-022).
    */
  def predict(df: DataFrame, fit: LogisticFit): DataFrame = {
    val spark = df.sparkSession
    val missing = (fit.spec.features :+ fit.spec.provider).filterNot(df.columns.contains)
    if (missing.nonEmpty) throw new InvalidInputException(missing.map(InputProblem.MissingColumn))
    val providers = fit.providers
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
    val unknown = joined.filter(col(gammaColumn).isNull).count()
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
    */
  private def areaUnderCurve(blocks: Dataset[LogisticBlock], beta: Array[Double]): Double = {
    val b = beta.clone()
    val parts = blocks
      .flatMap { (block: LogisticBlock) =>
        val pi = Kernel.fitted(block.rows, block.gamma, b)
        pi.indices.iterator.map(r => LogisticScore(pi(r), block.y(r) > 0.5))
      }(Encoders.product[LogisticScore])
      .orderBy("pi")
      .mapPartitions((scores: Iterator[LogisticScore]) => Iterator(aucPartial(scores)))(
        Encoders.product[LogisticAucPartial]
      )
      .collect()
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

  private def dropColumn(block: LogisticBlock, j: Int): LogisticBlock = {
    val p = block.p
    val rows = block.y.length
    val x = new Array[Double](rows * (p - 1))
    var r = 0
    while (r < rows) {
      var k = 0
      var c = 0
      while (k < p) {
        if (k != j) {
          x(r * (p - 1) + c) = block.x(r * p + k)
          c += 1
        }
        k += 1
      }
      r += 1
    }
    block.copy(p = p - 1, x = x)
  }

  private def summarize(block: LogisticBlock): LogisticSummary = {
    val p = block.p
    val providers = block.groupIndex.length
    val weighted = new NeumaierVector(p)
    val records = new Array[Long](providers)
    val providerEvents = new Array[Double](providers)
    val providerTrials = new Array[Double](providers)
    val values = new Array[Double](p + 2)
    var events = 0.0
    var trials = 0.0
    var fingerprint = 0L
    var maxOutcome = 0.0
    var zeroOutcomes = 0L
    var g = 0
    while (g < providers) {
      val until = if (g + 1 < providers) block.groupStart(g + 1) else block.rowId.length
      var r = block.groupStart(g)
      while (r < until) {
        records(g) += 1L
        providerEvents(g) += block.y(r)
        providerTrials(g) += block.n(r)
        events += block.y(r)
        trials += block.n(r)
        maxOutcome = math.max(maxOutcome, block.y(r))
        if (block.y(r) == 0.0) zeroOutcomes += 1L
        values(0) = block.y(r)
        values(1) = block.n(r)
        var j = 0
        while (j < p) {
          weighted.addAt(j, block.n(r) * block.x(r * p + j))
          values(j + 2) = block.x(r * p + j)
          j += 1
        }
        fingerprint += Fingerprint.row(block.groupIndex(g), block.rowId(r), values, 0, p + 2)
        r += 1
      }
      g += 1
    }
    val moments = Moments.centred(block.x, p, 0, block.rowId.length)
    LogisticSummary(
      block.blockId,
      block.rowId.length.toLong,
      events,
      trials,
      weighted.values,
      moments.mean,
      moments.comoments,
      fingerprint,
      records,
      providerEvents,
      providerTrials,
      maxOutcome,
      zeroOutcomes
    )
  }

  /** Passes A and B over the persisted blocks, γ shipped per block as a broadcast-joined table
    * (§6.7) and partials reduced in block order (§6.8).
    */
  private final class SparkPasses(
      blocks: Dataset[LogisticBlock],
      providersOfBlock: Array[Array[Int]],
      p: Int
  ) extends SerbinPasses {

    private val schema = StructType(
      Seq(
        StructField("blockId", IntegerType, nullable = false),
        StructField("gamma", ArrayType(DoubleType, containsNull = false), nullable = false)
      )
    )

    def withGamma(gamma: Array[Double]): Dataset[LogisticBlock] = {
      val rows = providersOfBlock.indices.map(b => Row(b, providersOfBlock(b).toSeq.map(gamma(_))))
      val frame = blocks.sparkSession.createDataFrame(rows.asJava, schema)
      blocks
        .drop("gamma")
        .join(broadcast(frame), Seq("blockId"))
        .as[LogisticBlock](Encoders.product[LogisticBlock])
    }

    def schur(gamma: Array[Double], beta: Array[Double]): Kernel.Schur = {
      val b = beta.clone()
      val parts = withGamma(gamma)
        .map { (block: LogisticBlock) =>
          val s = Kernel.schur(block.rows, block.gamma, b)
          LogisticSchurPartial(block.blockId, s.packed, s.rhs, s.scoreTerm, s.loglik)
        }(Encoders.product[LogisticSchurPartial])
        .collect()
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
      val parts = withGamma(gamma)
        .map { (block: LogisticBlock) =>
          val t = Kernel.trial(block.rows, block.gamma, b, d, v)
          LogisticTrialPartial(block.blockId, t.deltaGamma, t.loglik)
        }(Encoders.product[LogisticTrialPartial])
        .collect()
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
}
