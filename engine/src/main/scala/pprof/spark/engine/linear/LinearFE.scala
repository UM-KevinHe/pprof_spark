package pprof.spark.engine.linear

import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.functions.{array, col, lit, sqrt}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType}
import pprof.spark.engine.backend.{DriverGuards, PlanFrames}
import pprof.spark.engine.data.{InputProblem, InvalidInputException, Validation}
import pprof.spark.engine.layout.{GroupSizes, LayoutPlan}
import pprof.spark.engine.metadata.SoftwareInfo
import pprof.spark.engine.skeleton.LayoutSummary
import pprof.spark.numerics.{Cholesky, NeumaierSum, StudentT, Summation}
import pprof.spark.numerics.kernels.{Fingerprint, Moments, LinearFE => Kernel}

/** The linear fixed-effect model (docs/spec/linear/fixed-effect-estimation.md): the within estimator in two passes
  * over ProviderLocal blocks, partials reduced in block order (§6.8), and the provider table kept distributed.
  */
object LinearFE {

  val Alternatives: Seq[String] = Seq("two_sided", "greater", "less")

  def fit(df: DataFrame, spec: LinearSpec, options: LinearOptions = LinearOptions()): LinearFit = {
    val input = LinearValidation.validate(df, spec)
    val p = spec.features.size
    val sizes = GroupSizes.collectGroupSizes(input.frame, options.blocks).sortBy(_._1)
    val m = sizes.size
    val degrees = input.rowCount - m - p
    if (degrees < 1L)
      throw new InvalidInputException(
        Seq(InputProblem.NoResidualDegreesOfFreedom(input.rowCount, m.toLong, p))
      )
    val plan = LayoutPlan.create(sizes, options.blocks.targetRowsPerBlock(p + 1))
    DriverGuards.requireWithinBudget(
      s"${plan.blockCount} block partials",
      plan.blockCount.toLong * 8L * (Moments.packedLength(p) + p + 4L),
      options.blocks
    )
    val workingSet = LinearWorkingSet.build(input, plan, options.blocks.resolvedStorageLevel)
    try {
      val partials = workingSet.blocks
        .map((block: LinearBlock) => moments(block))(Encoders.product[LinearMomentPartial])
        .collect()
        .sortBy(_.blockId)
      val rows = partials.iterator.map(_.rows).sum
      if (rows != plan.rowCount)
        throw new IllegalStateException(
          s"the blocks hold $rows rows; the plan has ${plan.rowCount}"
        )
      val xx =
        Array.tabulate(Moments.packedLength(p))(k => Summation.pairwise(partials.map(_.xx(k))))
      val xy = Array.tabulate(p)(k => Summation.pairwise(partials.map(_.xy(k))))
      val mean = Summation.pairwise(partials.map(_.ySum)) / rows.toDouble
      val factor = Cholesky.factor(xx, p)
      if (!factor.isFullRank) throw new LinearAliasingException(factor.aliased.map(spec.features))
      val beta = factor.solve(xy)
      val v = factor.inversePacked
      val pivots = factor.pivots
      val condition = pivots.max / pivots.min
      val sums = workingSet.blocks
        .map((block: LinearBlock) => residuals(block, beta, v, mean))(
          Encoders.product[LinearResidualPartial]
        )
        .collect()
        .sortBy(_.blockId)
      val rss = Summation.pairwise(sums.map(_.rss))
      val tss = Summation.pairwise(sums.map(_.tss))
      val sigma2 = rss / degrees.toDouble
      val n = rows.toDouble
      val loglik = -0.5 * n * (StrictMath.log(2.0 * math.Pi) + StrictMath.log(rss / n) + 1.0)
      val parameters = (m + p + 1).toDouble
      val covariance = v.map(_ * sigma2)
      val coefficients = spec.features.indices.map { i =>
        LinearCoefficient(
          spec.features(i),
          beta(i),
          math.sqrt(covariance(Moments.packedIndex(i, i, p)))
        )
      }.toVector
      val warnings =
        if (rss == 0.0)
          Vector(
            "the within fit is exact (RSS = 0): sigma is 0, so t statistics are infinite (NN-10)"
          )
        else Vector.empty[String]
      LinearFit(
        coefficients,
        covariance.toVector,
        providerTable(workingSet, plan, input, options, beta, v, sigma2),
        math.sqrt(sigma2),
        rss,
        tss,
        rows,
        m,
        degrees,
        loglik,
        -2.0 * loglik + 2.0 * parameters,
        -2.0 * loglik + parameters * StrictMath.log(n),
        condition,
        warnings,
        spec,
        options,
        SoftwareInfo.capture(df.sparkSession),
        LayoutSummary.of(plan),
        partials.iterator.map(_.fingerprint).sum
      )
    } finally workingSet.release()
  }

  /** t tests and intervals for β on n − m − p degrees of freedom (§4); tails evaluated directly (X-031). */
  def summary(
      fit: LinearFit,
      level: Double = 0.95,
      nullValue: Double = 0.0,
      alternative: String = "two_sided"
  ): Vector[LinearTest] = {
    require(level > 0.0 && level < 1.0, s"level must lie strictly between 0 and 1, got $level")
    require(Alternatives.contains(alternative), s"unknown alternative $alternative")
    val df = fit.degreesOfFreedom.toDouble
    fit.coefficients.map { c =>
      val t = (c.estimate - nullValue) / c.standardError
      alternative match {
        case "two_sided" =>
          val q = StudentT.upperQuantile((1.0 - level) / 2.0, df)
          LinearTest(
            c.feature,
            c.estimate,
            c.standardError,
            t,
            StudentT.twoSided(t, df),
            c.estimate - q * c.standardError,
            c.estimate + q * c.standardError
          )
        case "greater" =>
          val q = StudentT.upperQuantile(1.0 - level, df)
          LinearTest(
            c.feature,
            c.estimate,
            c.standardError,
            t,
            StudentT.upperTail(t, df),
            c.estimate - q * c.standardError,
            Double.PositiveInfinity
          )
        case _ =>
          val q = StudentT.upperQuantile(1.0 - level, df)
          LinearTest(
            c.feature,
            c.estimate,
            c.standardError,
            t,
            StudentT.cdf(t, df),
            Double.NegativeInfinity,
            c.estimate + q * c.standardError
          )
      }
    }
  }

  /** `df` with `prediction` = γ̂ⱼ + xᵀβ̂; rows of providers not in the fit fail with their count. */
  def predict(df: DataFrame, fit: LinearFit): DataFrame = {
    val spec = fit.spec
    val input = LinearValidation.validate(df, spec, outcomeRequired = false)
    val text = requireKeyType(df, fit, input)
    val key = "__pprof_key"
    val effect = "__pprof_gamma"
    val effects = fit.providers.select(col(spec.provider).as(key), col("gamma").as(effect))
    val provider = Validation.column(spec.provider)
    val joined = df.join(
      effects,
      (if (text) provider.cast(StringType) else provider.cast(LongType)) === col(key),
      "left"
    )
    val unknown = joined.filter(col(effect).isNull).count()
    if (unknown > 0)
      throw new InvalidInputException(Seq(InputProblem.UnknownProviders(spec.provider, unknown)))
    val prediction = spec.features.indices.foldLeft(col(effect)) { (sum, i) =>
      sum + Validation.column(spec.features(i)).cast(DoubleType) * lit(fit.coefficients(i).estimate)
    }
    joined.withColumn("prediction", prediction).drop(key, effect)
  }

  /** R² of the fit on `df`: 1 − Σ(y − ŷ)² / Σ(y − ȳ)², pprof_py's `score`, for any provider key type (X-034). Each
    * row's residual is a column expression; the scored rows then pass through ProviderLocal blocks in canonical
    * order and the sums are reduced in block order, so the result does not depend on the input's partitioning
    * (NN-4). Rows of providers not in the fit fail with their count.
    */
  def score(df: DataFrame, fit: LinearFit): Double = {
    val spec = fit.spec
    val input = LinearValidation.validate(df, spec)
    requireKeyType(df, fit, input)
    val key = "__pprof_key"
    val effect = "__pprof_gamma"
    val effects = fit.providers.select(col(spec.provider).as(key), col("gamma").as(effect))
    val joined = input.frame.join(effects, col(Validation.GroupColumn) === col(key), "left")
    val unknown = joined.filter(col(effect).isNull).count()
    if (unknown > 0)
      throw new InvalidInputException(Seq(InputProblem.UnknownProviders(spec.provider, unknown)))
    val features = col(Validation.FeaturesColumn)
    val prediction = spec.features.indices.foldLeft(col(effect)) { (sum, i) =>
      sum + features.getItem(i) * lit(fit.coefficients(i).estimate)
    }
    val outcome = col(LinearValidation.OutcomeColumn)
    val residuals = joined.select(
      col(Validation.GroupColumn),
      outcome,
      array(outcome - prediction).as(Validation.FeaturesColumn),
      col(Validation.RowIdColumn)
    )
    val scored = LinearInput(
      residuals,
      spec.copy(features = Seq("residual")),
      input.rowCount,
      input.providerKeyIsText
    )
    val sizes = GroupSizes.collectGroupSizes(residuals, fit.options.blocks).sortBy(_._1)
    val plan = LayoutPlan.create(sizes, fit.options.blocks.targetRowsPerBlock(2))
    val workingSet = LinearWorkingSet.build(scored, plan, fit.options.blocks.resolvedStorageLevel)
    try {
      val first = workingSet.blocks
        .map((block: LinearBlock) => scoreSums(block))(Encoders.product[LinearScorePartial])
        .collect()
        .sortBy(_.blockId)
      val rows = first.iterator.map(_.rows).sum.toDouble
      val mean = Summation.pairwise(first.map(_.ySum)) / rows
      val totals = workingSet.blocks
        .map((block: LinearBlock) => LinearTotalPartial(block.blockId, totalSquares(block, mean)))(
          Encoders.product[LinearTotalPartial]
        )
        .collect()
        .sortBy(_.blockId)
      1.0 - Summation.pairwise(first.map(_.sse)) / Summation.pairwise(totals.map(_.tss))
    } finally workingSet.release()
  }

  /** The provider key's type must match the fit's; returns whether it is text. */
  private def requireKeyType(df: DataFrame, fit: LinearFit, input: LinearInput): Boolean = {
    val spec = fit.spec
    val text = fit.providers.schema(spec.provider).dataType == StringType
    if (text != input.providerKeyIsText)
      throw new InvalidInputException(
        Seq(
          InputProblem.UnsupportedType(
            spec.provider,
            df.schema(spec.provider).dataType.simpleString,
            if (text) "a string type, as in the fit" else "an integral type, as in the fit"
          )
        )
      )
    text
  }

  /** Σ residual² and Σ y over a block of scored rows (values: residual, then y), in canonical order. */
  private def scoreSums(block: LinearBlock): LinearScorePartial = {
    val sse = new NeumaierSum
    val ySum = new NeumaierSum
    val rows = block.rowId.length
    var r = 0
    while (r < rows) {
      val residual = block.values(2 * r)
      sse.add(residual * residual)
      ySum.add(block.values(2 * r + 1))
      r += 1
    }
    LinearScorePartial(block.blockId, rows.toLong, sse.value, ySum.value)
  }

  private def totalSquares(block: LinearBlock, mean: Double): Double = {
    val tss = new NeumaierSum
    var r = 0
    while (r < block.rowId.length) {
      val d = block.values(2 * r + 1) - mean
      tss.add(d * d)
      r += 1
    }
    tss.value
  }

  private def moments(block: LinearBlock): LinearMomentPartial = {
    val within = Kernel.within(block.rows)
    val q = block.p + 1
    val values = new Array[Double](q)
    var fingerprint = 0L
    var g = 0
    while (g < block.groupIndex.length) {
      var r = block.groupStart(g)
      val until = block.rows.until(g)
      while (r < until) {
        System.arraycopy(block.values, r * q, values, 0, q)
        fingerprint += Fingerprint.row(block.groupIndex(g), block.rowId(r), values, 0, q)
        r += 1
      }
      g += 1
    }
    LinearMomentPartial(block.blockId, within.rows, within.xx, within.xy, within.ySum, fingerprint)
  }

  private def residuals(block: LinearBlock, beta: Array[Double], v: Array[Double], mean: Double) = {
    val effects = Kernel.providers(block.rows, beta, v).map(_.gamma)
    val sums = Kernel.sums(block.rows, beta, effects, mean)
    LinearResidualPartial(block.blockId, sums.rss, sums.tss)
  }

  /** The provider table: effects and variance factors per block, keyed through the plan, persisted (§7, step 4). */
  private def providerTable(
      workingSet: LinearWorkingSet,
      plan: LayoutPlan,
      input: LinearInput,
      options: LinearOptions,
      beta: Array[Double],
      v: Array[Double],
      sigma2: Double
  ): DataFrame = {
    val spark = input.frame.sparkSession
    val spec = input.spec
    val rows = workingSet.blocks.flatMap { (block: LinearBlock) =>
      val providers = Kernel.providers(block.rows, beta, v)
      providers.indices.iterator.map(g =>
        LinearProviderRow(
          block.groupIndex(g),
          providers(g).rows,
          providers(g).gamma,
          providers(g).q
        )
      )
    }(Encoders.product[LinearProviderRow])
    val keys = PlanFrames
      .placements(spark, plan, input.providerKeyIsText)
      .select(
        col(PlanFrames.GroupIndexColumn).as("groupIndex"),
        col(Validation.GroupColumn).as(spec.provider)
      )
    val variance =
      if (options.varianceOption == "complete") col("q") * lit(sigma2)
      else lit(sigma2) / col("records").cast(DoubleType)
    val table = rows
      .toDF()
      .join(keys, Seq("groupIndex"))
      .orderBy("groupIndex")
      .select(
        col(spec.provider),
        col("records"),
        col("gamma"),
        variance.as("variance"),
        sqrt(variance).as("se")
      )
      .persist(options.blocks.resolvedStorageLevel)
    table.count()
    table
  }
}
