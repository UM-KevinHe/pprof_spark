package pprof.spark.engine.linear

import org.apache.spark.sql.{Column, DataFrame, Encoders}
import org.apache.spark.sql.functions.{
  array,
  coalesce,
  col,
  count,
  countDistinct,
  lit,
  sqrt,
  sum,
  when
}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, NumericType, StringType}
import pprof.spark.engine.backend.{BlockOptions, DriverGuards, PlanFrames}
import pprof.spark.engine.data.{InputProblem, InvalidInputException, Validation}
import pprof.spark.engine.layout.{BudgetedLayout, GroupSizes, LayoutPlan}
import pprof.spark.engine.metadata.SoftwareInfo
import pprof.spark.engine.skeleton.LayoutSummary
import pprof.spark.numerics.Summation
import pprof.spark.numerics.kernels.{Moments, LinearRE => Kernel}

/** Column roles of the linear random-intercept model (docs/spec/linear/random-effect-estimation.md §5). */
final case class LinearRESpec(
    outcome: String,
    features: Seq[String],
    provider: String,
    offset: Option[String] = None,
    weights: Option[String] = None,
    rowId: Option[String] = None
)

/** The criterion (REML by default), the intercept, θ's upper bound and the search's evaluation cap (§3, §6). */
final case class LinearREOptions(
    reml: Boolean = true,
    includeIntercept: Boolean = true,
    thetaUpper: Double = Double.PositiveInfinity,
    maxEvaluations: Int = 200,
    blocks: BlockOptions = BlockOptions()
) {
  require(thetaUpper > 0.0, s"thetaUpper must be positive, got $thetaUpper")
  require(
    maxEvaluations >= 20,
    s"maxEvaluations must be at least 20, the grid's size, got $maxEvaluations"
  )
}

/** The search's record (§3): evaluations, the root's bracket, |d′(τ̂)| and the boundary, if any. */
final case class LinearRESearch(
    evaluations: Int,
    lower: Double,
    upper: Double,
    slope: Double,
    boundary: Option[String]
)

/** The fitted model (§4, §8). `covariance` is Var(β̂), packed. `providers` is the distributed provider table: the
  * key, `records`, `weight`, `blup`, and `variance` and `se`, the BLUP's conditional variance and SD (X-037).
  */
final case class LinearREFit(
    coefficients: Vector[LinearCoefficient],
    covariance: Vector[Double],
    providers: DataFrame,
    sigma: Double,
    theta: Double,
    sigmaAlpha: Double,
    criterion: String,
    rows: Long,
    providerCount: Int,
    deviance: Double,
    loglik: Double,
    aic: Double,
    bic: Double,
    pwrss: Double,
    logDetH: Double,
    ussq: Double,
    search: LinearRESearch,
    warnings: Vector[String],
    spec: LinearRESpec,
    options: LinearREOptions,
    software: SoftwareInfo,
    layout: LayoutSummary,
    fingerprint: Long,
    status: String = "experimental"
) {
  def estimates: Vector[Double] = coefficients.map(_.estimate)
  def standardErrors: Vector[Double] = coefficients.map(_.standardError)
}

/** Aliased features (X-039): the weighted cross-product X′WX of the full design is singular. */
final class LinearREAliasingException(val features: Seq[String])
    extends IllegalArgumentException(
      s"the features ${features.mkString(", ")} are aliased: collinear with the other features or the intercept " +
        "(X-039)"
    )

/** Pass 1's partial of one block: its totals and its providers' summaries, in the block's provider order. */
final case class LinearREMomentPartial(
    blockId: Int,
    rows: Long,
    logWeights: Double,
    xx: Array[Double],
    xy: Array[Double],
    fingerprint: Long,
    groupIndex: Array[Int],
    records: Array[Long],
    weights: Array[Double],
    xMeans: Array[Double],
    yMeans: Array[Double]
)

/** One provider of the table: its BLUP and the BLUP's conditional variance. */
final case class LinearREProviderRow(
    groupIndex: Int,
    records: Long,
    weight: Double,
    blup: Double,
    variance: Double
)

/** Validation once over every input row, every problem reported at once, with counts and never values (§5, T6). */
object LinearREValidation {

  val InterceptName: String = "(Intercept)"
  private val WeightName = "__pprof_weight"

  def validate(df: DataFrame, spec: LinearRESpec, options: LinearREOptions): LinearInput = {
    val types = df.schema.fields.map(f => f.name -> f.dataType).toMap
    val structural = schemaProblems(types, spec)
    if (structural.nonEmpty) throw new InvalidInputException(structural)
    val aggregates = valueCounts(types, spec).zipWithIndex.map { case (c, i) => c.as(s"count_$i") }
    val counts = df.agg(aggregates.head, aggregates.tail: _*).collect().head
    var position = 0
    def next(): Long = {
      val value = counts.getLong(position)
      position += 1
      value
    }
    val rows = next()
    val problems = Vector.newBuilder[InputProblem]
    def values(name: String): Unit = {
      val nulls = next()
      val nonFinite = next()
      if (nulls > 0 || nonFinite > 0) problems += InputProblem.InvalidValues(name, nulls, nonFinite)
    }
    values(spec.outcome)
    spec.features.foreach(values)
    val providerNulls = next()
    if (providerNulls > 0) problems += InputProblem.InvalidValues(spec.provider, providerNulls, 0L)
    spec.offset.foreach(values)
    spec.weights.foreach { name =>
      values(name)
      val nonPositive = next()
      if (nonPositive > 0) problems += InputProblem.NonPositiveValues(name, nonPositive)
    }
    spec.rowId.foreach { name =>
      val nulls = next()
      val duplicates = (rows - nulls) - next()
      if (nulls > 0) problems += InputProblem.InvalidValues(name, nulls, 0L)
      if (duplicates > 0) problems += InputProblem.DuplicateRowIds(name, duplicates)
    }
    if (rows == 0) problems += InputProblem.EmptyInput
    val found = problems.result()
    if (found.nonEmpty) throw new InvalidInputException(found)
    val text = types(spec.provider) == StringType
    val provider = Validation.column(spec.provider)
    val outcome = spec.offset.foldLeft(Validation.column(spec.outcome).cast(DoubleType)) { (y, o) =>
      y - Validation.column(o).cast(DoubleType)
    }
    val x = (if (options.includeIntercept) Seq(lit(1.0)) else Seq.empty[Column]) ++
      spec.features.map(name => Validation.column(name).cast(DoubleType)) :+
      spec.weights.fold(lit(1.0))(name => Validation.column(name).cast(DoubleType))
    val frame = df.select(
      (if (text) provider else provider.cast(LongType)).as(Validation.GroupColumn),
      outcome.as(LinearValidation.OutcomeColumn),
      array(x: _*).as(Validation.FeaturesColumn),
      spec.rowId
        .fold(lit(0L))(name => Validation.column(name).cast(LongType))
        .as(Validation.RowIdColumn)
    )
    val names =
      (if (options.includeIntercept) Seq(InterceptName) else Seq.empty[String]) ++ spec.features :+
        WeightName
    LinearInput(frame, LinearSpec(spec.outcome, names, spec.provider, spec.rowId), rows, text)
  }

  private def schemaProblems(
      types: Map[String, DataType],
      spec: LinearRESpec
  ): Seq[InputProblem] = {
    def check(name: String, accepted: DataType => Boolean, expected: String): Option[InputProblem] =
      types.get(name) match {
        case None                            => Some(InputProblem.MissingColumn(name))
        case Some(found) if !accepted(found) =>
          Some(InputProblem.UnsupportedType(name, found.simpleString, expected))
        case Some(_) => None
      }
    val numeric = (t: DataType) => t.isInstanceOf[NumericType]
    check(spec.outcome, numeric, "a numeric type").toSeq ++
      check(
        spec.provider,
        t => Validation.isIntegral(t) || t == StringType,
        "an integral or string type"
      ).toSeq ++
      (spec.features ++ spec.offset ++ spec.weights).flatMap(name =>
        check(name, numeric, "a numeric type")
      ) ++
      spec.rowId.toSeq.flatMap(name => check(name, Validation.isIntegral, "an integral type"))
  }

  private def valueCounts(types: Map[String, DataType], spec: LinearRESpec): Seq[Column] = {
    def countWhere(condition: Column): Column =
      coalesce(sum(when(condition, 1L).otherwise(0L)), lit(0L))
    def invalid(name: String): Seq[Column] = {
      val value = Validation.column(name)
      Seq(countWhere(value.isNull), countWhere(Validation.nonFinite(value, types(name))))
    }
    Seq(count(lit(1))) ++ invalid(spec.outcome) ++ spec.features.flatMap(invalid) ++
      Seq(countWhere(Validation.column(spec.provider).isNull)) ++ spec.offset.toSeq.flatMap(
        invalid
      ) ++
      spec.weights.toSeq.flatMap(name =>
        invalid(name) :+ countWhere(Validation.column(name) <= lit(0.0))
      ) ++
      spec.rowId.toSeq.flatMap { name =>
        val value = Validation.column(name)
        Seq(countWhere(value.isNull), countDistinct(value))
      }
  }
}

/** The linear random-intercept model (docs/spec/linear/random-effect-estimation.md): two passes over ProviderLocal
  * blocks, partials reduced in block order (§6.8), the search of §3 on the driver (OI-95), and the provider table
  * kept distributed.
  */
object LinearRE {

  def fit(
      df: DataFrame,
      spec: LinearRESpec,
      options: LinearREOptions = LinearREOptions()
  ): LinearREFit = {
    val input = LinearREValidation.validate(df, spec, options)
    val p =
      input.spec.features.size - 1 // the kernel's features: the intercept, then the spec's features
    val sizes = GroupSizes.collectGroupSizes(input.frame, options.blocks).sortBy(_._1)
    val m = sizes.size
    val design = Vector.newBuilder[InputProblem]
    if (m < 2) design += InputProblem.TooFewProviders(spec.provider, m.toLong)
    if (m.toLong >= input.rowCount)
      design += InputProblem.ProvidersNotFewerThanRows(spec.provider, m.toLong, input.rowCount)
    if (input.rowCount <= p.toLong) design += InputProblem.TooFewRows(input.rowCount, p)
    val problems = design.result()
    if (problems.nonEmpty) throw new InvalidInputException(problems)
    val partialBytes = 8L * (Moments.packedLength(p) + p + 6L)
    val plan = BudgetedLayout.plan(sizes, options.blocks, p + 2, partialBytes)
    DriverGuards.requireWithinBudget(
      s"${plan.blockCount} block partials",
      plan.blockCount.toLong * partialBytes,
      options.blocks
    )
    DriverGuards.requireWithinBudget(
      s"the summaries of $m providers, which the search reads on the driver (OI-95),",
      8L * m.toLong * (p + 4L),
      options.blocks
    )
    val workingSet = LinearWorkingSet.build(input, plan, options.blocks.resolvedStorageLevel)
    try {
      val partials = workingSet.blocks
        .map((block: LinearBlock) => moments(block))(Encoders.product[LinearREMomentPartial])
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
      val providers = partials
        .flatMap(partial =>
          partial.groupIndex.indices.map(g => (partial.groupIndex(g), partial, g))
        )
        .sortBy(_._1)
      val records = providers.map { case (_, partial, g) => partial.records(g) }
      val weights = providers.map { case (_, partial, g) => partial.weights(g) }
      val xMeans = providers.flatMap { case (_, partial, g) =>
        partial.xMeans.slice(g * p, (g + 1) * p)
      }
      val yMeans = providers.map { case (_, partial, g) => partial.yMeans(g) }
      val betaWithin = Kernel.withinSolution(xx, xy, p)
      val within = workingSet.blocks
        .map((block: LinearBlock) =>
          LinearResidualPartial(block.blockId, withinResiduals(block, betaWithin), 0.0)
        )(
          Encoders.product[LinearResidualPartial]
        )
        .collect()
        .sortBy(_.blockId)
      val data = Kernel.Data(
        records,
        weights,
        xMeans,
        yMeans,
        p,
        options.includeIntercept,
        rows,
        Summation.pairwise(partials.map(_.logWeights)),
        xx,
        xy,
        betaWithin,
        Summation.pairwise(within.map(_.rss))
      )
      val names = input.spec.features.take(p)
      val aliased = Kernel.aliased(data)
      if (aliased.nonEmpty) throw new LinearREAliasingException(aliased.map(names))
      val result = Kernel.fit(data, options.reml, options.thetaUpper, options.maxEvaluations)
      val coefficients = names.indices.map { i =>
        LinearCoefficient(
          names(i),
          result.beta(i),
          math.sqrt(result.covariance(Moments.packedIndex(i, i, p)))
        )
      }.toVector
      val search = result.search
      LinearREFit(
        coefficients,
        result.covariance.toVector,
        providerTable(input, plan, options, providers.map(_._1), records, weights, result),
        result.sigma,
        result.theta,
        result.sigmaAlpha,
        if (options.reml) "REML" else "ML",
        rows,
        m,
        result.deviance,
        result.logLikelihood,
        result.aic,
        result.bic,
        result.pwrss,
        result.logDetH,
        result.ussq,
        LinearRESearch(
          search.evaluations,
          search.lower,
          search.upper,
          search.slope,
          search.boundary
        ),
        search.warnings,
        spec,
        options,
        SoftwareInfo.capture(df.sparkSession),
        LayoutSummary.of(plan),
        partials.iterator.map(_.fingerprint).sum
      )
    } finally workingSet.release()
  }

  /** `df` with `prediction`: the fixed part xᵀβ̂, plus the offset column when the fit has one and `withOffset`
    * (pprof_py adds it only when `offset_var` is passed), plus with `useRe` the BLUP of providers in the fit and 0
    * for others, as both references (§6).
    */
  def predict(
      df: DataFrame,
      fit: LinearREFit,
      useRe: Boolean = false,
      withOffset: Boolean = true
  ): DataFrame = {
    val (frame, fixed) = linearPart(df, fit, withOffset, providerNeeded = useRe)
    if (!useRe) frame.withColumn("prediction", fixed)
    else {
      val (joined, blup) = joinBlups(frame, fit)
      joined.withColumn("prediction", fixed + blup).drop(Key, Effect)
    }
  }

  /** The training rows with `xbeta` (offset + xᵀβ̂), `fitted` (with the BLUP) and `residual` (y − fitted) (§4). */
  def fitted(df: DataFrame, fit: LinearREFit): DataFrame = {
    val (frame, fixed) = linearPart(df, fit, withOffset = true, providerNeeded = true)
    val (joined, blup) = joinBlups(frame, fit)
    joined
      .withColumn("xbeta", fixed)
      .withColumn("fitted", fixed + blup)
      .withColumn("residual", Validation.column(fit.spec.outcome).cast(DoubleType) - (fixed + blup))
      .drop(Key, Effect)
  }

  private val Key = "__pprof_key"
  private val Effect = "__pprof_blup"

  /** Checks the columns a prediction reads (features, the offset, the provider) and builds xᵀβ̂ (+ offset). */
  private def linearPart(
      df: DataFrame,
      fit: LinearREFit,
      withOffset: Boolean,
      providerNeeded: Boolean
  ): (DataFrame, Column) = {
    val spec = fit.spec
    val offset = if (withOffset) spec.offset else None
    val types = df.schema.fields.map(f => f.name -> f.dataType).toMap
    val numeric = spec.features ++ offset
    val structural = numeric.flatMap { name =>
      types.get(name) match {
        case None => Some(InputProblem.MissingColumn(name))
        case Some(found) if !found.isInstanceOf[NumericType] =>
          Some(InputProblem.UnsupportedType(name, found.simpleString, "a numeric type"))
        case Some(_) => None
      }
    } ++ (if (providerNeeded && !types.contains(spec.provider))
            Seq(InputProblem.MissingColumn(spec.provider))
          else Nil)
    if (structural.nonEmpty) throw new InvalidInputException(structural)
    if (numeric.nonEmpty) {
      def countWhere(condition: Column): Column =
        coalesce(sum(when(condition, 1L).otherwise(0L)), lit(0L))
      val checks = numeric.flatMap { name =>
        val value = Validation.column(name)
        Seq(countWhere(value.isNull), countWhere(Validation.nonFinite(value, types(name))))
      }
      val counts = df.agg(checks.head, checks.tail: _*).collect().head
      val problems = numeric.indices.flatMap { k =>
        val (nulls, nonFinite) = (counts.getLong(2 * k), counts.getLong(2 * k + 1))
        if (nulls > 0 || nonFinite > 0)
          Some(InputProblem.InvalidValues(numeric(k), nulls, nonFinite))
        else None
      }
      if (problems.nonEmpty) throw new InvalidInputException(problems)
    }
    val estimates = fit.coefficients.map(c => c.feature -> c.estimate).toMap
    val start = estimates.get(LinearREValidation.InterceptName).fold(lit(0.0))(lit(_))
    val linear = spec.features.foldLeft(start) { (total, name) =>
      total + Validation.column(name).cast(DoubleType) * lit(estimates(name))
    }
    (df, offset.fold(linear)(name => linear + Validation.column(name).cast(DoubleType)))
  }

  /** The frame left-joined with the provider table's BLUPs: 0 for providers not in the fit. */
  private def joinBlups(frame: DataFrame, fit: LinearREFit): (DataFrame, Column) = {
    val spec = fit.spec
    val text = fit.providers.schema(spec.provider).dataType == StringType
    val found = frame.schema(spec.provider).dataType
    if ((found == StringType) != text || !(found == StringType || Validation.isIntegral(found)))
      throw new InvalidInputException(
        Seq(
          InputProblem.UnsupportedType(
            spec.provider,
            found.simpleString,
            if (text) "a string type, as in the fit" else "an integral type, as in the fit"
          )
        )
      )
    val provider = Validation.column(spec.provider)
    val effects = fit.providers.select(col(spec.provider).as(Key), col("blup").as(Effect))
    val joined = frame.join(
      effects,
      (if (text) provider.cast(StringType) else provider.cast(LongType)) === col(Key),
      "left"
    )
    (joined, coalesce(col(Effect), lit(0.0)))
  }

  /** A block's rows in the kernel's layout: the features, ỹ, then w (the working set holds w among the features). */
  private def kernelRows(block: LinearBlock): Kernel.Rows = {
    val p = block.p - 1
    val q = block.p + 1
    val count = block.rowId.length
    val values = new Array[Double](count * (p + 2))
    var r = 0
    while (r < count) {
      System.arraycopy(block.values, r * q, values, r * (p + 2), p)
      values(r * (p + 2) + p) = block.values(r * q + p + 1)
      values(r * (p + 2) + p + 1) = block.values(r * q + p)
      r += 1
    }
    Kernel.Rows(block.groupStart, values, p)
  }

  private def moments(block: LinearBlock): LinearREMomentPartial = {
    val (providers, within) = Kernel.summaries(kernelRows(block))
    LinearREMomentPartial(
      block.blockId,
      within.rows,
      within.logWeights,
      within.xx,
      within.xy,
      LinearFE.fingerprintOf(block),
      block.groupIndex,
      providers.map(_.rows),
      providers.map(_.weight),
      providers.flatMap(_.xMean),
      providers.map(_.yMean)
    )
  }

  private def withinResiduals(block: LinearBlock, betaWithin: Array[Double]): Double = {
    val rows = kernelRows(block)
    Kernel.withinResiduals(rows, Kernel.summaries(rows)._1, betaWithin)
  }

  /** The provider table: BLUPs and conditional variances from the driver, keyed through the plan, persisted. */
  private def providerTable(
      input: LinearInput,
      plan: LayoutPlan,
      options: LinearREOptions,
      groups: Array[Int],
      records: Array[Long],
      weights: Array[Double],
      result: Kernel.Fit
  ): DataFrame = {
    val spark = input.frame.sparkSession
    val rows = groups.indices.map { j =>
      LinearREProviderRow(
        groups(j),
        records(j),
        weights(j),
        result.blups(j),
        result.conditionalVariances(j)
      )
    }
    val keys = PlanFrames
      .placements(spark, plan, input.providerKeyIsText)
      .select(
        col(PlanFrames.GroupIndexColumn).as("groupIndex"),
        col(Validation.GroupColumn).as(input.spec.provider)
      )
    val table = spark
      .createDataset(rows)(Encoders.product[LinearREProviderRow])
      .toDF()
      .join(keys, Seq("groupIndex"))
      .orderBy("groupIndex")
      .select(
        col(input.spec.provider),
        col("records"),
        col("weight"),
        col("blup"),
        col("variance"),
        sqrt(col("variance")).as("se")
      )
      .persist(options.blocks.resolvedStorageLevel)
    table.count()
    table
  }
}
