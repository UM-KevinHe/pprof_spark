package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Column, DataFrame, Encoders, Row}
import org.apache.spark.sql.functions.{
  array,
  coalesce,
  col,
  count,
  countDistinct,
  exp,
  lit,
  log,
  sum,
  when
}
import org.apache.spark.sql.types.{
  DataType,
  DoubleType,
  LongType,
  NumericType,
  StringType,
  StructField,
  StructType
}
import pprof.spark.engine.backend.{BlockOptions, DriverGuards, PlanFrames}
import pprof.spark.engine.data.{InputProblem, InvalidInputException, Validation}
import pprof.spark.engine.layout.{BudgetedLayout, GroupKey, GroupSizes, LayoutPlan}
import pprof.spark.engine.linear.{
  LinearBlock,
  LinearFE,
  LinearInput,
  LinearSpec,
  LinearValidation,
  LinearWorkingSet
}
import pprof.spark.engine.metadata.SoftwareInfo
import pprof.spark.engine.skeleton.LayoutSummary
import pprof.spark.numerics.{Cholesky, NeumaierSum, NeumaierVector, Summation}
import pprof.spark.numerics.kernels.{LogisticRE => Kernel, Moments}

/** Column roles of the logistic random-intercept model (docs/spec/logistic/random-effect-estimation.md §5). */
final case class LogisticRESpec(
    outcome: String,
    features: Seq[String],
    provider: String,
    offset: Option[String] = None,
    rowId: Option[String] = None
)

/** Options (§6, §9): `nagq1` is pprof_py's `stage2`; σ ≤ `sigmaUpper`; `maxIterations` caps the quasi-Newton. */
final case class LogisticREOptions(
    nagq1: Boolean = true,
    includeIntercept: Boolean = true,
    sigmaUpper: Double = Double.PositiveInfinity,
    maxIterations: Int = 200,
    blocks: BlockOptions = BlockOptions()
) {
  require(sigmaUpper > 0.0, s"sigmaUpper must be positive, got $sigmaUpper")
  require(maxIterations >= 20, s"maxIterations must be at least 20, got $maxIterations")
}

final case class LogisticRECoefficient(feature: String, estimate: Double, standardError: Double)

/** The search's record: evaluations of each stage, the final projected gradient, convergence and the boundary. */
final case class LogisticRESearch(
    stage1Evaluations: Int,
    stage2Evaluations: Int,
    projectedGradient: Double,
    converged: Boolean,
    boundary: Option[String]
)

/** A fitted logistic random-intercept model (§4). `providers` holds the key, `records`, `weight` (Wⱼ at the modes),
  * `blup`, `variance` (σ̂²/hⱼ) and `se`, ordered by key; `covariance` is Var(β̂) packed.
  */
final case class LogisticREFit(
    coefficients: Vector[LogisticRECoefficient],
    covariance: Vector[Double],
    providers: DataFrame,
    sigma: Double,
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
    search: LogisticRESearch,
    warnings: Vector[String],
    spec: LogisticRESpec,
    options: LogisticREOptions,
    software: SoftwareInfo,
    layout: LayoutSummary,
    fingerprint: Long,
    status: String = "Experimental"
) {
  def estimates: Vector[Double] = coefficients.map(_.estimate)
  def standardErrors: Vector[Double] = coefficients.map(_.standardError)
}

final class LogisticREAliasingException(val features: Seq[String])
    extends IllegalArgumentException(
      s"the fixed effects are aliased: ${features.mkString(", ")} (X-039's rule, spec 4b-1 §5)"
    )

final case class LogisticREFirstPartial(
    blockId: Int,
    rows: Long,
    xSum: Array[Double],
    ySum: Double,
    fingerprint: Long
)
final case class LogisticRECrossPartial(blockId: Int, xx: Array[Double])
final case class LogisticRELaplacePartial(blockId: Int, deviance: Double, gradient: Array[Double])
final case class LogisticREStage1Partial(
    blockId: Int,
    groupIndex: Array[Int],
    weights: Array[Double],
    wx: Array[Double],
    residuals: Array[Double],
    xwx: Array[Double],
    rx: Array[Double],
    penalized: Double,
    logH: Double
)
final case class LogisticREModesPartial(
    blockId: Int,
    groupIndex: Array[Int],
    records: Array[Long],
    modes: Array[Double],
    weights: Array[Double]
)

/** Validation (§5): every role checked at once; the working set's features are the intercept's constant, the
  * features and the offset (0 without one), and its outcome is y ∈ [0, 1].
  */
object LogisticREValidation {

  val InterceptName: String = "(Intercept)"
  private val OffsetName = "__pprof_offset"

  def validate(df: DataFrame, spec: LogisticRESpec, options: LogisticREOptions): LinearInput = {
    val types = df.schema.fields.map(f => f.name -> f.dataType).toMap
    def check(name: String, accepted: DataType => Boolean, expected: String): Option[InputProblem] =
      types.get(name) match {
        case None                            => Some(InputProblem.MissingColumn(name))
        case Some(found) if !accepted(found) =>
          Some(InputProblem.UnsupportedType(name, found.simpleString, expected))
        case Some(_) => None
      }
    val numeric = (t: DataType) => t.isInstanceOf[NumericType]
    val structural = check(spec.outcome, numeric, "a numeric type").toSeq ++
      check(
        spec.provider,
        t => Validation.isIntegral(t) || t == StringType,
        "an integral or string type"
      ).toSeq ++
      (spec.features ++ spec.offset).flatMap(name => check(name, numeric, "a numeric type")) ++
      spec.rowId.toSeq.flatMap(name => check(name, Validation.isIntegral, "an integral type"))
    if (structural.nonEmpty) throw new InvalidInputException(structural)
    def countWhere(condition: Column): Column =
      coalesce(sum(when(condition, 1L).otherwise(0L)), lit(0L))
    def invalid(name: String): Seq[Column] = {
      val value = Validation.column(name)
      Seq(countWhere(value.isNull), countWhere(Validation.nonFinite(value, types(name))))
    }
    val y = Validation.column(spec.outcome).cast(DoubleType)
    val aggregates = Seq(count(lit(1))) ++ invalid(spec.outcome) ++ Seq(
      countWhere(y < lit(0.0) || y > lit(1.0))
    ) ++
      spec.features.flatMap(invalid) ++ Seq(countWhere(Validation.column(spec.provider).isNull)) ++
      spec.offset.toSeq.flatMap(invalid) ++
      spec.rowId.toSeq.flatMap { name =>
        val value = Validation.column(name)
        Seq(countWhere(value.isNull), countDistinct(value))
      }
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
    val outside = next()
    if (outside > 0) problems += InputProblem.OutsideUnitInterval(spec.outcome, outside)
    spec.features.foreach(values)
    val providerNulls = next()
    if (providerNulls > 0) problems += InputProblem.InvalidValues(spec.provider, providerNulls, 0L)
    spec.offset.foreach(values)
    spec.rowId.foreach { name =>
      val nulls = next()
      val duplicates = (rows - nulls) - next()
      if (nulls > 0) problems += InputProblem.InvalidValues(name, nulls, 0L)
      if (duplicates > 0) problems += InputProblem.DuplicateRowIds(name, duplicates)
    }
    if (rows == 0) problems += InputProblem.EmptyInput
    val listed = problems.result()
    if (listed.nonEmpty) throw new InvalidInputException(listed)
    val text = types(spec.provider) == StringType
    val provider = Validation.column(spec.provider)
    val x = (if (options.includeIntercept) Seq(lit(1.0)) else Seq.empty[Column]) ++
      spec.features.map(name => Validation.column(name).cast(DoubleType)) :+
      spec.offset.fold(lit(0.0))(name => Validation.column(name).cast(DoubleType))
    val frame = df.select(
      (if (text) provider else provider.cast(LongType)).as(Validation.GroupColumn),
      y.as(LinearValidation.OutcomeColumn),
      array(x: _*).as(Validation.FeaturesColumn),
      spec.rowId
        .fold(lit(0L))(name => Validation.column(name).cast(LongType))
        .as(Validation.RowIdColumn)
    )
    val names =
      (if (options.includeIntercept) Seq(InterceptName) else Seq.empty[String]) ++ spec.features :+
        OffsetName
    LinearInput(frame, LinearSpec(spec.outcome, names, spec.provider, spec.rowId), rows, text)
  }
}

/** The logistic random-intercept model (spec 4b-1 §7): provider-local blocks; two passes for the centring and the
  * aliasing check; then the kernel's searches, each evaluation one pass whose block sums are reduced in block order,
  * the nAGQ = 0 step's per-provider sums gathered on the driver under its budget (OI-98).
  */
object LogisticRE {

  def fit(
      df: DataFrame,
      spec: LogisticRESpec,
      options: LogisticREOptions = LogisticREOptions()
  ): LogisticREFit = {
    val (input, sizes, plan) = layout(df, spec, options)
    val p = input.spec.features.size - 1
    val m = sizes.size
    val design = Vector.newBuilder[InputProblem]
    if (m < 2) design += InputProblem.TooFewProviders(spec.provider, m.toLong)
    if (input.rowCount <= p.toLong) design += InputProblem.TooFewRows(input.rowCount, p)
    val problems = design.result()
    if (problems.nonEmpty) throw new InvalidInputException(problems)
    DriverGuards.requireWithinBudget(
      s"the nAGQ = 0 step's sums for $m providers (OI-98)",
      8L * m.toLong * (p + 3L),
      options.blocks
    )
    val workingSet = LinearWorkingSet.build(input, plan, options.blocks.resolvedStorageLevel)
    try fitOn(df, spec, options, input, plan, workingSet, p, m)
    finally workingSet.release()
  }

  private def fitOn(
      df: DataFrame,
      spec: LogisticRESpec,
      options: LogisticREOptions,
      input: LinearInput,
      plan: LayoutPlan,
      workingSet: LinearWorkingSet,
      p: Int,
      m: Int
  ): LogisticREFit = {
    val first = workingSet.blocks
      .map((block: LinearBlock) => firstSums(block))(Encoders.product[LogisticREFirstPartial])
      .collect()
      .sortBy(_.blockId)
    val rows = first.iterator.map(_.rows).sum
    if (rows != plan.rowCount)
      throw new IllegalStateException(s"the blocks hold $rows rows; the plan has ${plan.rowCount}")
    val intercept = options.includeIntercept
    val centre = Array.tabulate(p) { k =>
      if (!intercept || k == 0) 0.0 else Summation.pairwise(first.map(_.xSum(k))) / rows.toDouble
    }
    val outcomeMean = Summation.pairwise(first.map(_.ySum)) / rows.toDouble
    val cross = workingSet.blocks
      .map((block: LinearBlock) => crossProducts(block, centre))(
        Encoders.product[LogisticRECrossPartial]
      )
      .collect()
      .sortBy(_.blockId)
    val xx = Array.tabulate(Moments.packedLength(p))(k => Summation.pairwise(cross.map(_.xx(k))))
    val names = input.spec.features.take(p)
    if (p > 0) {
      val aliased = Cholesky.factor(xx, p).aliased
      if (aliased.nonEmpty) throw new LogisticREAliasingException(aliased.map(names))
    }
    val blocks = new DistributedBlocks(workingSet, p, centre, m)
    val solution = Kernel.fit(
      blocks,
      rows,
      m,
      p,
      intercept,
      centre,
      outcomeMean,
      options.nagq1,
      options.sigmaUpper,
      options.maxIterations
    )
    val coefficients = names.indices.map { i =>
      LogisticRECoefficient(
        names(i),
        solution.beta(i),
        math.sqrt(solution.covariance(Moments.packedIndex(i, i, p)))
      )
    }.toVector
    LogisticREFit(
      coefficients,
      solution.covariance.toVector,
      providerTable(df, input, plan, options, solution, blocks.records),
      solution.sigma,
      if (options.nagq1) "Laplace" else "nAGQ0",
      rows,
      m,
      solution.deviance,
      solution.logLikelihood,
      solution.aic,
      solution.bic,
      solution.pwrss,
      solution.logDetH,
      solution.ussq,
      LogisticRESearch(
        solution.stage1Evaluations,
        solution.stage2Evaluations,
        solution.projectedGradient,
        solution.converged,
        solution.boundary
      ),
      solution.warnings,
      spec,
      options,
      SoftwareInfo.capture(df.sparkSession),
      LayoutSummary.of(plan),
      first.iterator.map(_.fingerprint).sum
    )
  }

  /** Validation, group sizes and the budgeted plan. */
  private def layout(
      df: DataFrame,
      spec: LogisticRESpec,
      options: LogisticREOptions
  ): (LinearInput, Vector[(GroupKey, Long)], LayoutPlan) = {
    val input = LogisticREValidation.validate(df, spec, options)
    val p = input.spec.features.size - 1
    val sizes = GroupSizes.collectGroupSizes(input.frame, options.blocks).sortBy(_._1)
    val partialBytes = 8L * (Moments.packedLength(p) + 2L * p + 6L)
    val plan = BudgetedLayout.plan(sizes, options.blocks, p + 2, partialBytes)
    DriverGuards.requireWithinBudget(
      s"${plan.blockCount} block partials",
      plan.blockCount.toLong * partialBytes,
      options.blocks
    )
    (input, sizes, plan)
  }

  /** The Laplace deviance D(σ, β) of the fit's model on `df` at β in the original coordinates (spec §2), for the
    * optimality checks of D-59 and slice 4b-2's profile.
    */
  def laplaceDeviance(
      df: DataFrame,
      fit: LogisticREFit,
      sigma: Double,
      beta: Seq[Double]
  ): Double = {
    val (input, _, plan) = layout(df, fit.spec, fit.options)
    val p = input.spec.features.size - 1
    require(beta.length == p, s"beta has ${beta.length} values; the model has $p fixed effects")
    val workingSet = LinearWorkingSet.build(input, plan, fit.options.blocks.resolvedStorageLevel)
    try
      new DistributedBlocks(workingSet, p, new Array[Double](p), fit.providerCount)
        .laplace(sigma, beta.toArray)
        .deviance
    finally workingSet.release()
  }

  private def kernelRows(block: LinearBlock): Kernel.Rows =
    Kernel.Rows(block.groupStart, block.values, block.p - 1)

  private def firstSums(block: LinearBlock): LogisticREFirstPartial = {
    val p = block.p - 1
    val q = block.p + 1
    val x = new NeumaierVector(p)
    val y = new NeumaierSum
    var r = 0
    while (r < block.rowId.length) {
      var k = 0
      while (k < p) {
        x.addAt(k, block.values(r * q + k))
        k += 1
      }
      y.add(block.values(r * q + block.p))
      r += 1
    }
    LogisticREFirstPartial(
      block.blockId,
      block.rowId.length.toLong,
      x.values,
      y.value,
      LinearFE.fingerprintOf(block)
    )
  }

  private def crossProducts(block: LinearBlock, centre: Array[Double]): LogisticRECrossPartial = {
    val p = block.p - 1
    val q = block.p + 1
    val xx = new NeumaierVector(Moments.packedLength(p))
    val x = new Array[Double](p)
    var r = 0
    while (r < block.rowId.length) {
      var k = 0
      while (k < p) {
        x(k) = block.values(r * q + k) - centre(k)
        k += 1
      }
      var j = 0
      while (j < p) {
        var l = j
        while (l < p) {
          xx.addAt(Moments.packedIndex(j, l, p), x(j) * x(l))
          l += 1
        }
        j += 1
      }
      r += 1
    }
    LogisticRECrossPartial(block.blockId, xx.values)
  }

  /** The kernel's blocks over the working set: each evaluation one pass, block sums reduced in block order. */
  private final class DistributedBlocks(
      workingSet: LinearWorkingSet,
      p: Int,
      centre: Array[Double],
      m: Int
  ) extends Kernel.Blocks {

    var records: Array[Long] = new Array[Long](m)

    def laplace(sigma: Double, beta: Array[Double]): Kernel.LaplaceSums = {
      val c = centre
      val parts = workingSet.blocks
        .map { (block: LinearBlock) =>
          val s = Kernel.laplace(kernelRows(block), sigma, beta, c)
          LogisticRELaplacePartial(block.blockId, s.deviance, s.gradient)
        }(Encoders.product[LogisticRELaplacePartial])
        .collect()
        .sortBy(_.blockId)
      Kernel.combineLaplace(parts.map(t => Kernel.LaplaceSums(t.deviance, t.gradient)).toSeq)
    }

    def stage1(sigma: Double, beta: Array[Double], u: Array[Double]): Kernel.Stage1Sums = {
      val c = centre
      val parts = workingSet.blocks
        .map { (block: LinearBlock) =>
          val s = Kernel.stage1(kernelRows(block), sigma, beta, block.groupIndex.map(u), c)
          LogisticREStage1Partial(
            block.blockId,
            block.groupIndex,
            s.weights,
            s.wx,
            s.residuals,
            s.xwx,
            s.rx,
            s.penalized,
            s.logH
          )
        }(Encoders.product[LogisticREStage1Partial])
        .collect()
        .sortBy(_.blockId)
      val weights = new Array[Double](m)
      val wx = new Array[Double](m * p)
      val residuals = new Array[Double](m)
      parts.foreach { t =>
        t.groupIndex.indices.foreach { g =>
          val j = t.groupIndex(g)
          weights(j) = t.weights(g)
          residuals(j) = t.residuals(g)
          System.arraycopy(t.wx, g * p, wx, j * p, p)
        }
      }
      Kernel.Stage1Sums(
        weights,
        wx,
        residuals,
        Array.tabulate(Moments.packedLength(p))(k => Summation.pairwise(parts.map(_.xwx(k)))),
        Array.tabulate(p)(k => Summation.pairwise(parts.map(_.rx(k)))),
        Summation.pairwise(parts.map(_.penalized)),
        Summation.pairwise(parts.map(_.logH))
      )
    }

    def modes(sigma: Double, beta: Array[Double]): (Array[Double], Array[Double]) = {
      val c = centre
      val parts = workingSet.blocks
        .map { (block: LinearBlock) =>
          val (u, w) = Kernel.modes(kernelRows(block), sigma, beta, c)
          val sizes = block.groupStart.indices.map { g =>
            val until =
              if (g + 1 < block.groupStart.length) block.groupStart(g + 1) else block.rowId.length
            (until - block.groupStart(g)).toLong
          }.toArray
          LogisticREModesPartial(block.blockId, block.groupIndex, sizes, u, w)
        }(Encoders.product[LogisticREModesPartial])
        .collect()
      val u = new Array[Double](m)
      val w = new Array[Double](m)
      val counts = new Array[Long](m)
      parts.foreach { t =>
        t.groupIndex.indices.foreach { g =>
          u(t.groupIndex(g)) = t.modes(g)
          w(t.groupIndex(g)) = t.weights(g)
          counts(t.groupIndex(g)) = t.records(g)
        }
      }
      records = counts
      (u, w)
    }
  }

  /** The provider table (§4) by group index, with the keys from the plan's placements, persisted and ordered by key. */
  private def providerTable(
      df: DataFrame,
      input: LinearInput,
      plan: LayoutPlan,
      options: LogisticREOptions,
      solution: Kernel.Fit,
      records: Array[Long]
  ): DataFrame = {
    val spark = df.sparkSession
    val schema = StructType(
      Seq(
        StructField("groupIndex", org.apache.spark.sql.types.IntegerType, nullable = false),
        StructField("records", LongType, nullable = false),
        StructField("weight", DoubleType, nullable = false),
        StructField("blup", DoubleType, nullable = false),
        StructField("variance", DoubleType, nullable = false),
        StructField("se", DoubleType, nullable = false)
      )
    )
    val rows = solution.blups.indices.map { j =>
      Row(
        j,
        records(j),
        solution.weights(j),
        solution.blups(j),
        solution.conditionalVariances(j),
        math.sqrt(solution.conditionalVariances(j))
      )
    }
    val key = input.spec.provider
    val keys = PlanFrames
      .placements(spark, plan, input.providerKeyIsText)
      .select(
        col(PlanFrames.GroupIndexColumn).as("groupIndex"),
        col(Validation.GroupColumn).as(key)
      )
    val table = spark
      .createDataFrame(rows.asJava, schema)
      .join(keys, Seq("groupIndex"))
      .select(col(key), col("records"), col("weight"), col("blup"), col("variance"), col("se"))
      .orderBy(key)
      .persist(options.blocks.resolvedStorageLevel)
    table.count()
    table
  }

  private val Key = "__pprof_key"
  private val Effect = "__pprof_blup"

  /** `df` with `prediction` (§6): the linear predictor of the fixed effects, plus the offset column when the fit has
    * one and `withOffset`, plus with `useRe` the BLUP of providers in the fit and 0 for others, under any key type
    * (X-044); `type` response (the default) or link.
    */
  def predict(
      df: DataFrame,
      fit: LogisticREFit,
      useRe: Boolean = false,
      withOffset: Boolean = true,
      `type`: String = "response"
  ): DataFrame = {
    require(
      Seq("response", "link").contains(`type`),
      s"type must be response or link, got ${`type`}"
    )
    val fixed = linearPart(df, fit, withOffset)
    val (frame, eta) =
      if (!useRe) (df, fixed)
      else {
        val (joined, blup) = joinBlups(df, fit)
        (joined, fixed + blup)
      }
    frame
      .withColumn("prediction", if (`type` == "link") eta else lit(1.0) / (lit(1.0) + exp(-eta)))
      .drop(Key, Effect)
  }

  /** The training rows with `xbeta` (offset + xᵀβ̂), `eta` (with the BLUP), `fitted` (μ̂), `residual` (y − μ̂),
    * `pearson` and `deviance_residual` (with the saturated terms of fractional outcomes) (§4).
    */
  def fitted(df: DataFrame, fit: LogisticREFit): DataFrame = {
    val fixed = linearPart(df, fit, withOffset = true)
    val (joined, blup) = joinBlups(df, fit)
    val eta = fixed + blup
    val mu = lit(1.0) / (lit(1.0) + exp(-eta))
    val y = Validation.column(fit.spec.outcome).cast(DoubleType)
    def xlogx(a: Column, b: Column): Column = when(a > lit(0.0), a * log(a / b)).otherwise(lit(0.0))
    val unit = lit(2.0) * (xlogx(y, mu) + xlogx(lit(1.0) - y, lit(1.0) - mu))
    val sign = when(y >= mu, lit(1.0)).otherwise(lit(-1.0))
    joined
      .withColumn("xbeta", fixed)
      .withColumn("eta", eta)
      .withColumn("fitted", mu)
      .withColumn("residual", y - mu)
      .withColumn("pearson", (y - mu) / org.apache.spark.sql.functions.sqrt(mu * (lit(1.0) - mu)))
      .withColumn("deviance_residual", sign * org.apache.spark.sql.functions.sqrt(unit))
      .drop(Key, Effect)
  }

  private def linearPart(df: DataFrame, fit: LogisticREFit, withOffset: Boolean): Column = {
    val spec = fit.spec
    val offset = if (withOffset) spec.offset else None
    val types = df.schema.fields.map(f => f.name -> f.dataType).toMap
    val structural = (spec.features ++ offset).flatMap { name =>
      types.get(name) match {
        case None => Some(InputProblem.MissingColumn(name))
        case Some(found) if !found.isInstanceOf[NumericType] =>
          Some(InputProblem.UnsupportedType(name, found.simpleString, "a numeric type"))
        case Some(_) => None
      }
    }
    if (structural.nonEmpty) throw new InvalidInputException(structural)
    val estimates = fit.coefficients.map(c => c.feature -> c.estimate).toMap
    val start = estimates.get(LogisticREValidation.InterceptName).fold(lit(0.0))(lit(_))
    val linear = spec.features.foldLeft(start) { (total, name) =>
      total + Validation.column(name).cast(DoubleType) * lit(estimates(name))
    }
    offset.fold(linear)(name => linear + Validation.column(name).cast(DoubleType))
  }

  private def joinBlups(frame: DataFrame, fit: LogisticREFit): (DataFrame, Column) = {
    val spec = fit.spec
    val text = fit.providers.schema(spec.provider).dataType == StringType
    val found = frame.schema.fields.find(_.name == spec.provider).map(_.dataType)
    found match {
      case None => throw new InvalidInputException(Seq(InputProblem.MissingColumn(spec.provider)))
      case Some(t) if (t == StringType) != text || !(t == StringType || Validation.isIntegral(t)) =>
        throw new InvalidInputException(
          Seq(
            InputProblem.UnsupportedType(
              spec.provider,
              t.simpleString,
              if (text) "a string type, as in the fit" else "an integral type, as in the fit"
            )
          )
        )
      case _ => ()
    }
    val provider = Validation.column(spec.provider)
    val effects = fit.providers.select(col(spec.provider).as(Key), col("blup").as(Effect))
    val joined = frame.join(
      effects,
      (if (text) provider.cast(StringType) else provider.cast(LongType)) === col(Key),
      "left"
    )
    (joined, coalesce(col(Effect), lit(0.0)))
  }
}
