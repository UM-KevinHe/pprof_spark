package pprof.spark.engine.logistic

import org.apache.spark.sql.{Column, DataFrame, Encoders}
import org.apache.spark.sql.functions.{broadcast, col, concat, count, lit, sum, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}
import pprof.spark.engine.data.{InputProblem, InvalidInputException, Validation}
import pprof.spark.engine.backend.{BlockOptions, DriverGuards}
import pprof.spark.engine.layout.GroupKey
import pprof.spark.numerics.{NeumaierSum, NeumaierVector, TaylorBins}
import pprof.spark.numerics.kernels.ThreeStageKernel

/** Column roles of He et al. (2013)'s three-stage model: a binary outcome, features, the provider
  * (facility) and the cluster (hospital) (docs/spec/logistic/three-stage-preparation.md §2).
  */
final case class ThreeStageSpec(
    outcome: String,
    features: Seq[String],
    provider: String,
    cluster: String,
    rowId: Option[String] = None
)

/** `cutoff`: providers and cells need more than this many records; `stage1`: slice 2a's options for
  * stage 1, without screening (pprof_py's `screen_providers=False`).
  */
final case class ThreeStageOptions(
    cutoff: Long = 10L,
    stage1: LogisticOptions = LogisticOptions(screen = false)
) {
  require(cutoff >= 0L, s"cutoff must be non-negative, got $cutoff")
}

/** The prepared records (input columns plus `y_adj`, `cell`, `included` and `stage1_offset`), the stage 1
  * fit (its providers are the included cells), counts, and the excluded providers with their sizes.
  */
final case class ThreeStagePreparation(
    data: DataFrame,
    stage1: LogisticFit,
    providers: Long,
    clusters: Long,
    cells: Long,
    includedCells: Long,
    excluded: Vector[(GroupKey, Long)]
)

/** Stage 3's options (docs/spec/logistic/three-stage-stage3.md §2): pprof_py's defaults. */
final case class ThreeStageStage3Options(
    nNodes: Int = 20,
    maxIter: Int = 10000,
    tol: Double = 1e-5,
    bound: Double = 10.0,
    boundMode: String = "relative",
    blocks: BlockOptions = BlockOptions()
) {
  require(
    boundMode == "relative" || boundMode == "absolute",
    s"boundMode must be relative or absolute, got $boundMode"
  )
  require(
    nNodes >= 1 && maxIter >= 0 && tol > 0.0 && bound > 0.0,
    "nNodes, maxIter, tol and bound must be positive"
  )
  def kernel: ThreeStageKernel.Options =
    ThreeStageKernel.Options(nNodes, maxIter, tol, bound, boundMode == "relative")
}

/** A prepared data set compressed into (cluster, provider) cells with offset-bin moments (X-029). */
final case class ThreeStageCells(
    providers: Vector[GroupKey],
    clusters: Vector[GroupKey],
    cells: Array[ThreeStageKernel.Cell]
) {
  def bins: Long = cells.iterator.map(_.bins.length.toLong).sum
}

/** Stage 3's fit: provider effects in `providers` order and cluster posterior moments in `clusters` order. */
final case class ThreeStageStage3(
    providers: Vector[GroupKey],
    clusters: Vector[GroupKey],
    gamma: Array[Double],
    sigma: Double,
    converged: Boolean,
    stalled: Boolean,
    iterations: Int,
    criterion: Double,
    loglik: Double,
    alphaMean: Array[Double],
    alphaVar: Array[Double],
    held: Array[Boolean],
    cells: Int,
    bins: Long,
    options: ThreeStageStage3Options
)

/** One prepared record, by provider and cluster index. */
final case class ThreeStageRecord(provider: Int, cluster: Int, y: Double, offset: Double)

/** Preparation and stage 1 of the three-stage model (Phase 2f-1), as pprof_py's `glmm_data_prep` and
  * the first stage of `LogisticThreeStageModel`.
  */
object ThreeStage {

  /** Cell keys are `cluster`, U+001F, `provider`. */
  val Separator: String = "\u001f"

  val Outputs: Seq[String] = Seq("y_adj", "cell", "included", "stage1_offset")

  private val Size = "__pprof_size"
  private val Events = "__pprof_events"
  private val Key = "__pprof_provider"
  private val CellSize = "__pprof_cell_size"

  def prepare(
      df: DataFrame,
      spec: ThreeStageSpec,
      options: ThreeStageOptions = ThreeStageOptions()
  ): ThreeStagePreparation = {
    validate(df, spec)
    val cutoff = options.cutoff
    val y = Validation.column(spec.outcome)
    val provider = Validation.column(spec.provider)
    val cluster = Validation.column(spec.cluster)
    val sizes = df
      .groupBy(provider.as(Key))
      .agg(count(lit(1)).as(Size), sum(y.cast(LongType)).as(Events))
      .persist()
    try {
      val providerCount = sizes.count()
      val excludedRows = sizes.filter(col(Size) <= lit(cutoff))
      val excludedCount = excludedRows.count()
      require(
        excludedCount <= options.stage1.maxProvidersOnDriver.toLong,
        s"$excludedCount excluded providers exceed maxProvidersOnDriver ${options.stage1.maxProvidersOnDriver}"
      )
      val text = df.schema(spec.provider).dataType == StringType
      val excluded = excludedRows
        .select(col(Key), col(Size))
        .collect()
        .map { r =>
          val key: GroupKey =
            if (text) GroupKey.Text(r.getString(0))
            else GroupKey.Integral(r.getAs[Number](0).longValue)
          key -> r.getLong(1)
        }
        .sortBy(_._1)
        .toVector
      val kept = providerCount - excludedCount
      if (kept == 0L)
        throw new IllegalArgumentException(
          s"no provider has more than $cutoff records, so the three-stage preparation keeps nothing"
        )
      val joinSizes =
        if (providerCount <= options.stage1.maxProvidersOnDriver) broadcast(sizes) else sizes
      val size = col(Size).cast(DoubleType)
      // pprof_py: y + (events == 0) * 0.01 / size - (events == size) * 0.01 / size, in that order
      val yAdj =
        (y.cast(DoubleType) + when(col(Events) === 0L, lit(0.01)).otherwise(lit(0.0)) / size) -
          when(col(Events) === col(Size), lit(0.01)).otherwise(lit(0.0)) / size
      val records = df
        .withColumn(Key, provider)
        .join(joinSizes, Seq(Key))
        .filter(col(Size) > lit(cutoff))
        .withColumn("y_adj", yAdj)
        .withColumn(
          "cell",
          concat(cluster.cast(StringType), lit(Separator), provider.cast(StringType))
        )
      val cellSizes = records.groupBy("cell").agg(count(lit(1)).as(CellSize)).persist()
      try {
        val cells = cellSizes.count()
        val includedCells = cellSizes.filter(col(CellSize) > lit(cutoff)).count()
        if (includedCells == 0L)
          throw new IllegalArgumentException(
            s"no provider × cluster cell has more than $cutoff records, so stage 1 has nothing to fit"
          )
        val joinCells =
          if (cells <= options.stage1.maxProvidersOnDriver) broadcast(cellSizes) else cellSizes
        val prepared = records
          .join(joinCells, Seq("cell"))
          .withColumn("included", (col(CellSize) > lit(cutoff)).cast(IntegerType))
          .drop(Key, Size, Events, CellSize)
        val stage1 = LogisticFE.fit(
          prepared.filter(col("included") === 1),
          LogisticSpec(spec.outcome, spec.features, "cell", None, spec.rowId),
          options.stage1
        )
        val offset: Column =
          spec.features.zip(stage1.estimates).foldLeft(lit(0.0)) { case (acc, (name, b)) =>
            acc + Validation.column(name).cast(DoubleType) * lit(b)
          }
        val data = prepared.withColumn("stage1_offset", offset)
        val clusters = data.select(cluster).distinct().count()
        ThreeStagePreparation(data, stage1, kept, clusters, cells, includedCells, excluded)
      } finally {
        cellSizes.unpersist()
        ()
      }
    } finally {
      sizes.unpersist()
      ()
    }
  }

  /** Compresses prepared records (2f-1's `data`) into cells: one pass, a shuffle by cell, and per cell
    * Σ`y_adj`, Σ`y_adj`·offset and the offsets' bin moments, accumulated in a canonical order (X-029).
    */
  def compress(
      prepared: DataFrame,
      spec: ThreeStageSpec,
      options: ThreeStageStage3Options = ThreeStageStage3Options(),
      offsetColumn: String = "stage1_offset"
  ): ThreeStageCells = {
    val missing =
      Seq(spec.provider, spec.cluster, "y_adj", offsetColumn).filterNot(prepared.columns.contains)
    if (missing.nonEmpty) throw new InvalidInputException(missing.map(InputProblem.MissingColumn))
    def keys(name: String): Vector[GroupKey] = {
      val text = prepared.schema(name).dataType == StringType
      val values = prepared.select(Validation.column(name)).distinct().collect()
      require(
        values.length.toLong <= options.blocks.maxGroupsOnDriver.toLong,
        s"${values.length} distinct $name values exceed maxGroupsOnDriver ${options.blocks.maxGroupsOnDriver}"
      )
      values
        .map(r =>
          if (text) GroupKey.Text(r.getString(0)): GroupKey
          else GroupKey.Integral(r.getAs[Number](0).longValue)
        )
        .sorted
        .toVector
    }
    val providers = keys(spec.provider)
    val clusters = keys(spec.cluster)
    val providerIndex = providers.map(k => GroupKey.value(k).toString).zipWithIndex.toMap
    val clusterIndex = clusters.map(k => GroupKey.value(k).toString).zipWithIndex.toMap
    val m = providers.size.toLong
    val cells = prepared
      .select(
        Validation.column(spec.provider).cast(StringType),
        Validation.column(spec.cluster).cast(StringType),
        col("y_adj").cast(DoubleType),
        Validation.column(offsetColumn).cast(DoubleType)
      )
      .as(
        Encoders.tuple(Encoders.STRING, Encoders.STRING, Encoders.scalaDouble, Encoders.scalaDouble)
      )
      .map { case (p, h, y, o) => ThreeStageRecord(providerIndex(p), clusterIndex(h), y, o) }(
        Encoders.product[ThreeStageRecord]
      )
      .groupByKey((r: ThreeStageRecord) => r.cluster.toLong * m + r.provider)(Encoders.scalaLong)
      .mapGroups { (_: Long, rows: Iterator[ThreeStageRecord]) =>
        val sorted = rows.toArray.sortBy(r => (r.offset, r.y))
        val events = new NeumaierSum
        val yOffset = new NeumaierSum
        val bins = scala.collection.mutable.TreeMap.empty[Long, NeumaierVector]
        sorted.foreach { r =>
          events.add(r.y)
          yOffset.add(r.y * r.offset)
          val b = TaylorBins.bin(r.offset)
          bins
            .getOrElseUpdate(b, new NeumaierVector(TaylorBins.Order + 1))
            .add(TaylorBins.rowMoments(r.offset, 1.0, b))
        }
        ThreeStageKernel.Cell(
          sorted.head.cluster,
          sorted.head.provider,
          events.value,
          yOffset.value,
          bins.keys.toArray,
          bins.values.flatMap(_.values).toArray
        )
      }(Encoders.product[ThreeStageKernel.Cell])
      .collect()
      .sortBy(c => (c.cluster, c.provider))
    val compressed = ThreeStageCells(providers, clusters, cells)
    DriverGuards.requireWithinBudget(
      s"${compressed.bins} compressed cell bins",
      compressed.bins * (TaylorBins.Order + 1) * 8L,
      options.blocks
    )
    compressed
  }

  /** Stage 3 with σ fixed, from `start` (one effect per provider in key order) (Phase 2f-2). */
  def stage3(
      prepared: DataFrame,
      spec: ThreeStageSpec,
      sigma: Double,
      start: Array[Double],
      options: ThreeStageStage3Options = ThreeStageStage3Options(),
      offsetColumn: String = "stage1_offset"
  ): ThreeStageStage3 =
    stage3(compress(prepared, spec, options, offsetColumn), sigma, start, options)

  def stage3(
      cells: ThreeStageCells,
      sigma: Double,
      start: Array[Double],
      options: ThreeStageStage3Options
  ): ThreeStageStage3 = {
    require(
      start.length == cells.providers.size,
      s"the start has ${start.length} effects for ${cells.providers.size} providers"
    )
    val r = ThreeStageKernel.fit(
      cells.cells,
      cells.providers.size,
      cells.clusters.size,
      sigma,
      start,
      options.kernel
    )
    ThreeStageStage3(
      cells.providers,
      cells.clusters,
      r.gamma,
      sigma,
      r.converged,
      r.stalled,
      r.iterations,
      r.criterion,
      r.loglik,
      r.alphaMean,
      r.alphaVar,
      r.held,
      cells.cells.length,
      cells.bins,
      options
    )
  }

  /** The marginal log-likelihood and score at γ, with each cluster's nodes centred for γ (pprof_py's
    * `_cluster_modes` from zero, then `_marginal_terms`); σ = 0 takes the limit.
    */
  def marginal(
      cells: ThreeStageCells,
      sigma: Double,
      gamma: Array[Double],
      nNodes: Int = 20
  ): (Double, Array[Double]) = {
    val m = cells.providers.size
    val t =
      if (sigma == 0.0) ThreeStageKernel.limitTerms(cells.cells, m, gamma, hessian = false)
      else {
        val groups = ThreeStageKernel.byCluster(cells.cells, cells.clusters.size)
        val (center, scale) = ThreeStageKernel.modes(
          cells.cells,
          groups,
          gamma,
          sigma,
          new Array[Double](cells.clusters.size)
        )
        val rule = pprof.spark.numerics.GaussHermite.rule(nNodes)
        ThreeStageKernel.terms(
          cells.cells,
          groups,
          m,
          gamma,
          sigma,
          center,
          scale,
          rule,
          rule.adaptiveLogWeights,
          hessian = false
        )
      }
    (t.loglik, t.score)
  }

  /** Every problem with the input at once, as counts (spec 2f-1 §2). */
  private def validate(df: DataFrame, spec: ThreeStageSpec): Unit = {
    val schema = df.schema
    val names = schema.fieldNames.toSet
    val wanted = Seq(spec.outcome, spec.provider, spec.cluster) ++ spec.features ++ spec.rowId
    val problems = Vector.newBuilder[InputProblem]
    wanted.filterNot(names).foreach(name => problems += InputProblem.MissingColumn(name))
    Outputs.filter(names).foreach(name => problems += InputProblem.ReservedColumn(name))
    def typed(name: String, ok: Boolean, expected: String): Unit =
      if (names(name) && !ok)
        problems += InputProblem.UnsupportedType(name, schema(name).dataType.simpleString, expected)
    if (names(spec.outcome))
      typed(spec.outcome, Validation.isIntegral(schema(spec.outcome).dataType), "an integral type")
    Seq(spec.provider, spec.cluster).filter(names).foreach { name =>
      val t = schema(name).dataType
      typed(name, Validation.isIntegral(t) || t == StringType, "an integral or string type")
    }
    spec.features.filter(names).foreach { name =>
      typed(
        name,
        Validation.isIntegral(schema(name).dataType) || schema(name).dataType == DoubleType ||
          schema(name).dataType.typeName == "float",
        "a numeric type"
      )
    }
    val early = problems.result()
    if (early.nonEmpty) throw new InvalidInputException(early)
    def countWhere(condition: Column): Column = sum(when(condition, 1L).otherwise(0L))
    val y = Validation.column(spec.outcome)
    val features = spec.features.map(Validation.column)
    val aggregates = Seq(
      count(lit(1)),
      countWhere(y.isNull),
      countWhere(y.isNotNull && !y.isin(0, 1)),
      countWhere(Validation.column(spec.provider).isNull),
      countWhere(Validation.column(spec.cluster).isNull)
    ) ++ features.flatMap { f =>
      val d = f.cast(DoubleType)
      Seq(
        countWhere(f.isNull),
        countWhere(
          f.isNotNull && (d.isNaN || d === Double.PositiveInfinity || d === Double.NegativeInfinity)
        )
      )
    }
    val row = df.agg(aggregates.head, aggregates.tail: _*).head()
    def at(i: Int): Long = if (row.isNullAt(i)) 0L else row.getLong(i)
    if (at(0) == 0L) problems += InputProblem.EmptyInput
    if (at(1) > 0L) problems += InputProblem.InvalidValues(spec.outcome, at(1), 0L)
    if (at(2) > 0L) problems += InputProblem.NonBinaryValues(spec.outcome, at(2))
    if (at(3) > 0L) problems += InputProblem.InvalidValues(spec.provider, at(3), 0L)
    if (at(4) > 0L) problems += InputProblem.InvalidValues(spec.cluster, at(4), 0L)
    spec.features.zipWithIndex.foreach { case (name, i) =>
      val (nulls, nonFinite) = (at(5 + 2 * i), at(6 + 2 * i))
      if (nulls > 0L || nonFinite > 0L)
        problems += InputProblem.InvalidValues(name, nulls, nonFinite)
    }
    val found = problems.result()
    if (found.nonEmpty) throw new InvalidInputException(found)
  }
}
