package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, Row}
import org.apache.spark.sql.functions.{broadcast, col, concat, count, lit, sum, when}
import org.apache.spark.sql.types.{
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import pprof.spark.engine.data.{InputProblem, InvalidInputException, Validation}
import pprof.spark.engine.backend.{BlockOptions, DriverGuards}
import pprof.spark.engine.layout.GroupKey
import pprof.spark.numerics.{GaussHermite, NeumaierSum, NeumaierVector, TaylorBins}
import pprof.spark.numerics.kernels.{ThreeStageGlmm, ThreeStageKernel}

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
    blocks: BlockOptions = BlockOptions(),
    path: String = "auto"
) {
  require(
    Seq("auto", "driver", "executors").contains(path),
    s"path must be auto, driver or executors, got $path"
  )
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

/** Stage 2's options (docs/spec/logistic/three-stage-stage2.md §2). */
final case class ThreeStageStage2Options(
    pirlsTolerance: Double = 1e-12,
    pirlsMaxIterations: Int = 100,
    gradientTolerance: Double = 1e-7,
    maxIterations: Int = 200,
    blocks: BlockOptions = BlockOptions()
) {
  def kernel: ThreeStageGlmm.Options =
    ThreeStageGlmm.Options(
      pirlsTolerance,
      pirlsMaxIterations,
      pprof.spark.numerics.BoundedQuasiNewton.Options(gradientTolerance, maxIterations)
    )
}

/** Stage 2's fit: the SDs, the intercept, and the BLUPs in provider and cluster key order. */
final case class ThreeStageStage2(
    providers: Vector[GroupKey],
    clusters: Vector[GroupKey],
    sigmaProvider: Double,
    sigmaCluster: Double,
    intercept: Double,
    blupProviders: Array[Double],
    blupClusters: Array[Double],
    deviance: Double,
    converged: Boolean,
    iterations: Int,
    evaluations: Int,
    projectedGradient: Double
) {

  /** Stage 3's start: each provider's BLUP plus the intercept (pprof_py's `_values_from_stages`). */
  def start: Array[Double] = blupProviders.map(_ + intercept)
}

/** One prepared record, by provider and cluster index. */
final case class ThreeStageRecord(provider: Int, cluster: Int, y: Double, offset: Double)

/** Whole clusters of compressed cells for the executor path (spec 2f-2 §7); cells in (cluster, provider) order. */
final case class ThreeStageClusterBlock(
    blockId: Int,
    clusters: Array[Int],
    cells: Array[ThreeStageKernel.Cell]
)

final case class ThreeStageClusterPartial(blockId: Int, terms: Array[ThreeStageKernel.ClusterTerms])

final case class ThreeStageClusterSize(cluster: Int, bins: Long)

/** Stage 3's per-cluster work on executors: closures carry γ and the clusters' centres (§6.7). */
final class SparkThreeStagePasses(
    blocks: Dataset[ThreeStageClusterBlock],
    val providers: Int,
    val clusters: Int,
    sigma: Double,
    nNodes: Int
) extends ThreeStageKernel.Passes {

  private def run(
      gamma: Array[Double],
      start: Array[Double],
      fixed: Option[(Array[Double], Array[Double])],
      hessian: Boolean
  ) = {
    val g = gamma.clone()
    val st = start.clone()
    val fx = fixed.map { case (c, s) => (c.clone(), s.clone()) }
    val sig = sigma
    val n = nNodes
    val parts = blocks
      .map { (b: ThreeStageClusterBlock) =>
        val rule = GaussHermite.rule(n)
        val logWeights = rule.adaptiveLogWeights
        val terms = b.clusters.map { h =>
          val cells = b.cells.filter(_.cluster == h)
          ThreeStageKernel.clusterTerms(
            h,
            cells,
            g,
            sig,
            st(h),
            fx.map { case (c, s) => (c(h), s(h)) },
            rule,
            logWeights,
            hessian
          )
        }
        ThreeStageClusterPartial(b.blockId, terms)
      }(Encoders.product[ThreeStageClusterPartial])
      .collect()
    ThreeStageKernel.assemble(parts.toSeq.flatMap(_.terms.toSeq), providers, clusters, hessian)
  }

  def full(gamma: Array[Double], start: Array[Double]): ThreeStageKernel.Terms =
    run(gamma, start, None, hessian = true)

  def value(gamma: Array[Double], center: Array[Double], scale: Array[Double]): Double =
    run(gamma, center, Some((center, scale)), hessian = false).loglik
}

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
    val (providers, clusters, cellData) = compressed(prepared, spec, options, offsetColumn)
    val cells = cellData.collect().sortBy(c => (c.cluster, c.provider))
    val result = ThreeStageCells(providers, clusters, cells)
    DriverGuards.requireWithinBudget(
      s"${result.bins} compressed cell bins",
      result.bins * (TaylorBins.Order + 1) * 8L,
      options.blocks
    )
    result
  }

  /** The keys and the distributed compressed cells (spec 2f-2 §3, §7). */
  private def compressed(
      prepared: DataFrame,
      spec: ThreeStageSpec,
      options: ThreeStageStage3Options,
      offsetColumn: String
  ): (Vector[GroupKey], Vector[GroupKey], Dataset[ThreeStageKernel.Cell]) = {
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
    (providers, clusters, cells)
  }

  /** Stage 3 with σ fixed, from `start` (one effect per provider in key order) (Phase 2f-2). */
  def stage3(
      prepared: DataFrame,
      spec: ThreeStageSpec,
      sigma: Double,
      start: Array[Double],
      options: ThreeStageStage3Options = ThreeStageStage3Options(),
      offsetColumn: String = "stage1_offset"
  ): ThreeStageStage3 = {
    val (providers, clusters, cellData) = compressed(prepared, spec, options, offsetColumn)
    val cells = cellData.persist(options.blocks.resolvedStorageLevel)
    try {
      val bins = cells
        .map((c: ThreeStageKernel.Cell) => c.bins.length.toLong)(Encoders.scalaLong)
        .collect()
        .sum
      val fits = bins * (TaylorBins.Order + 1) * 8L <= options.blocks.driverBudgetBytes
      if (options.path == "driver" || (options.path == "auto" && fits)) {
        DriverGuards.requireWithinBudget(
          s"$bins compressed cell bins",
          bins * (TaylorBins.Order + 1) * 8L,
          options.blocks
        )
        stage3(
          ThreeStageCells(
            providers,
            clusters,
            cells.collect().sortBy(c => (c.cluster, c.provider))
          ),
          sigma,
          start,
          options
        )
      } else onExecutors(providers, clusters, cells, bins, sigma, start, options)
    } finally {
      cells.unpersist()
      ()
    }
  }

  /** Stage 3 with the compressed cells kept on executors in cluster-local blocks (spec 2f-2 §7). */
  private def onExecutors(
      providers: Vector[GroupKey],
      clusters: Vector[GroupKey],
      cells: Dataset[ThreeStageKernel.Cell],
      bins: Long,
      sigma: Double,
      start: Array[Double],
      options: ThreeStageStage3Options
  ): ThreeStageStage3 = {
    require(
      start.length == providers.size,
      s"the start has ${start.length} effects for ${providers.size} providers"
    )
    val sizes = cells
      .map((c: ThreeStageKernel.Cell) => ThreeStageClusterSize(c.cluster, c.bins.length.toLong))(
        Encoders.product[ThreeStageClusterSize]
      )
      .groupByKey((s: ThreeStageClusterSize) => s.cluster)(Encoders.scalaInt)
      .mapGroups((h: Int, rows: Iterator[ThreeStageClusterSize]) =>
        ThreeStageClusterSize(h, rows.map(_.bins).sum)
      )(
        Encoders.product[ThreeStageClusterSize]
      )
      .collect()
      .sortBy(_.cluster)
    val target = math.max(1L, options.blocks.targetBlockBytes / ((TaylorBins.Order + 1) * 8L))
    val blockOf = new Array[Int](clusters.size)
    var block = 0
    var filled = 0L
    sizes.foreach { s =>
      if (filled > 0L && filled + s.bins > target) {
        block += 1
        filled = 0L
      }
      blockOf(s.cluster) = block
      filled += s.bins
    }
    val assignment = blockOf.clone()
    val blocks = cells
      .groupByKey((c: ThreeStageKernel.Cell) => assignment(c.cluster))(Encoders.scalaInt)
      .mapGroups { (id: Int, rows: Iterator[ThreeStageKernel.Cell]) =>
        val sorted = rows.toArray.sortBy(c => (c.cluster, c.provider))
        ThreeStageClusterBlock(id, sorted.map(_.cluster).distinct, sorted)
      }(Encoders.product[ThreeStageClusterBlock])
      .persist(options.blocks.resolvedStorageLevel)
    try {
      val passes =
        new SparkThreeStagePasses(blocks, providers.size, clusters.size, sigma, options.nNodes)
      val r = ThreeStageKernel.fit(passes, sigma, start, options.kernel)
      ThreeStageStage3(
        providers,
        clusters,
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
        sizes.length,
        bins,
        options
      )
    } finally {
      blocks.unpersist()
      ()
    }
  }

  /** The prepared records with `fitted` = σ(γⱼ + E_post[aₕ] + o), stage 3's fitted probability (§2). */
  def fitted(
      prepared: DataFrame,
      spec: ThreeStageSpec,
      fit: ThreeStageStage3,
      offsetColumn: String = "stage1_offset"
  ): DataFrame = {
    val spark = prepared.sparkSession
    def table(
        keys: Vector[GroupKey],
        values: Array[Double],
        keyColumn: String,
        valueColumn: String,
        text: Boolean
    ): DataFrame =
      spark.createDataFrame(
        keys.indices.map(i => Row(GroupKey.value(keys(i)), values(i))).asJava,
        StructType(
          Seq(
            StructField(keyColumn, if (text) StringType else LongType, nullable = false),
            StructField(valueColumn, DoubleType, nullable = false)
          )
        )
      )
    val providerText = prepared.schema(spec.provider).dataType == StringType
    val clusterText = prepared.schema(spec.cluster).dataType == StringType
    val effects = table(fit.providers, fit.gamma, "__pprof_p", "__pprof_gamma", providerText)
    val alphas = table(fit.clusters, fit.alphaMean, "__pprof_h", "__pprof_alpha", clusterText)
    val provider = Validation.column(spec.provider)
    val cluster = Validation.column(spec.cluster)
    val eta = (col("__pprof_gamma") + col("__pprof_alpha")) + Validation.column(offsetColumn)
    prepared
      .join(
        broadcast(effects),
        (if (providerText) provider else provider.cast(LongType)) === col("__pprof_p")
      )
      .join(
        broadcast(alphas),
        (if (clusterText) cluster else cluster.cast(LongType)) === col("__pprof_h")
      )
      .withColumn("fitted", lit(1.0) / (lit(1.0) + org.apache.spark.sql.functions.exp(-eta)))
      .drop("__pprof_p", "__pprof_gamma", "__pprof_h", "__pprof_alpha")
  }

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

  /** Stage 2 on compressed cells (Phase 2f-3): the Laplace deviance's optimum over (σₚ, σ_c, μ). */
  def stage2(cells: ThreeStageCells, options: ThreeStageStage2Options): ThreeStageStage2 = {
    val m = cells.providers.size
    val h = cells.clusters.size
    val r = ThreeStageGlmm.fit(cells.cells, m, h, options.kernel)
    ThreeStageStage2(
      cells.providers,
      cells.clusters,
      r.sigmaProvider,
      r.sigmaCluster,
      r.mu,
      r.u.take(m).map(_ * r.sigmaProvider),
      r.u.drop(m).map(_ * r.sigmaCluster),
      r.deviance,
      r.converged,
      r.iterations,
      r.evaluations,
      r.projectedGradient
    )
  }

  def stage2(
      prepared: DataFrame,
      spec: ThreeStageSpec,
      options: ThreeStageStage2Options = ThreeStageStage2Options(),
      offsetColumn: String = "stage1_offset"
  ): ThreeStageStage2 =
    stage2(
      compress(prepared, spec, ThreeStageStage3Options(blocks = options.blocks), offsetColumn),
      options
    )

  /** The Laplace deviance and û (providers, then clusters) at fixed (σₚ, σ_c, μ). */
  def laplace(
      cells: ThreeStageCells,
      sigmaProvider: Double,
      sigmaCluster: Double,
      intercept: Double,
      options: ThreeStageStage2Options = ThreeStageStage2Options()
  ): (Double, Array[Double]) = {
    val e = ThreeStageGlmm.evaluate(
      cells.cells,
      cells.providers.size,
      cells.clusters.size,
      sigmaProvider,
      sigmaCluster,
      intercept,
      options.kernel
    )
    (e.deviance, e.u)
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
    val t = new ThreeStageKernel.InMemoryPasses(
      cells.cells,
      cells.providers.size,
      cells.clusters.size,
      sigma,
      nNodes
    )
      .full(gamma, new Array[Double](cells.clusters.size))
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
