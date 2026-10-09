package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import pprof.spark.engine.layout.GroupKey

/** A saved three-stage fit: everything but the records, which `ThreeStagePipeline.attach` re-joins. */
final case class ThreeStageSaved(
    spec: ThreeStageSpec,
    options: ThreeStageFitOptions,
    stage1: LogisticFit,
    stage2: ThreeStageStage2,
    stage3: ThreeStageStage3,
    providers: Long,
    clusters: Long,
    cells: Long,
    includedCells: Long,
    excluded: Vector[(GroupKey, Long)]
)

/** Persistence of three-stage fits (docs/spec/logistic/three-stage-pipeline.md §5): `metadata` (JSON, doubles as
  * 64-bit patterns), `stage1` (LogisticFitIO), and `providers` and `clusters` as Parquet; never overwriting; a
  * bit-for-bit round trip.
  */
object ThreeStageFitIO {

  val Kind: String = "ThreeStageFit"
  val FormatVersion: Int = 1

  private def bit(v: Double): Long = java.lang.Double.doubleToRawLongBits(v)
  private def unbit(v: Long): Double = java.lang.Double.longBitsToDouble(v)
  private def f(name: String, t: DataType, nullable: Boolean = false) =
    StructField(name, t, nullable)

  private val Metadata = StructType(
    Seq(
      f("kind", StringType),
      f("formatVersion", IntegerType),
      f("outcome", StringType),
      f("features", ArrayType(StringType, false)),
      f("provider", StringType),
      f("cluster", StringType),
      f("rowId", StringType, nullable = true),
      f("cutoff", LongType),
      f("providerText", BooleanType),
      f("clusterText", BooleanType),
      f("providers", LongType),
      f("clusters", LongType),
      f("cells", LongType),
      f("includedCells", LongType),
      f("excludedKeys", ArrayType(StringType, false)),
      f("excludedSizes", ArrayType(LongType, false)),
      f("pirlsTolerance", LongType),
      f("pirlsMaxIterations", IntegerType),
      f("gradientTolerance", LongType),
      f("maxIterations", IntegerType),
      f("sigmaProvider", LongType),
      f("sigmaCluster", LongType),
      f("intercept", LongType),
      f("deviance", LongType),
      f("stage2Converged", BooleanType),
      f("stage2Iterations", IntegerType),
      f("stage2Evaluations", IntegerType),
      f("projectedGradient", LongType),
      f("nNodes", IntegerType),
      f("maxIter", IntegerType),
      f("tol", LongType),
      f("bound", LongType),
      f("boundMode", StringType),
      f("stage3Sigma", LongType),
      f("stage3Converged", BooleanType),
      f("stalled", BooleanType),
      f("stage3Iterations", IntegerType),
      f("criterion", LongType),
      f("loglik", LongType),
      f("stage3Cells", IntegerType),
      f("bins", LongType)
    )
  )

  private val Providers = StructType(
    Seq(
      f("index", IntegerType),
      f("key", StringType),
      f("gamma", DoubleType),
      f("blup", DoubleType),
      f("held", BooleanType)
    )
  )

  private val Clusters = StructType(
    Seq(
      f("index", IntegerType),
      f("key", StringType),
      f("alphaMean", DoubleType),
      f("alphaVar", DoubleType),
      f("blup", DoubleType)
    )
  )

  def save(spark: SparkSession, fit: ThreeStageFit, path: String): Unit = {
    val out = path.stripSuffix("/")
    val s2 = fit.stage2
    val s3 = fit.stage3
    val o2 = fit.options.stage2
    val o3 = fit.options.stage3
    val p = fit.preparation
    val text = (keys: Vector[GroupKey]) => keys.headOption.exists(_.isInstanceOf[GroupKey.Text])
    val row = Row(
      Kind,
      FormatVersion,
      fit.spec.outcome,
      fit.spec.features,
      fit.spec.provider,
      fit.spec.cluster,
      fit.spec.rowId.orNull,
      fit.options.preparation.cutoff,
      text(s3.providers),
      text(s3.clusters),
      p.providers,
      p.clusters,
      p.cells,
      p.includedCells,
      p.excluded.map(e => GroupKey.value(e._1).toString),
      p.excluded.map(_._2),
      bit(o2.pirlsTolerance),
      o2.pirlsMaxIterations,
      bit(o2.gradientTolerance),
      o2.maxIterations,
      bit(s2.sigmaProvider),
      bit(s2.sigmaCluster),
      bit(s2.intercept),
      bit(s2.deviance),
      s2.converged,
      s2.iterations,
      s2.evaluations,
      bit(s2.projectedGradient),
      o3.nNodes,
      o3.maxIter,
      bit(o3.tol),
      bit(o3.bound),
      o3.boundMode,
      bit(s3.sigma),
      s3.converged,
      s3.stalled,
      s3.iterations,
      bit(s3.criterion),
      bit(s3.loglik),
      s3.cells,
      s3.bins
    )
    spark
      .createDataFrame(java.util.List.of(row), Metadata)
      .coalesce(1)
      .write
      .mode("errorifexists")
      .json(s"$out/metadata")
    LogisticFitIO.save(spark, p.stage1, s"$out/stage1")
    spark
      .createDataFrame(
        s3.providers.indices.map { j =>
          Row(
            j,
            GroupKey.value(s3.providers(j)).toString,
            s3.gamma(j),
            s2.blupProviders(j),
            s3.held.nonEmpty && s3.held(j)
          )
        }.asJava,
        Providers
      )
      .write
      .mode("errorifexists")
      .parquet(s"$out/providers")
    spark
      .createDataFrame(
        s3.clusters.indices.map { h =>
          Row(
            h,
            GroupKey.value(s3.clusters(h)).toString,
            s3.alphaMean(h),
            s3.alphaVar(h),
            s2.blupClusters(h)
          )
        }.asJava,
        Clusters
      )
      .write
      .mode("errorifexists")
      .parquet(s"$out/clusters")
  }

  def load(spark: SparkSession, path: String): ThreeStageSaved = {
    val out = path.stripSuffix("/")
    val m = spark.read.schema(Metadata).json(s"$out/metadata").collect().head
    val kind = m.getAs[String]("kind")
    val version = m.getAs[Int]("formatVersion")
    require(
      kind == Kind && version == FormatVersion,
      s"$path holds $kind format version $version; this release reads $Kind format version $FormatVersion"
    )
    def key(text: Boolean)(s: String): GroupKey =
      if (text) GroupKey.Text(s) else GroupKey.Integral(s.toLong)
    val providerKey = key(m.getAs[Boolean]("providerText")) _
    val clusterKey = key(m.getAs[Boolean]("clusterText")) _
    val providers =
      spark.read.schema(Providers).parquet(s"$out/providers").collect().sortBy(_.getInt(0))
    val clusters =
      spark.read.schema(Clusters).parquet(s"$out/clusters").collect().sortBy(_.getInt(0))
    val pk = providers.map(r => providerKey(r.getString(1))).toVector
    val ck = clusters.map(r => clusterKey(r.getString(1))).toVector
    val stage1 = LogisticFitIO.load(spark, s"$out/stage1")
    val o2 = ThreeStageStage2Options(
      unbit(m.getAs[Long]("pirlsTolerance")),
      m.getAs[Int]("pirlsMaxIterations"),
      unbit(m.getAs[Long]("gradientTolerance")),
      m.getAs[Int]("maxIterations")
    )
    val o3 = ThreeStageStage3Options(
      m.getAs[Int]("nNodes"),
      m.getAs[Int]("maxIter"),
      unbit(m.getAs[Long]("tol")),
      unbit(m.getAs[Long]("bound")),
      m.getAs[String]("boundMode")
    )
    val spec = ThreeStageSpec(
      m.getAs[String]("outcome"),
      m.getSeq[String](m.fieldIndex("features")),
      m.getAs[String]("provider"),
      m.getAs[String]("cluster"),
      Option(m.getAs[String]("rowId"))
    )
    val options =
      ThreeStageFitOptions(ThreeStageOptions(m.getAs[Long]("cutoff"), stage1.options), o2, o3)
    val stage2 = ThreeStageStage2(
      pk,
      ck,
      unbit(m.getAs[Long]("sigmaProvider")),
      unbit(m.getAs[Long]("sigmaCluster")),
      unbit(m.getAs[Long]("intercept")),
      providers.map(_.getDouble(3)),
      clusters.map(_.getDouble(4)),
      unbit(m.getAs[Long]("deviance")),
      m.getAs[Boolean]("stage2Converged"),
      m.getAs[Int]("stage2Iterations"),
      m.getAs[Int]("stage2Evaluations"),
      unbit(m.getAs[Long]("projectedGradient"))
    )
    val stage3 = ThreeStageStage3(
      pk,
      ck,
      providers.map(_.getDouble(2)),
      unbit(m.getAs[Long]("stage3Sigma")),
      m.getAs[Boolean]("stage3Converged"),
      m.getAs[Boolean]("stalled"),
      m.getAs[Int]("stage3Iterations"),
      unbit(m.getAs[Long]("criterion")),
      unbit(m.getAs[Long]("loglik")),
      clusters.map(_.getDouble(2)),
      clusters.map(_.getDouble(3)),
      providers.map(_.getBoolean(4)),
      m.getAs[Int]("stage3Cells"),
      m.getAs[Long]("bins"),
      o3
    )
    val excluded = m
      .getSeq[String](m.fieldIndex("excludedKeys"))
      .zip(m.getSeq[Long](m.fieldIndex("excludedSizes")))
      .map { case (k, n) => providerKey(k) -> n }
      .toVector
    ThreeStageSaved(
      spec,
      options,
      stage1,
      stage2,
      stage3,
      m.getAs[Long]("providers"),
      m.getAs[Long]("clusters"),
      m.getAs[Long]("cells"),
      m.getAs[Long]("includedCells"),
      excluded
    )
  }

  /** The saved fit with `df`'s records re-attached; `df` must be the training data (stage 1's fingerprint, API-3). */
  def attach(df: DataFrame, saved: ThreeStageSaved): ThreeStageFit = {
    val prepared = ThreeStage.prepare(df, saved.spec, saved.options.preparation)
    require(
      prepared.stage1.fingerprint == saved.stage1.fingerprint,
      "the data differ from the saved fit's (stage 1 fingerprint); attach needs the training data (API-3)"
    )
    val cells = ThreeStage.compress(prepared.data, saved.spec, saved.options.stage3)
    ThreeStageFit(prepared, cells, saved.stage2, saved.stage3, saved.spec, saved.options)
  }
}
