package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  DataType,
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import pprof.spark.engine.backend.{BlockOptions, DriverLimitExceededException}
import pprof.spark.engine.layout.GroupKey
import pprof.spark.engine.metadata.SoftwareInfo
import pprof.spark.engine.skeleton.LayoutSummary
import pprof.spark.numerics.SerbinStep

/** Saves and loads [[LogisticFit]] as a self-describing directory (§6.10, PERS-1 to PERS-3): one JSON
  * record in `metadata`, with every double stored as its 64-bit pattern, and the fitted providers in
  * `providers` (Parquet, one row per provider, doubles exact). A loaded fit is bitwise identical to
  * the saved one on any JDK. Writing goes through Spark's public API, so it works on any file system
  * Spark reaches and under Spark Connect; an existing path is never overwritten. A directory of
  * another kind or format version fails to load with a message naming both (PERS-2).
  */
object LogisticFitIO {

  val FormatVersion: Int = 1
  val ReadableVersions: Set[Int] = Set(1)
  val Kind: String = "pprof.spark.engine.logistic.LogisticFit"

  private def field(name: String, dataType: DataType, nullable: Boolean = false) =
    StructField(name, dataType, nullable)

  private val Strings = ArrayType(StringType, containsNull = false)
  private val Longs = ArrayType(LongType, containsNull = false)
  private val Step = StructType(
    Seq(
      field("iteration", IntegerType),
      field("loglikBits", LongType),
      field("stepBits", LongType),
      field("betaChangeBits", LongType),
      field("evaluated", IntegerType)
    )
  )

  val MetadataSchema: StructType = StructType(
    Seq(
      field("formatVersion", IntegerType),
      field("kind", StringType),
      field("featureStatus", StringType),
      field("softwareKeys", Strings),
      field("softwareValues", Strings),
      field("outcomeCol", StringType),
      field("featureCols", Strings),
      field("providerCol", StringType),
      field("trialsCol", StringType, nullable = true),
      field("rowIdCol", StringType, nullable = true),
      field("tolBits", LongType),
      field("maxIter", IntegerType),
      field("boundBits", LongType),
      field("backtrack", BooleanType),
      field("screen", BooleanType),
      field("minRecords", LongType),
      field("confidenceLevelBits", LongType),
      field("maxProvidersOnDriver", IntegerType),
      field("aliasToleranceBits", LongType),
      field("targetBlockBytes", LongType),
      field("maxGroupsOnDriver", IntegerType),
      field("driverBudgetBytes", LongType),
      field("storageLevel", StringType),
      field("estimateBits", Longs),
      field("standardErrorBits", Longs),
      field("zBits", Longs),
      field("pValueBits", Longs),
      field("lowerBits", Longs),
      field("upperBits", Longs),
      field("covarianceBits", Longs),
      field("loglikBits", LongType),
      field("aicBits", LongType),
      field("bicBits", LongType),
      field("iterations", IntegerType),
      field("converged", BooleanType),
      field("criterionBits", LongType),
      field("lastStepBits", LongType),
      field("rows", LongType),
      field("trialsBits", LongType),
      field("eventsBits", LongType),
      field("warnings", Strings),
      field("trace", ArrayType(Step, containsNull = false)),
      field("providerKeyIsText", BooleanType),
      field("providers", IntegerType),
      field("excludedKeys", Strings),
      field("excludedRecords", Longs),
      field("targetRowsPerBlock", IntegerType),
      field("blockCount", IntegerType),
      field("oversizedGroups", IntegerType),
      field("largestBlockRows", LongType),
      field("smallestBlockRows", LongType),
      field("fingerprint", LongType)
    )
  )

  /** One row per fitted provider; `position` is its index in key order. */
  val ProviderSchema: StructType = StructType(
    Seq(
      field("position", IntegerType),
      field("key", StringType),
      field("gamma", DoubleType),
      field("var_gamma", DoubleType),
      field("var_case_mix", DoubleType),
      field("records", LongType),
      field("events", DoubleType),
      field("trials", DoubleType),
      field("at_bound", BooleanType)
    )
  )

  private def bit(value: Double): Long = java.lang.Double.doubleToRawLongBits(value)
  private def bits(values: Seq[Double]): Seq[Long] = values.map(bit)
  private def keyText(key: GroupKey): String = GroupKey.value(key).toString

  /** Writes `fit` under `path`, which must not exist yet. */
  def save(spark: SparkSession, fit: LogisticFit, path: String): Unit = {
    val software = fit.software.toFields
    val c = fit.coefficients
    val o = fit.options
    val p = fit.providers
    val record = Row(
      FormatVersion,
      Kind,
      fit.status,
      software.map(_._1),
      software.map(_._2),
      fit.spec.outcome,
      fit.spec.features,
      fit.spec.provider,
      fit.spec.trials.orNull,
      fit.spec.rowId.orNull,
      bit(o.tol),
      o.maxIter,
      bit(o.bound),
      o.backtrack,
      o.screen,
      o.minRecords,
      bit(o.confidenceLevel),
      o.maxProvidersOnDriver,
      bit(o.aliasTolerance),
      o.blocks.targetBlockBytes,
      o.blocks.maxGroupsOnDriver,
      o.blocks.driverBudgetBytes,
      o.blocks.storageLevel,
      bits(c.map(_.estimate)),
      bits(c.map(_.standardError)),
      bits(c.map(_.z)),
      bits(c.map(_.pValue)),
      bits(c.map(_.lower)),
      bits(c.map(_.upper)),
      bits(fit.covariance),
      bit(fit.loglik),
      bit(fit.aic),
      bit(fit.bic),
      fit.iterations,
      fit.converged,
      bit(fit.criterion),
      bit(fit.lastStep),
      fit.rows,
      bit(fit.trials),
      bit(fit.events),
      fit.warnings,
      fit.trace.map(s =>
        Row(s.iteration, bit(s.loglik), bit(s.step), bit(s.betaChange), s.evaluated)
      ),
      p.keys.headOption.exists(_.isInstanceOf[GroupKey.Text]),
      p.size,
      fit.excluded.map(e => keyText(e._1)),
      fit.excluded.map(_._2),
      fit.layout.targetRowsPerBlock,
      fit.layout.blockCount,
      fit.layout.oversizedGroups,
      fit.layout.largestBlockRows,
      fit.layout.smallestBlockRows,
      fit.fingerprint
    )
    spark
      .createDataFrame(java.util.List.of(record), MetadataSchema)
      .coalesce(1)
      .write
      .mode("errorifexists")
      .json(s"$path/metadata")
    val rows = (0 until p.size).map { k =>
      Row(
        k,
        keyText(p.keys(k)),
        p.gamma(k),
        p.varGamma(k),
        p.varCaseMix(k),
        p.records(k),
        p.events(k),
        p.trials(k),
        p.atBound(k)
      )
    }
    spark
      .createDataFrame(rows.asJava, ProviderSchema)
      .coalesce(1)
      .write
      .mode("errorifexists")
      .parquet(s"$path/providers")
  }

  private def metadata(spark: SparkSession, path: String): Row = {
    val records = spark.read.schema(MetadataSchema).json(s"$path/metadata").collect()
    require(
      records.length == 1,
      s"expected one metadata record under $path, found ${records.length}"
    )
    val r = records.head
    val kind = Option(r.getAs[String]("kind"))
    val version = Option(r.getAs[Any]("formatVersion"))
    require(
      kind.contains(Kind) && version.exists(v => ReadableVersions.exists(_ == v)),
      s"$path holds ${kind.getOrElse("an unknown kind")} of format version " +
        s"${version.getOrElse("unknown")}; this release reads $Kind of format versions " +
        s"${ReadableVersions.min} to ${ReadableVersions.max}"
    )
    r
  }

  /** Reads a fit written by [[save]]; every double comes back bit for bit (PERS-1). The providers
    * are collected to the driver under the saved `maxProvidersOnDriver` (DIST-1).
    */
  def load(spark: SparkSession, path: String): LogisticFit = {
    val r = metadata(spark, path)
    def seq[A](name: String): Seq[A] = r.getSeq[A](r.fieldIndex(name)).toSeq
    def double(name: String): Double = java.lang.Double.longBitsToDouble(r.getAs[Long](name))
    def doubles(name: String): Vector[Double] =
      seq[Long](name).iterator.map(java.lang.Double.longBitsToDouble).toVector
    val text = r.getAs[Boolean]("providerKeyIsText")
    def key(value: String): GroupKey =
      if (text) GroupKey.Text(value) else GroupKey.Integral(value.toLong)
    val options = LogisticOptions(
      tol = double("tolBits"),
      maxIter = r.getAs[Int]("maxIter"),
      bound = double("boundBits"),
      backtrack = r.getAs[Boolean]("backtrack"),
      screen = r.getAs[Boolean]("screen"),
      minRecords = r.getAs[Long]("minRecords"),
      confidenceLevel = double("confidenceLevelBits"),
      maxProvidersOnDriver = r.getAs[Int]("maxProvidersOnDriver"),
      aliasTolerance = double("aliasToleranceBits"),
      blocks = BlockOptions(
        r.getAs[Long]("targetBlockBytes"),
        r.getAs[Int]("maxGroupsOnDriver"),
        r.getAs[Long]("driverBudgetBytes"),
        r.getAs[String]("storageLevel")
      )
    )
    val count = r.getAs[Int]("providers")
    if (count > options.maxProvidersOnDriver)
      throw new DriverLimitExceededException(
        s"$path holds $count providers, more than maxProvidersOnDriver = ${options.maxProvidersOnDriver}"
      )
    val rows =
      spark.read.schema(ProviderSchema).parquet(s"$path/providers").collect().sortBy(_.getInt(0))
    require(
      rows.length == count && rows.indices.forall(k => rows(k).getInt(0) == k),
      s"$path/providers holds ${rows.length} providers; the metadata records $count"
    )
    val providers = new LogisticProviders(
      rows.map(row => key(row.getString(1))),
      rows.map(_.getDouble(2)),
      rows.map(_.getDouble(3)),
      rows.map(_.getDouble(4)),
      rows.map(_.getLong(5)),
      rows.map(_.getDouble(6)),
      rows.map(_.getDouble(7)),
      rows.map(_.getBoolean(8))
    )
    val names = seq[String]("featureCols")
    val estimate = doubles("estimateBits")
    val se = doubles("standardErrorBits")
    val z = doubles("zBits")
    val pValue = doubles("pValueBits")
    val lower = doubles("lowerBits")
    val upper = doubles("upperBits")
    val steps = r.getSeq[Row](r.fieldIndex("trace")).map { s =>
      SerbinStep(
        s.getInt(0),
        java.lang.Double.longBitsToDouble(s.getLong(1)),
        java.lang.Double.longBitsToDouble(s.getLong(2)),
        java.lang.Double.longBitsToDouble(s.getLong(3)),
        s.getInt(4)
      )
    }
    LogisticFit(
      names.indices
        .map(j =>
          LogisticCoefficient(names(j), estimate(j), se(j), z(j), pValue(j), lower(j), upper(j))
        )
        .toVector,
      doubles("covarianceBits"),
      providers,
      seq[String]("excludedKeys").map(key).zip(seq[Long]("excludedRecords")).toVector,
      double("loglikBits"),
      double("aicBits"),
      double("bicBits"),
      r.getAs[Int]("iterations"),
      r.getAs[Boolean]("converged"),
      double("criterionBits"),
      double("lastStepBits"),
      r.getAs[Long]("rows"),
      double("trialsBits"),
      double("eventsBits"),
      seq[String]("warnings").toVector,
      steps.toVector,
      LogisticSpec(
        r.getAs[String]("outcomeCol"),
        names,
        r.getAs[String]("providerCol"),
        Option(r.getAs[String]("trialsCol")),
        Option(r.getAs[String]("rowIdCol"))
      ),
      options,
      SoftwareInfo.fromFields(seq[String]("softwareKeys").zip(seq[String]("softwareValues"))),
      LayoutSummary(
        r.getAs[Int]("targetRowsPerBlock"),
        r.getAs[Int]("blockCount"),
        r.getAs[Int]("oversizedGroups"),
        r.getAs[Long]("largestBlockRows"),
        r.getAs[Long]("smallestBlockRows")
      ),
      r.getAs[Long]("fingerprint"),
      r.getAs[String]("featureStatus")
    )
  }
}
