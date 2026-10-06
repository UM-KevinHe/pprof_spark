package pprof.spark.engine.cox

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  DataType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}

import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.metadata.SoftwareInfo
import pprof.spark.engine.skeleton.LayoutSummary
import pprof.spark.numerics.IterationRecord

/** Saves and loads [[CoxFit]] as a self-describing directory (§6.10, PERS-1 to PERS-3). `metadata`
  * holds one JSON record with every double stored as its 64-bit pattern, so a loaded fit is bitwise
  * identical to the saved one on any JDK. Writing goes through Spark's public API, so it works on
  * any file system Spark reaches and under Spark Connect, and an existing path is never
  * overwritten. A directory of another kind or format version fails to load with a message that
  * says so (PERS-2).
  */
object CoxFitIO {

  val FormatVersion: Int = 2

  /** Versions this release reads: 1 lacks `entryCol` and `hasBaseline`. */
  val ReadableVersions: Set[Int] = Set(1, 2)
  val Kind: String = "pprof.spark.engine.cox.CoxFit"

  private def field(name: String, dataType: DataType, nullable: Boolean = false) =
    StructField(name, dataType, nullable)

  private val Strings = ArrayType(StringType, containsNull = false)
  private val Longs = ArrayType(LongType, containsNull = false)
  private val Iteration = StructType(
    Seq(
      field("iteration", IntegerType),
      field("betaBits", Longs),
      field("valueBits", LongType),
      field("halvings", IntegerType)
    )
  )

  val MetadataSchema: StructType = StructType(
    Seq(
      field("formatVersion", IntegerType),
      field("kind", StringType),
      field("featureStatus", StringType),
      field("softwareKeys", Strings),
      field("softwareValues", Strings),
      field("timeCol", StringType),
      field("eventCol", StringType),
      field("featureCols", Strings),
      field("strataCol", StringType, nullable = true),
      field("rowIdCol", StringType, nullable = true),
      field("weightCol", StringType, nullable = true),
      field("offsetCol", StringType, nullable = true),
      field("entryCol", StringType, nullable = true),
      field("hasBaseline", BooleanType, nullable = true),
      field("ties", StringType),
      field("maxIterations", IntegerType),
      field("epsBits", LongType),
      field("maxHalvings", IntegerType),
      field("aliasToleranceBits", LongType),
      field("confidenceLevelBits", LongType),
      field("maxStratumRows", LongType),
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
      field("informationBits", Longs),
      field("logLikelihoodBits", LongType),
      field("logLikelihoodNullBits", LongType),
      field("iterations", IntegerType),
      field("halvings", IntegerType),
      field("converged", BooleanType),
      field("message", StringType),
      field("observations", LongType),
      field("events", LongType),
      field("strata", IntegerType),
      field("strataWithoutEvents", IntegerType),
      field("warnings", Strings),
      field("iterationLog", ArrayType(Iteration, containsNull = false)),
      field("targetRowsPerBlock", IntegerType),
      field("blockCount", IntegerType),
      field("oversizedGroups", IntegerType),
      field("largestBlockRows", LongType),
      field("smallestBlockRows", LongType),
      field("fingerprint", LongType)
    )
  )

  private def bit(value: Double): Long = java.lang.Double.doubleToRawLongBits(value)

  private def bits(values: Seq[Double]): Seq[Long] = values.map(bit)

  /** Writes `fit`, and its baseline table when given (Parquet, §6.10), under `path`, which must not
    * exist yet.
    */
  def save(
      spark: SparkSession,
      fit: CoxFit,
      path: String,
      baseline: Option[DataFrame] = None
  ): Unit = {
    val software = fit.software.toFields
    val c = fit.coefficients
    val o = fit.options
    val record = Row(
      FormatVersion,
      Kind,
      fit.featureStatus,
      software.map(_._1),
      software.map(_._2),
      fit.spec.time,
      fit.spec.event,
      fit.spec.features,
      fit.spec.strata.orNull,
      fit.spec.rowId.orNull,
      fit.spec.weight.orNull,
      fit.spec.offset.orNull,
      fit.spec.entry.orNull,
      baseline.isDefined,
      o.ties.name,
      o.maxIterations,
      bit(o.eps),
      o.maxHalvings,
      bit(o.aliasTolerance),
      bit(o.confidenceLevel),
      o.maxStratumRows,
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
      bits(fit.information),
      bit(fit.logLikelihood),
      bit(fit.logLikelihoodNull),
      fit.iterations,
      fit.halvings,
      fit.converged,
      fit.message,
      fit.observations,
      fit.events,
      fit.strata,
      fit.strataWithoutEvents,
      fit.warnings,
      fit.iterationLog.map(i => Row(i.iteration, bits(i.beta), bit(i.value), i.halvings)),
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
    baseline.foreach(_.coalesce(1).write.mode("errorifexists").parquet(s"$path/baseline"))
  }

  /** The baseline table saved with the fit, if any. */
  def loadBaseline(spark: SparkSession, path: String): Option[DataFrame] = {
    val record = metadata(spark, path)
    if (Option(record.getAs[Any]("hasBaseline")).contains(true))
      Some(spark.read.parquet(s"$path/baseline"))
    else None
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
        s"${ReadableVersions.toSeq.sorted.mkString(" and ")}"
    )
    r
  }

  /** Reads a fit written by [[save]]; every double comes back bit for bit (PERS-1). */
  def load(spark: SparkSession, path: String): CoxFit = {
    val r = metadata(spark, path)
    def seq[A](name: String): Seq[A] = r.getSeq[A](r.fieldIndex(name)).toSeq
    def double(name: String): Double = java.lang.Double.longBitsToDouble(r.getAs[Long](name))
    def doubles(name: String): Vector[Double] =
      seq[Long](name).iterator.map(java.lang.Double.longBitsToDouble).toVector
    def optional(name: String): Option[String] = Option(r.getAs[String](name))
    val spec = CoxSpec(
      r.getAs[String]("timeCol"),
      r.getAs[String]("eventCol"),
      seq[String]("featureCols"),
      optional("strataCol"),
      optional("rowIdCol"),
      optional("weightCol"),
      optional("offsetCol"),
      optional("entryCol")
    )
    val options = CoxOptions(
      Ties.fromName(r.getAs[String]("ties")),
      r.getAs[Int]("maxIterations"),
      double("epsBits"),
      r.getAs[Int]("maxHalvings"),
      double("aliasToleranceBits"),
      double("confidenceLevelBits"),
      r.getAs[Long]("maxStratumRows"),
      BlockOptions(
        r.getAs[Long]("targetBlockBytes"),
        r.getAs[Int]("maxGroupsOnDriver"),
        r.getAs[Long]("driverBudgetBytes"),
        r.getAs[String]("storageLevel")
      )
    )
    val (estimate, se, z, p, lower, upper) = (
      doubles("estimateBits"),
      doubles("standardErrorBits"),
      doubles("zBits"),
      doubles("pValueBits"),
      doubles("lowerBits"),
      doubles("upperBits")
    )
    val coefficients = spec.features.indices.map { j =>
      CoxCoefficient(spec.features(j), estimate(j), se(j), z(j), p(j), lower(j), upper(j))
    }.toVector
    val log = seq[Row]("iterationLog").map { i =>
      IterationRecord(
        i.getAs[Int]("iteration"),
        i.getSeq[Long](i.fieldIndex("betaBits"))
          .iterator
          .map(java.lang.Double.longBitsToDouble)
          .toVector,
        java.lang.Double.longBitsToDouble(i.getAs[Long]("valueBits")),
        i.getAs[Int]("halvings")
      )
    }.toVector
    CoxFit(
      coefficients,
      doubles("covarianceBits"),
      doubles("informationBits"),
      double("logLikelihoodBits"),
      double("logLikelihoodNullBits"),
      r.getAs[Int]("iterations"),
      r.getAs[Int]("halvings"),
      r.getAs[Boolean]("converged"),
      r.getAs[String]("message"),
      r.getAs[Long]("observations"),
      r.getAs[Long]("events"),
      r.getAs[Int]("strata"),
      r.getAs[Int]("strataWithoutEvents"),
      seq[String]("warnings").toVector,
      log,
      spec,
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
