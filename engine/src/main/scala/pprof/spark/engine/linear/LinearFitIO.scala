package pprof.spark.engine.linear

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{
  ArrayType,
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

/** Persistence of linear fits (docs/spec/linear/fixed-effect-estimation.md §8), format version 1: `metadata` (one JSON
  * record, every double as its 64-bit pattern) and `providers` (the provider table in Parquet, whose doubles are
  * exact); never overwriting; a bit-for-bit round trip. Other kinds and versions are refused, naming both.
  */
object LinearFitIO {

  val FormatVersion: Int = 1
  val ReadableVersions: Set[Int] = Set(1)
  val Kind: String = "pprof.spark.engine.linear.LinearFit"

  private def field(name: String, dataType: DataType, nullable: Boolean = false) =
    StructField(name, dataType, nullable)

  private val Strings = ArrayType(StringType, containsNull = false)
  private val Longs = ArrayType(LongType, containsNull = false)

  val MetadataSchema: StructType = StructType(
    Seq(
      field("formatVersion", IntegerType),
      field("kind", StringType),
      field("status", StringType),
      field("softwareKeys", Strings),
      field("softwareValues", Strings),
      field("outcomeCol", StringType),
      field("featureCols", Strings),
      field("providerCol", StringType),
      field("rowIdCol", StringType, nullable = true),
      field("varianceOption", StringType),
      field("targetBlockBytes", LongType),
      field("maxGroupsOnDriver", IntegerType),
      field("driverBudgetBytes", LongType),
      field("storageLevel", StringType),
      field("estimateBits", Longs),
      field("standardErrorBits", Longs),
      field("covarianceBits", Longs),
      field("sigmaBits", LongType),
      field("rssBits", LongType),
      field("tssBits", LongType),
      field("loglikBits", LongType),
      field("aicBits", LongType),
      field("bicBits", LongType),
      field("conditionBits", LongType),
      field("rows", LongType),
      field("providers", IntegerType),
      field("degreesOfFreedom", LongType),
      field("warnings", Strings),
      field("targetRowsPerBlock", IntegerType),
      field("blockCount", IntegerType),
      field("oversizedGroups", IntegerType),
      field("largestBlockRows", LongType),
      field("smallestBlockRows", LongType),
      field("fingerprint", LongType)
    )
  )

  private def bit(value: Double): Long = java.lang.Double.doubleToRawLongBits(value)

  def save(spark: SparkSession, fit: LinearFit, path: String): Unit = {
    val software = fit.software.toFields
    val o = fit.options
    val l = fit.layout
    val record = Row(
      FormatVersion,
      Kind,
      fit.status,
      software.map(_._1),
      software.map(_._2),
      fit.spec.outcome,
      fit.spec.features,
      fit.spec.provider,
      fit.spec.rowId.orNull,
      o.varianceOption,
      o.blocks.targetBlockBytes,
      o.blocks.maxGroupsOnDriver,
      o.blocks.driverBudgetBytes,
      o.blocks.storageLevel,
      fit.coefficients.map(c => bit(c.estimate)),
      fit.coefficients.map(c => bit(c.standardError)),
      fit.covariance.map(bit),
      bit(fit.sigma),
      bit(fit.rss),
      bit(fit.tss),
      bit(fit.loglik),
      bit(fit.aic),
      bit(fit.bic),
      bit(fit.condition),
      fit.rows,
      fit.providerCount,
      fit.degreesOfFreedom,
      fit.warnings,
      l.targetRowsPerBlock,
      l.blockCount,
      l.oversizedGroups,
      l.largestBlockRows,
      l.smallestBlockRows,
      fit.fingerprint
    )
    spark
      .createDataFrame(java.util.List.of(record), MetadataSchema)
      .coalesce(1)
      .write
      .mode("errorifexists")
      .json(s"$path/metadata")
    fit.providers.coalesce(1).write.mode("errorifexists").parquet(s"$path/providers")
  }

  def load(spark: SparkSession, path: String): LinearFit = {
    val records = spark.read.schema(MetadataSchema).json(s"$path/metadata").collect()
    require(records.length == 1, s"$path/metadata holds ${records.length} records; expected one")
    val r = records.head
    val kind = Option(r.getAs[String]("kind"))
    val version = Option(r.getAs[Any]("formatVersion"))
    require(
      kind.contains(Kind) && version.exists(v => ReadableVersions.exists(_ == v)),
      s"$path holds ${kind.getOrElse("an unknown kind")} of format version ${version.getOrElse("unknown")}; " +
        s"this release reads $Kind of format version $FormatVersion"
    )
    def seq[A](name: String): Seq[A] = r.getSeq[A](r.fieldIndex(name)).toSeq
    def double(name: String): Double = java.lang.Double.longBitsToDouble(r.getAs[Long](name))
    def doubles(name: String): Vector[Double] =
      seq[Long](name).iterator.map(java.lang.Double.longBitsToDouble).toVector
    val spec = LinearSpec(
      r.getAs[String]("outcomeCol"),
      seq[String]("featureCols"),
      r.getAs[String]("providerCol"),
      Option(r.getAs[String]("rowIdCol"))
    )
    val options = LinearOptions(
      r.getAs[String]("varianceOption"),
      BlockOptions(
        r.getAs[Long]("targetBlockBytes"),
        r.getAs[Int]("maxGroupsOnDriver"),
        r.getAs[Long]("driverBudgetBytes"),
        r.getAs[String]("storageLevel")
      )
    )
    val count = r.getAs[Int]("providers")
    val providers = spark.read
      .parquet(s"$path/providers")
      .select(col(spec.provider), col("records"), col("gamma"), col("variance"), col("se"))
      .orderBy(spec.provider)
      .persist(options.blocks.resolvedStorageLevel)
    val stored = providers.count()
    require(
      stored == count,
      s"$path/providers holds $stored providers; the metadata records $count"
    )
    val estimates = doubles("estimateBits")
    val standardErrors = doubles("standardErrorBits")
    LinearFit(
      spec.features.indices
        .map(i => LinearCoefficient(spec.features(i), estimates(i), standardErrors(i)))
        .toVector,
      doubles("covarianceBits"),
      providers,
      double("sigmaBits"),
      double("rssBits"),
      double("tssBits"),
      r.getAs[Long]("rows"),
      count,
      r.getAs[Long]("degreesOfFreedom"),
      double("loglikBits"),
      double("aicBits"),
      double("bicBits"),
      double("conditionBits"),
      seq[String]("warnings").toVector,
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
      r.getAs[String]("status")
    )
  }
}
