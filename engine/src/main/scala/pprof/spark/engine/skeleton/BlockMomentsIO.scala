package pprof.spark.engine.skeleton

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{
  ArrayType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}

import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.data.InputSpec
import pprof.spark.engine.metadata.SoftwareInfo

/** Saves and loads [[BlockMomentsResult]] as a self-describing directory (§6.10): `metadata` holds
  * one JSON record, with every double stored as its 64-bit pattern so that values round-trip
  * exactly on any JDK (PERS-1); `groups` holds the group table as Parquet. Writing goes through
  * Spark, so it works on any file system Spark can reach and under Spark Connect.
  */
object BlockMomentsIO {

  val FormatVersion: Int = 1
  val Kind: String = "pprof.spark.engine.skeleton.BlockMoments"

  private def field(
      name: String,
      dataType: org.apache.spark.sql.types.DataType,
      nullable: Boolean = false
  ) =
    StructField(name, dataType, nullable)

  private val Strings = ArrayType(StringType, containsNull = false)
  private val Longs = ArrayType(LongType, containsNull = false)

  val MetadataSchema: StructType = StructType(
    Seq(
      field("formatVersion", IntegerType),
      field("kind", StringType),
      field("resultSchemaVersion", IntegerType),
      field("softwareKeys", Strings),
      field("softwareValues", Strings),
      field("groupCol", StringType),
      field("featureCols", Strings),
      field("rowIdCol", StringType, nullable = true),
      field("targetBlockBytes", LongType),
      field("maxGroupsOnDriver", IntegerType),
      field("driverBudgetBytes", LongType),
      field("storageLevel", StringType),
      field("rowCount", LongType),
      field("groupCount", IntegerType),
      field("fingerprint", LongType),
      field("targetRowsPerBlock", IntegerType),
      field("blockCount", IntegerType),
      field("oversizedGroups", IntegerType),
      field("largestBlockRows", LongType),
      field("smallestBlockRows", LongType),
      field("columnSumsBits", Longs),
      field("crossProductsBits", Longs)
    )
  )

  /** Writes `result` under `path`, which must not exist yet. */
  def save(result: BlockMomentsResult, path: String): Unit = {
    val s = result.summary
    val software = s.software.toFields
    val record = Row(
      FormatVersion,
      Kind,
      BlockMoments.ResultSchemaVersion,
      software.map(_._1),
      software.map(_._2),
      s.spec.groupCol,
      s.spec.featureCols,
      s.spec.rowIdCol.orNull,
      s.options.targetBlockBytes,
      s.options.maxGroupsOnDriver,
      s.options.driverBudgetBytes,
      s.options.storageLevel,
      s.rowCount,
      s.groupCount,
      s.fingerprint,
      s.layout.targetRowsPerBlock,
      s.layout.blockCount,
      s.layout.oversizedGroups,
      s.layout.largestBlockRows,
      s.layout.smallestBlockRows,
      s.columnSums.toSeq.map(java.lang.Double.doubleToRawLongBits),
      s.crossProducts.toSeq.map(java.lang.Double.doubleToRawLongBits)
    )
    val spark = result.groupTable.sparkSession
    spark
      .createDataFrame(java.util.List.of(record), MetadataSchema)
      .coalesce(1)
      .write
      .mode("errorifexists")
      .json(s"$path/metadata")
    result.groupTable.coalesce(1).write.mode("errorifexists").parquet(s"$path/groups")
  }

  /** Reads a result written by [[save]]; doubles come back bit for bit. */
  def load(spark: SparkSession, path: String): BlockMomentsResult = {
    val records = spark.read.schema(MetadataSchema).json(s"$path/metadata").collect()
    require(
      records.length == 1,
      s"expected one metadata record under $path, found ${records.length}"
    )
    val r = records.head
    require(
      r.getInt(0) == FormatVersion && r.getString(1) == Kind,
      s"$path does not hold a $Kind result of format version $FormatVersion"
    )
    def bits(index: Int): Array[Double] =
      r.getSeq[Long](index).iterator.map(java.lang.Double.longBitsToDouble).toArray
    val summary = BlockMomentsSummary(
      software = SoftwareInfo.fromFields(r.getSeq[String](3).zip(r.getSeq[String](4)).toSeq),
      spec = InputSpec(r.getString(5), r.getSeq[String](6).toSeq, Option(r.getString(7))),
      options = BlockOptions(r.getLong(8), r.getInt(9), r.getLong(10), r.getString(11)),
      rowCount = r.getLong(12),
      groupCount = r.getInt(13),
      fingerprint = r.getLong(14),
      layout =
        LayoutSummary(r.getInt(15), r.getInt(16), r.getInt(17), r.getLong(18), r.getLong(19)),
      columnSums = bits(20),
      crossProducts = bits(21)
    )
    BlockMomentsResult(summary, spark.read.parquet(s"$path/groups"))
  }
}
