package pprof.spark.engine.cox

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.apache.spark.sql.functions.{broadcast, col, pmod, xxhash64}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.backend.PlanFrames
import pprof.spark.engine.backend.RowRecords
import pprof.spark.engine.backend.Storage._
import pprof.spark.engine.data.Validation
import pprof.spark.engine.layout.{LayoutPlan, PartitionKeys, ReductionGroups}
import pprof.spark.engine.backend.Operations._
import pprof.spark.numerics.kernels.ChunkedCoxRows

/** One input row placed in its block (§6.6). */
final case class CoxRow(
    blockId: Int,
    groupIndex: Int,
    rowId: Long,
    time: Double,
    event: Boolean,
    weight: Double,
    offset: Double,
    x: Array[Double],
    entry: Double = 0.0,
    cluster: String = null
)

/** One stored record of a block (docs/spec/performance/step2-chunks.md §3): the block's rows `firstRow` on, in
  * canonical order (Cox specification §6, DIST-6), covariates row-major, with the same positions of each stratum's
  * entry order. It lists the strata whose first row it holds, by group index and block row. Record `record` of the
  * block's `records`; every record but the last holds as many rows.
  */
final case class CoxChunk(
    blockId: Int,
    record: Int,
    records: Int,
    firstRow: Int,
    p: Int,
    groupIndex: Array[Int],
    groupStart: Array[Int],
    rowId: Array[Long],
    time: Array[Double],
    event: Array[Boolean],
    weight: Array[Double],
    offset: Array[Double],
    x: Array[Double],
    entry: Array[Double],
    entryOrder: Array[Int],
    cluster: Array[String]
)

/** Whole strata in canonical order, read from a block's records in order: stratum `g` holds rows `groupStart(g)` until
  * `groupEnd(g)`. Kernels read its rows through `rows` (step2-chunks.md §3).
  */
final class CoxBlock private[cox] (parts: Array[CoxChunk]) {
  val blockId: Int = parts(0).blockId
  val p: Int = parts(0).p
  val groupIndex: Array[Int] = Array.concat(parts.map(_.groupIndex).toSeq: _*)
  val groupStart: Array[Int] = Array.concat(parts.map(_.groupStart).toSeq: _*)
  private val recordRows = math.max(1, parts(0).rowId.length)
  val rows: ChunkedCoxRows = new ChunkedCoxRows(
    p,
    recordRows,
    parts.map(_.time),
    parts.map(_.entry),
    parts.map(_.entryOrder),
    parts.map(_.event),
    parts.map(_.weight),
    parts.map(_.offset),
    parts.map(_.x)
  )
  def rowCount: Int = rows.rows
  def groupCount: Int = groupIndex.length
  def groupEnd(g: Int): Int = if (g + 1 < groupCount) groupStart(g + 1) else rowCount
  def rowId(r: Int): Long = parts(r / recordRows).rowId(r % recordRows)
  def cluster(r: Int): String = parts(r / recordRows).cluster(r % recordRows)
  def time(r: Int): Double = rows.timeAt(r)
  def event(r: Int): Boolean = rows.eventAt(r)
  def weight(r: Int): Double = rows.weightAt(r)
  def offset(r: Int): Double = rows.offsetAt(r)
  def entry(r: Int): Double = rows.entryAt(r)
}

object CoxBlockBuilder {

  /** Stratum, exit time descending, events first, row identifier, then covariate values, weight
    * and offset.
    */
  val canonicalOrder: Ordering[CoxRow] = new Ordering[CoxRow] {
    def compare(a: CoxRow, b: CoxRow): Int = {
      val byGroup = Integer.compare(a.groupIndex, b.groupIndex)
      if (byGroup != 0) byGroup
      else {
        val byTime = java.lang.Double.compare(b.time, a.time)
        if (byTime != 0) byTime
        else {
          val byEvent = java.lang.Boolean.compare(b.event, a.event)
          if (byEvent != 0) byEvent
          else {
            val byRowId = java.lang.Long.compare(a.rowId, b.rowId)
            if (byRowId != 0) byRowId
            else {
              val byValues = compareValues(a.x, b.x)
              if (byValues != 0) byValues
              else {
                val byWeight = java.lang.Double.compare(a.weight, b.weight)
                if (byWeight != 0) byWeight
                else {
                  val byOffset = java.lang.Double.compare(a.offset, b.offset)
                  if (byOffset != 0) byOffset
                  else {
                    val byEntry = java.lang.Double.compare(a.entry, b.entry)
                    if (byEntry != 0) byEntry
                    else if (a.cluster == b.cluster) 0
                    else if (a.cluster == null) -1
                    else if (b.cluster == null) 1
                    else a.cluster.compareTo(b.cluster)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  /** Sorts a block's rows canonically, normalizing negative zeros first so that the order and every later sum depend
    * on values alone; orders each stratum's rows by entry time; and stores both in records of `recordRows` rows, the
    * last possibly shorter (step2-chunks.md §3).
    */
  def build(blockId: Int, rows: Iterator[CoxRow], p: Int, recordRows: Int): Iterator[CoxChunk] = {
    require(recordRows > 0, s"a record holds at least one row, got $recordRows")
    val sorted = rows.map(normalize).toArray.sorted(canonicalOrder)
    val n = sorted.length
    val groups = Array.newBuilder[Int]
    val starts = Array.newBuilder[Int]
    var r = 0
    while (r < n) {
      val row = sorted(r)
      require(row.x.length == p, s"expected $p covariates, got ${row.x.length}")
      if (r == 0 || row.groupIndex != sorted(r - 1).groupIndex) {
        groups += row.groupIndex
        starts += r
      }
      r += 1
    }
    val groupIndex = groups.result()
    val groupStarts = starts.result()
    val entryOrder = new Array[Int](n)
    var g = 0
    while (g < groupStarts.length) {
      val from = groupStarts(g)
      val until = if (g + 1 < groupStarts.length) groupStarts(g + 1) else n
      val order = (from until until).sortWith { (a, b) =>
        val byEntry = java.lang.Double.compare(sorted(b).entry, sorted(a).entry)
        byEntry < 0 || (byEntry == 0 && a < b)
      }
      order.copyToArray(entryOrder, from)
      g += 1
    }
    val records = (n + recordRows - 1) / recordRows
    Iterator.tabulate(records) { i =>
      val from = i * recordRows
      val until = math.min(n, from + recordRows)
      val count = until - from
      val rowIds = new Array[Long](count)
      val times = new Array[Double](count)
      val events = new Array[Boolean](count)
      val weights = new Array[Double](count)
      val offsets = new Array[Double](count)
      val entries = new Array[Double](count)
      val clusters = new Array[String](count)
      val x = new Array[Double](count * p)
      var r = from
      while (r < until) {
        val row = sorted(r)
        rowIds(r - from) = row.rowId
        times(r - from) = row.time
        events(r - from) = row.event
        weights(r - from) = row.weight
        offsets(r - from) = row.offset
        entries(r - from) = row.entry
        clusters(r - from) = row.cluster
        System.arraycopy(row.x, 0, x, (r - from) * p, p)
        r += 1
      }
      val first = groupStarts.indexWhere(_ >= from)
      val last = if (first < 0) first else groupStarts.lastIndexWhere(_ < until)
      val (gi, gs) =
        if (first < 0 || last < first) (Array.emptyIntArray, Array.emptyIntArray)
        else (groupIndex.slice(first, last + 1), groupStarts.slice(first, last + 1))
      CoxChunk(
        blockId,
        i,
        records,
        from,
        p,
        gi,
        gs,
        rowIds,
        times,
        events,
        weights,
        offsets,
        x,
        entries,
        java.util.Arrays.copyOfRange(entryOrder, from, until),
        clusters
      )
    }
  }

  /** The records of rows sorted by block identifier: each block built in turn (step1-reduction.md §3) and stored in
    * records of `recordRows` rows (step2-chunks.md §3).
    */
  def buildAll(rows: Iterator[CoxRow], p: Int, recordRows: Int): Iterator[CoxChunk] = {
    val source = rows.buffered
    new Iterator[Iterator[CoxChunk]] {
      private var last = -1

      def hasNext: Boolean = source.hasNext

      def next(): Iterator[CoxChunk] = {
        val id = source.head.blockId
        if (id <= last)
          throw new IllegalStateException(s"block $id after block $last: rows out of block order")
        last = id
        val block = scala.collection.mutable.ArrayBuffer.empty[CoxRow]
        while (source.hasNext && source.head.blockId == id) block += source.next()
        build(id, block.iterator, p, recordRows)
      }
    }.flatten
  }

  private def normalize(row: CoxRow): CoxRow =
    row.copy(
      time = row.time + 0.0,
      weight = row.weight + 0.0,
      offset = row.offset + 0.0,
      entry = row.entry + 0.0,
      x = row.x.map(v => if (v == 0.0) 0.0 else v)
    )

  private def compareValues(a: Array[Double], b: Array[Double]): Int = {
    var i = 0
    var result = 0
    while (result == 0 && i < math.min(a.length, b.length)) {
      result = java.lang.Double.compare(a(i), b(i))
      i += 1
    }
    if (result != 0) result else Integer.compare(a.length, b.length)
  }
}

/** Reading the working set's records as blocks (docs/spec/performance/step2-chunks.md §3). */
object CoxBlocks {

  /** The blocks of a partition's records, each from its consecutive records; fails if a block's records are out of
    * order or incomplete. The group fold checks the blocks' order (step1-reduction.md §4).
    */
  def assemble(records: Iterator[CoxChunk]): Iterator[CoxBlock] =
    new Iterator[CoxBlock] {
      def hasNext: Boolean = records.hasNext

      def next(): CoxBlock = {
        val first = records.next()
        if (first.record != 0)
          throw new IllegalStateException(
            s"block ${first.blockId} begins with record ${first.record} of ${first.records}"
          )
        val parts = new Array[CoxChunk](first.records)
        parts(0) = first
        var i = 1
        while (i < parts.length) {
          if (!records.hasNext)
            throw new IllegalStateException(
              s"block ${first.blockId} ends after $i of ${parts.length} records"
            )
          val part = records.next()
          if (part.blockId != first.blockId || part.record != i)
            throw new IllegalStateException(
              s"record ${part.record} of block ${part.blockId} where record $i of block ${first.blockId} belongs"
            )
          parts(i) = part
          i += 1
        }
        new CoxBlock(parts)
      }
    }
}

/** The persisted Cox working set (§6.7): each block's records (docs/spec/performance/step2-chunks.md §3). */
final class CoxWorkingSet private (
    val records: Dataset[CoxChunk],
    val plan: LayoutPlan,
    val groups: ReductionGroups
) {
  def release(): Unit = {
    records.unpersist(blocking = false)
    ()
  }
}

object CoxWorkingSet {

  private val PartitionKeyColumn = "__pprof_partition_key"

  /** Joins the plan to the rows and, with one shuffle, places each reduction group in a partition of its own, its
    * rows sorted by block; builds the blocks one at a time and persists them at `blockOptions`' storage level
    * (step1-reduction.md §3).
    */
  def build(
      input: CoxInput,
      plan: LayoutPlan,
      groups: ReductionGroups,
      blockOptions: BlockOptions
  ): CoxWorkingSet = {
    val p = input.spec.features.size
    val recordRows = RowRecords.rowsPerRecord(p)
    val spark = input.frame.sparkSession
    val placements = PlanFrames.placementsWithParts(spark, plan, input.strataKeyIsText)
    val keys = PartitionKeys.forPartitions(spark, groups.groups)
    val blockKeys = spark.createDataFrame(
      (0 until plan.blockCount).map(b => Row(b, keys(groups.groupOf(b)))).asJava,
      StructType(
        Seq(
          StructField(PlanFrames.BlockIdColumn, IntegerType, nullable = false),
          StructField(PartitionKeyColumn, LongType, nullable = false)
        )
      )
    )
    val joined = input.frame.join(broadcast(placements), Seq(Validation.GroupColumn))
    // A split stratum's rows reach its blocks by a hash of their content, identifier included, so placement depends
    // on neither the input's order nor its partitioning (per-time-aggregates.md §3, DIST-6).
    val spread =
      if (plan.splitGroups.isEmpty) joined
      else
        joined.withColumn(
          PlanFrames.BlockIdColumn,
          (col(PlanFrames.BlockIdColumn) + pmod(
            xxhash64(
              col(Validation.RowIdColumn),
              col(CoxValidation.TimeColumn),
              col(CoxValidation.EntryColumn),
              col(CoxValidation.EventColumn),
              col(CoxValidation.WeightColumn),
              col(CoxValidation.OffsetColumn),
              col(Validation.FeaturesColumn)
            ),
            col(PlanFrames.PartsColumn).cast(LongType)
          )).cast(IntegerType)
        )
    val placed = spread
      .join(broadcast(blockKeys), Seq(PlanFrames.BlockIdColumn))
      .select(
        col(PartitionKeyColumn),
        col(PlanFrames.BlockIdColumn).as("blockId"),
        col(PlanFrames.GroupIndexColumn).as("groupIndex"),
        col(Validation.RowIdColumn).as("rowId"),
        col(CoxValidation.TimeColumn).as("time"),
        col(CoxValidation.EventColumn).as("event"),
        col(CoxValidation.WeightColumn).as("weight"),
        col(CoxValidation.OffsetColumn).as("offset"),
        col(CoxValidation.EntryColumn).as("entry"),
        col(CoxValidation.ClusterColumn).as("cluster"),
        col(Validation.FeaturesColumn).as("x")
      )
    val records = placed
      .repartition(groups.groups, col(PartitionKeyColumn))
      .sortWithinPartitions(col("blockId"))
      .drop(PartitionKeyColumn)
      .as[CoxRow](Encoders.product[CoxRow])
      .mapPartitions((rows: Iterator[CoxRow]) => CoxBlockBuilder.buildAll(rows, p, recordRows))(
        Encoders.product[CoxChunk]
      )
      .persistAt(blockOptions)
    try {
      val built = records.filter((record: CoxChunk) => record.record == 0).countAs("cox.build")
      if (built != plan.blockCount)
        throw new IllegalStateException(s"built $built blocks; the plan has ${plan.blockCount}")
      new CoxWorkingSet(records, plan, groups)
    } catch {
      case e: Throwable =>
        records.unpersist(blocking = false)
        throw e
    }
  }
}
