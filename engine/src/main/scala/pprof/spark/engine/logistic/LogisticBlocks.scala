package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.backend.PlanFrames
import pprof.spark.engine.backend.RowRecords
import pprof.spark.engine.backend.Storage._
import pprof.spark.engine.data.Validation
import pprof.spark.engine.layout.{LayoutPlan, PartitionKeys, ReductionGroups}
import pprof.spark.numerics.kernels.{LogisticFE => Kernel}
import pprof.spark.engine.backend.Operations._

/** One input row placed in its block (§6.6). */
final case class LogisticRow(
    blockId: Int,
    groupIndex: Int,
    rowId: Long,
    y: Double,
    n: Double,
    x: Array[Double],
    cluster: String
)

/** One stored record of a block (docs/spec/performance/step2-chunks.md §3): the block's rows `firstRow` on, in
  * canonical order (Phase 2a specification §11, DIST-6), covariates row-major. It lists the providers whose first
  * row it holds, by group index and block row, and ranks its rows' cluster keys in the block (empty without a
  * cluster column). Record `record` of the block's `records`; every record but the last holds as many rows.
  */
final case class LogisticChunk(
    blockId: Int,
    record: Int,
    records: Int,
    firstRow: Int,
    p: Int,
    groupIndex: Array[Int],
    groupStart: Array[Int],
    rowId: Array[Long],
    y: Array[Double],
    n: Array[Double],
    x: Array[Double],
    cluster: Array[Int]
)

/** Whole providers in canonical order, read from a block's records in order: provider `g` holds rows `groupStart(g)`
  * until the next start. Kernels read its rows through `rows` (step2-chunks.md §3).
  */
final class LogisticBlock private[logistic] (parts: Array[LogisticChunk]) {
  val blockId: Int = parts(0).blockId
  val p: Int = parts(0).p
  val groupIndex: Array[Int] = Array.concat(parts.map(_.groupIndex).toSeq: _*)
  val groupStart: Array[Int] = Array.concat(parts.map(_.groupStart).toSeq: _*)
  private val recordRows = math.max(1, parts(0).rowId.length)
  val rows: Kernel.ChunkedRows = new Kernel.ChunkedRows(
    p,
    recordRows,
    groupStart,
    parts.map(_.y),
    parts.map(_.n),
    parts.map(_.x),
    parts.map(_.cluster)
  )
  def rowCount: Int = rows.rows
  def rowId(r: Int): Long = parts(r / recordRows).rowId(r % recordRows)
  def y(r: Int): Double = rows.yAt(r)
  def n(r: Int): Double = rows.nAt(r)
}

/** Per block: counts, Σ n x, the fingerprint, and records, events and trials of each provider. */
final case class LogisticSummary(
    blockId: Int,
    rows: Long,
    events: Double,
    trials: Double,
    weightedX: Array[Double],
    featureMean: Array[Double],
    featureComoments: Array[Double],
    fingerprint: Long,
    records: Array[Long],
    providerEvents: Array[Double],
    providerTrials: Array[Double],
    maxOutcome: Double,
    zeroOutcomes: Long
)

final case class LogisticSchurPartial(
    blockId: Int,
    packed: Array[Double],
    rhs: Array[Double],
    scoreTerm: Double,
    loglik: Double
)

final case class LogisticTrialPartial(
    blockId: Int,
    deltaGamma: Array[Double],
    loglik: Array[Double]
)

final case class LogisticVariancePartial(blockId: Int, packed: Array[Double], loglik: Double)

final case class LogisticMeatPartial(blockId: Int, packed: Array[Double], clusters: Long)

final case class LogisticRobustPartial(
    blockId: Int,
    caseMix: Array[Double],
    fixedBeta: Array[Double]
)

final case class LogisticScorePartial(blockId: Int, packed: Array[Double], score: Array[Double])

final case class LogisticLoglikPartial(blockId: Int, loglik: Double)

/** One row's fitted probability and outcome, for the AUC's sort. */
final case class LogisticScore(pi: Double, positive: Boolean)

/** One sorted partition's positives, negatives and twice its Mann–Whitney numerator. */
final case class LogisticAucPartial(positives: Long, negatives: Long, twiceNumerator: Long)

final case class LogisticProviderPartial(
    blockId: Int,
    varGamma: Array[Double],
    varCaseMix: Array[Double]
)

object LogisticBlockBuilder {

  /** Provider, row identifier, then covariate values, events and trials. */
  val canonicalOrder: Ordering[LogisticRow] = new Ordering[LogisticRow] {
    def compare(a: LogisticRow, b: LogisticRow): Int = {
      val byGroup = Integer.compare(a.groupIndex, b.groupIndex)
      if (byGroup != 0) byGroup
      else {
        val byRowId = java.lang.Long.compare(a.rowId, b.rowId)
        if (byRowId != 0) byRowId
        else {
          var i = 0
          var result = 0
          while (result == 0 && i < math.min(a.x.length, b.x.length)) {
            result = java.lang.Double.compare(a.x(i), b.x(i))
            i += 1
          }
          if (result != 0) result
          else {
            val byEvents = java.lang.Double.compare(a.y, b.y)
            if (byEvents != 0) byEvents else java.lang.Double.compare(a.n, b.n)
          }
        }
      }
    }
  }

  /** The records of rows sorted by block identifier: each block built in turn (step1-reduction.md §3) and stored in
    * records of `recordRows` rows (step2-chunks.md §3).
    */
  def buildAll(rows: Iterator[LogisticRow], p: Int, recordRows: Int): Iterator[LogisticChunk] = {
    val source = rows.buffered
    new Iterator[Iterator[LogisticChunk]] {
      private var last = -1

      def hasNext: Boolean = source.hasNext

      def next(): Iterator[LogisticChunk] = {
        val id = source.head.blockId
        if (id <= last)
          throw new IllegalStateException(s"block $id after block $last: rows out of block order")
        last = id
        val block = scala.collection.mutable.ArrayBuffer.empty[LogisticRow]
        while (source.hasNext && source.head.blockId == id) block += source.next()
        build(id, block.iterator, p, recordRows)
      }
    }.flatten
  }

  /** Sorts a block's rows canonically after normalizing negative zeros (ADR-0004), then stores them in records of
    * `recordRows` rows, the last possibly shorter.
    */
  def build(
      blockId: Int,
      rows: Iterator[LogisticRow],
      p: Int,
      recordRows: Int
  ): Iterator[LogisticChunk] = {
    require(recordRows > 0, s"a record holds at least one row, got $recordRows")
    val sorted = rows
      .map(r => r.copy(y = r.y + 0.0, n = r.n + 0.0, x = r.x.map(v => if (v == 0.0) 0.0 else v)))
      .toArray
      .sorted(canonicalOrder)
    val count = sorted.length
    val keys = sorted.iterator.map(_.cluster).toArray.distinct.sorted
    val ranks =
      if (keys.forall(_.isEmpty)) Array.emptyIntArray
      else {
        val rank = keys.zipWithIndex.toMap
        sorted.map(row => rank(row.cluster))
      }
    val records = (count + recordRows - 1) / recordRows
    Iterator.tabulate(records) { i =>
      val from = i * recordRows
      val until = math.min(count, from + recordRows)
      val rowIds = new Array[Long](until - from)
      val y = new Array[Double](until - from)
      val n = new Array[Double](until - from)
      val x = new Array[Double]((until - from) * p)
      val groups = Array.newBuilder[Int]
      val starts = Array.newBuilder[Int]
      var r = from
      while (r < until) {
        val row = sorted(r)
        require(row.x.length == p, s"expected $p covariates, got ${row.x.length}")
        if (r == 0 || row.groupIndex != sorted(r - 1).groupIndex) {
          groups += row.groupIndex
          starts += r
        }
        rowIds(r - from) = row.rowId
        y(r - from) = row.y
        n(r - from) = row.n
        System.arraycopy(row.x, 0, x, (r - from) * p, p)
        r += 1
      }
      LogisticChunk(
        blockId,
        i,
        records,
        from,
        p,
        groups.result(),
        starts.result(),
        rowIds,
        y,
        n,
        x,
        if (ranks.isEmpty) Array.emptyIntArray else java.util.Arrays.copyOfRange(ranks, from, until)
      )
    }
  }
}

/** Reading the working set's records as blocks (docs/spec/performance/step2-chunks.md §3). */
object LogisticBlocks {

  /** The blocks of a partition's records, each from its consecutive records; fails if a block's records are out of
    * order or incomplete. The group fold checks the blocks' order (step1-reduction.md §4).
    */
  def assemble(records: Iterator[LogisticChunk]): Iterator[LogisticBlock] =
    new Iterator[LogisticBlock] {
      def hasNext: Boolean = records.hasNext

      def next(): LogisticBlock = {
        val first = records.next()
        if (first.record != 0)
          throw new IllegalStateException(
            s"block ${first.blockId} begins with record ${first.record} of ${first.records}"
          )
        val parts = new Array[LogisticChunk](first.records)
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
        new LogisticBlock(parts)
      }
    }

  /** Each block's providers' effects from `gamma`, in key order, for a task to take instead of a join (OI-105). */
  def effects(providersOfBlock: Array[Array[Int]], gamma: Array[Double]): Array[Array[Double]] =
    providersOfBlock.map(_.map(gamma(_)))
}

/** The persisted logistic working set (§6.7): each block's records (docs/spec/performance/step2-chunks.md §3). */
final class LogisticWorkingSet private (
    val records: Dataset[LogisticChunk],
    val plan: LayoutPlan,
    val groups: ReductionGroups
) {
  def release(): Unit = {
    records.unpersist(blocking = false)
    ()
  }
}

object LogisticWorkingSet {

  private val PartitionKeyColumn = "__pprof_partition_key"

  /** Joins the plan to the rows (dropping screened providers), builds each plan block's records
    * with one shuffle, and persists them at `blockOptions`' storage level.
    */
  def build(
      input: LogisticInput,
      plan: LayoutPlan,
      groups: ReductionGroups,
      blockOptions: BlockOptions
  ): LogisticWorkingSet = {
    val p = input.spec.features.size
    val recordRows = RowRecords.rowsPerRecord(p)
    val spark = input.frame.sparkSession
    val placements = PlanFrames.placements(spark, plan, input.providerKeyIsText)
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
    val placed = input.frame
      .join(broadcast(placements), Seq(Validation.GroupColumn))
      .join(broadcast(blockKeys), Seq(PlanFrames.BlockIdColumn))
      .select(
        col(PartitionKeyColumn),
        col(PlanFrames.BlockIdColumn).as("blockId"),
        col(PlanFrames.GroupIndexColumn).as("groupIndex"),
        col(Validation.RowIdColumn).as("rowId"),
        col(LogisticValidation.OutcomeColumn).as("y"),
        col(LogisticValidation.TrialsColumn).as("n"),
        col(Validation.FeaturesColumn).as("x"),
        col(LogisticValidation.ClusterColumn).as("cluster")
      )
    val records = placed
      .repartition(groups.groups, col(PartitionKeyColumn))
      .sortWithinPartitions(col("blockId"))
      .drop(PartitionKeyColumn)
      .as[LogisticRow](Encoders.product[LogisticRow])
      .mapPartitions((rows: Iterator[LogisticRow]) =>
        LogisticBlockBuilder.buildAll(rows, p, recordRows)
      )(Encoders.product[LogisticChunk])
      .persistAt(blockOptions)
    try {
      val built =
        records.filter((record: LogisticChunk) => record.record == 0).countAs("logistic.build")
      if (built != plan.blockCount)
        throw new IllegalStateException(s"built $built blocks; the plan has ${plan.blockCount}")
      new LogisticWorkingSet(records, plan, groups)
    } catch {
      case e: Throwable =>
        records.unpersist(blocking = false)
        throw e
    }
  }
}
