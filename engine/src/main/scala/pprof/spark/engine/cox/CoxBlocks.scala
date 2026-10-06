package pprof.spark.engine.cox

import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.storage.StorageLevel

import pprof.spark.engine.backend.PlanFrames
import pprof.spark.engine.data.Validation
import pprof.spark.engine.layout.LayoutPlan

/** One input row placed in its block (§6.6). */
final case class CoxRow(
    blockId: Int,
    groupIndex: Int,
    rowId: Long,
    time: Double,
    event: Boolean,
    x: Array[Double]
)

/** Whole strata in canonical order (Cox specification §6, DIST-6), covariates row-major. Stratum
  * `g` holds rows `groupStart(g)` until `groupEnd(g)`.
  */
final case class CoxBlock(
    blockId: Int,
    p: Int,
    groupIndex: Array[Int],
    groupStart: Array[Int],
    rowId: Array[Long],
    time: Array[Double],
    event: Array[Boolean],
    x: Array[Double]
) {
  def rowCount: Int = rowId.length
  def groupCount: Int = groupIndex.length
  def groupEnd(g: Int): Int = if (g + 1 < groupCount) groupStart(g + 1) else rowCount
}

object CoxBlockBuilder {

  /** Stratum, exit time descending, events first, row identifier, then covariate values. */
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
            if (byRowId != 0) byRowId else compareValues(a.x, b.x)
          }
        }
      }
    }
  }

  /** Sorts a block's rows canonically, normalizing negative zeros first so that the order and
    * every later sum depend on values alone.
    */
  def build(blockId: Int, rows: Iterator[CoxRow], p: Int): CoxBlock = {
    val sorted = rows.map(normalize).toArray.sorted(canonicalOrder)
    val n = sorted.length
    val rowIds = new Array[Long](n)
    val times = new Array[Double](n)
    val events = new Array[Boolean](n)
    val x = new Array[Double](n * p)
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
      rowIds(r) = row.rowId
      times(r) = row.time
      events(r) = row.event
      System.arraycopy(row.x, 0, x, r * p, p)
      r += 1
    }
    CoxBlock(blockId, p, groups.result(), starts.result(), rowIds, times, events, x)
  }

  private def normalize(row: CoxRow): CoxRow =
    row.copy(time = row.time + 0.0, x = row.x.map(v => if (v == 0.0) 0.0 else v))

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

/** The persisted Cox working set (§6.7). */
final class CoxWorkingSet private (val blocks: Dataset[CoxBlock], val plan: LayoutPlan) {
  def release(): Unit = {
    blocks.unpersist(blocking = false)
    ()
  }
}

object CoxWorkingSet {

  /** Joins the plan to the rows, builds one block per plan block with one shuffle, and persists
    * the blocks at `storageLevel`.
    */
  def build(input: CoxInput, plan: LayoutPlan, storageLevel: StorageLevel): CoxWorkingSet = {
    val p = input.spec.features.size
    val placements = PlanFrames.placements(input.frame.sparkSession, plan, input.strataKeyIsText)
    val placed = input.frame
      .join(broadcast(placements), Seq(Validation.GroupColumn))
      .select(
        col(PlanFrames.BlockIdColumn).as("blockId"),
        col(PlanFrames.GroupIndexColumn).as("groupIndex"),
        col(Validation.RowIdColumn).as("rowId"),
        col(CoxValidation.TimeColumn).as("time"),
        col(CoxValidation.EventColumn).as("event"),
        col(Validation.FeaturesColumn).as("x")
      )
      .as[CoxRow](Encoders.product[CoxRow])
    val blocks = placed
      .groupByKey((row: CoxRow) => row.blockId)(Encoders.scalaInt)
      .mapGroups((blockId: Int, rows: Iterator[CoxRow]) => CoxBlockBuilder.build(blockId, rows, p))(
        Encoders.product[CoxBlock]
      )
      .persist(storageLevel)
    try {
      val built = blocks.count()
      if (built != plan.blockCount)
        throw new IllegalStateException(s"built $built blocks; the plan has ${plan.blockCount}")
      new CoxWorkingSet(blocks, plan)
    } catch {
      case e: Throwable =>
        blocks.unpersist(blocking = false)
        throw e
    }
  }
}
