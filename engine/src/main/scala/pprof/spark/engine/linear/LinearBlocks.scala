package pprof.spark.engine.linear

import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.storage.StorageLevel
import pprof.spark.engine.backend.PlanFrames
import pprof.spark.engine.data.Validation
import pprof.spark.engine.layout.LayoutPlan
import pprof.spark.numerics.kernels.{LinearFE => Kernel}

/** One validated record placed in its block. */
final case class LinearRow(blockId: Int, groupIndex: Int, rowId: Long, y: Double, x: Array[Double])

/** A ProviderLocal block (§7): providers in key order, each provider's rows in canonical order; `values` holds each
  * row's p features and then its outcome.
  */
final case class LinearBlock(
    blockId: Int,
    p: Int,
    groupIndex: Array[Int],
    groupStart: Array[Int],
    rowId: Array[Long],
    values: Array[Double]
) {
  def rows: Kernel.Rows = Kernel.Rows(groupStart, values, p)
}

/** Pass 1's partial of one block. */
final case class LinearMomentPartial(
    blockId: Int,
    rows: Long,
    xx: Array[Double],
    xy: Array[Double],
    ySum: Double,
    fingerprint: Long
)

/** Pass 2's partial of one block. */
final case class LinearResidualPartial(blockId: Int, rss: Double, tss: Double)

/** One provider's effect and variance factor qⱼ, from pass 2. */
final case class LinearProviderRow(groupIndex: Int, records: Long, gamma: Double, q: Double)

object LinearBlockBuilder {

  /** Provider, then row identifier, then the values: so the order never depends on the input's. */
  val canonicalOrder: Ordering[LinearRow] = new Ordering[LinearRow] {
    def compare(a: LinearRow, b: LinearRow): Int = {
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
          if (result != 0) result else java.lang.Double.compare(a.y, b.y)
        }
      }
    }
  }

  def build(blockId: Int, rows: Iterator[LinearRow], p: Int): LinearBlock = {
    val sorted = rows
      .map(r => r.copy(y = r.y + 0.0, x = r.x.map(v => if (v == 0.0) 0.0 else v)))
      .toArray
      .sorted(canonicalOrder)
    val count = sorted.length
    val q = p + 1
    val rowIds = new Array[Long](count)
    val values = new Array[Double](count * q)
    val groups = Array.newBuilder[Int]
    val starts = Array.newBuilder[Int]
    var r = 0
    while (r < count) {
      val row = sorted(r)
      require(row.x.length == p, s"expected $p features, got ${row.x.length}")
      if (r == 0 || row.groupIndex != sorted(r - 1).groupIndex) {
        groups += row.groupIndex
        starts += r
      }
      rowIds(r) = row.rowId
      System.arraycopy(row.x, 0, values, r * q, p)
      values(r * q + p) = row.y
      r += 1
    }
    LinearBlock(blockId, p, groups.result(), starts.result(), rowIds, values)
  }
}

/** The persisted blocks of one fit. */
final class LinearWorkingSet private (val blocks: Dataset[LinearBlock], val plan: LayoutPlan) {
  def release(): Unit = {
    blocks.unpersist(blocking = false)
    ()
  }
}

object LinearWorkingSet {

  def build(input: LinearInput, plan: LayoutPlan, storageLevel: StorageLevel): LinearWorkingSet = {
    val p = input.spec.features.size
    val placements = PlanFrames.placements(input.frame.sparkSession, plan, input.providerKeyIsText)
    val placed = input.frame
      .join(broadcast(placements), Seq(Validation.GroupColumn))
      .select(
        col(PlanFrames.BlockIdColumn).as("blockId"),
        col(PlanFrames.GroupIndexColumn).as("groupIndex"),
        col(Validation.RowIdColumn).as("rowId"),
        col(LinearValidation.OutcomeColumn).as("y"),
        col(Validation.FeaturesColumn).as("x")
      )
      .as[LinearRow](Encoders.product[LinearRow])
    val blocks = placed
      .groupByKey((row: LinearRow) => row.blockId)(Encoders.scalaInt)
      .mapGroups((blockId: Int, rows: Iterator[LinearRow]) =>
        LinearBlockBuilder.build(blockId, rows, p)
      )(
        Encoders.product[LinearBlock]
      )
      .persist(storageLevel)
    try {
      val built = blocks.count()
      if (built != plan.blockCount)
        throw new IllegalStateException(s"built $built blocks; the plan has ${plan.blockCount}")
      new LinearWorkingSet(blocks, plan)
    } catch {
      case e: Throwable =>
        blocks.unpersist(blocking = false)
        throw e
    }
  }
}
