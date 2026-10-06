package pprof.spark.engine.backend

import pprof.spark.numerics.NeumaierVector

/** One input row on its way into a block; `x` holds the row's feature values. */
final case class PlacedRow(blockId: Int, groupIndex: Int, rowId: Long, x: Array[Double])

/** A logical block (§6.5): whole groups, rows in canonical order, values row-major in primitive
  * arrays. The rows of the `k`-th group are `groupStart(k)` to `groupStart(k + 1) - 1`.
  */
final case class BlockRecord(
    blockId: Int,
    p: Int,
    groupIndex: Array[Int],
    groupStart: Array[Int],
    rowId: Array[Long],
    values: Array[Double]
) {
  def rowCount: Int = rowId.length
  def groupCount: Int = groupIndex.length
}

object BlockBuilder {

  /** Canonical row order (DIST-6): group index, then row identifier, then the values compared one
    * by one with `java.lang.Double.compare`.
    */
  val canonicalOrder: Ordering[PlacedRow] = new Ordering[PlacedRow] {
    def compare(a: PlacedRow, b: PlacedRow): Int = {
      val byGroup = Integer.compare(a.groupIndex, b.groupIndex)
      if (byGroup != 0) byGroup
      else {
        val byRowId = java.lang.Long.compare(a.rowId, b.rowId)
        if (byRowId != 0) byRowId else compareValues(a.x, b.x)
      }
    }
  }

  /** Builds block `blockId` from its rows in any order. Negative zeros become positive zeros first,
    * because they compare equal and would otherwise leave their order to the input (DIST-6).
    */
  def build(blockId: Int, rows: Iterator[PlacedRow], p: Int): BlockRecord = {
    val sorted = rows.map(normalize(_, p)).toArray.sorted(canonicalOrder)
    require(sorted.nonEmpty, s"block $blockId has no rows")
    val n = sorted.length
    val values = new Array[Double](n * p)
    val rowIds = new Array[Long](n)
    val groups = Array.newBuilder[Int]
    val starts = Array.newBuilder[Int]
    var r = 0
    while (r < n) {
      val row = sorted(r)
      if (r == 0 || row.groupIndex != sorted(r - 1).groupIndex) {
        groups += row.groupIndex
        starts += r
      }
      rowIds(r) = row.rowId
      System.arraycopy(row.x, 0, values, r * p, p)
      r += 1
    }
    starts += n
    BlockRecord(blockId, p, groups.result(), starts.result(), rowIds, values)
  }

  private def normalize(row: PlacedRow, p: Int): PlacedRow = {
    require(row.x.length == p, s"expected $p values per row, found ${row.x.length}")
    row.copy(x = row.x.map(v => if (v == 0.0) 0.0 else v))
  }

  private def compareValues(a: Array[Double], b: Array[Double]): Int = {
    var i = 0
    var result = 0
    while (result == 0 && i < a.length && i < b.length) {
      result = java.lang.Double.compare(a(i), b(i))
      i += 1
    }
    if (result != 0) result else Integer.compare(a.length, b.length)
  }
}

object OrderedReduction {

  /** Combines per-block partial vectors in block-identifier order with element-wise Neumaier sums
    * (§6.8, ADR-0003), whatever order the partials arrive in. Runs on the driver.
    */
  def reduce(partials: Seq[(Int, Array[Double])], length: Int): Array[Double] = {
    val ordered = partials.sortBy(_._1)
    require(
      ordered.iterator.map(_._1).sliding(2).forall(ids => ids.size < 2 || ids(0) != ids(1)),
      "block identifiers must be distinct"
    )
    val sums = new NeumaierVector(length)
    ordered.foreach { case (_, partial) => sums.add(partial) }
    sums.values
  }
}
