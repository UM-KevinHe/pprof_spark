package pprof.spark.engine.layout

import java.nio.charset.StandardCharsets

import scala.collection.mutable

/** The key of one group. Integral keys compare as numbers and string keys by their unsigned UTF-8
  * bytes, which is the order of Spark's default collation (§7.2).
  */
sealed abstract class GroupKey extends Product with Serializable

object GroupKey {
  final case class Integral(value: Long) extends GroupKey
  final case class Text(value: String) extends GroupKey

  implicit val ordering: Ordering[GroupKey] = new Ordering[GroupKey] {
    def compare(a: GroupKey, b: GroupKey): Int = (a, b) match {
      case (Integral(x), Integral(y)) => java.lang.Long.compare(x, y)
      case (Text(x), Text(y))         =>
        java.util.Arrays.compareUnsigned(
          x.getBytes(StandardCharsets.UTF_8),
          y.getBytes(StandardCharsets.UTF_8)
        )
      case (_: Integral, _: Text) => -1
      case (_: Text, _: Integral) => 1
    }
  }

  /** The key of a value read from a validated group column: a long or a string. */
  def of(value: Any): GroupKey = value match {
    case v: Long   => Integral(v)
    case v: String => Text(v)
    case other     =>
      throw new IllegalArgumentException(
        s"unsupported group key of class ${other.getClass.getName}"
      )
  }

  /** The Spark value of a key, for tables built on the driver. */
  def value(key: GroupKey): Any = key match {
    case Integral(v) => v
    case Text(v)     => v
  }
}

/** Where the rows of one group go. `groupIndex` is the group's position in key order. */
final case class GroupPlacement(key: GroupKey, groupIndex: Int, rows: Long, blockId: Int)

/** A deterministic assignment of whole groups to logical blocks (§6.5, §6.6, DIST-5). It is computed
  * on the driver from group sizes alone, so it never depends on cluster size or input partitioning.
  *
  * @param placements one entry per group, in key order
  * @param oversizedGroups groups with more rows than `targetRowsPerBlock`; each has its own block
  */
final case class LayoutPlan(
    targetRowsPerBlock: Int,
    placements: Vector[GroupPlacement],
    blockCount: Int,
    oversizedGroups: Int
) {
  def groupCount: Int = placements.size

  def rowCount: Long = placements.iterator.map(_.rows).sum

  /** Rows of each block, indexed by block identifier. */
  def blockRows: Vector[Long] = {
    val rows = new Array[Long](blockCount)
    placements.foreach(g => rows(g.blockId) += g.rows)
    rows.toVector
  }
}

object LayoutPlan {

  /** Plans blocks for groups of the given sizes. Groups with more rows than `targetRowsPerBlock`
    * get blocks of their own, first and in key order; splitting them is TimeRange work (§6.6). The
    * other groups fill `ceil(rows / targetRowsPerBlock)` shared blocks: largest group first (ties by
    * key), each to the least-loaded block (ties by lowest block identifier). No block is empty.
    */
  def create(groupSizes: Seq[(GroupKey, Long)], targetRowsPerBlock: Int): LayoutPlan = {
    require(targetRowsPerBlock > 0, "targetRowsPerBlock must be positive")
    require(groupSizes.nonEmpty, "there must be at least one group")
    require(groupSizes.forall(_._2 > 0), "every group must have at least one row")
    val sorted = groupSizes.sortBy(_._1)
    require(
      sorted.iterator.sliding(2).forall(pair => pair.size < 2 || pair(0)._1 != pair(1)._1),
      "group keys must be distinct"
    )
    val indexed = sorted.zipWithIndex.map { case ((key, rows), index) => (key, index, rows) }
    val (oversized, regular) = indexed.partition(_._3 > targetRowsPerBlock)
    val dedicated = oversized.zipWithIndex.map { case ((key, index, rows), block) =>
      GroupPlacement(key, index, rows, block)
    }
    val regularRows = regular.iterator.map(_._3).sum
    val shared = ((regularRows + targetRowsPerBlock - 1) / targetRowsPerBlock).toInt
    val leastLoaded = mutable.PriorityQueue.empty[(Long, Int)](Ordering[(Long, Int)].reverse)
    (0 until shared).foreach(block => leastLoaded.enqueue((0L, block)))
    val largestFirst = regular.sortBy { case (key, _, rows) => (-rows, key) }
    val assigned = largestFirst.map { case (key, index, rows) =>
      val (load, block) = leastLoaded.dequeue()
      leastLoaded.enqueue((load + rows, block))
      GroupPlacement(key, index, rows, dedicated.size + block)
    }
    LayoutPlan(
      targetRowsPerBlock,
      (dedicated ++ assigned).sortBy(_.groupIndex).toVector,
      dedicated.size + shared,
      dedicated.size
    )
  }
}
