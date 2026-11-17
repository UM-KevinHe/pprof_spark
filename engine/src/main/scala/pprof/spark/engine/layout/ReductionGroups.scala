package pprof.spark.engine.layout

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, hash, lit, min, pmod}

import pprof.spark.engine.backend.{BlockOptions, DriverGuards}
import pprof.spark.engine.backend.Operations._

/** A working set's blocks in R groups of G consecutive block identifiers (docs/spec/performance/step1-reduction.md
  * §2). A pass folds each group's block partials in block order on the executors and sends one partial per group to
  * the driver, which folds them in group order; so the driver receives R partials whatever the number of blocks.
  */
final case class ReductionGroups(blocks: Int, blocksPerGroup: Int) {
  require(blocks >= 1, s"a working set has at least one block, got $blocks")
  require(blocksPerGroup >= 1, s"a group holds at least one block, got $blocksPerGroup")

  /** R, the number of groups. */
  val groups: Int = ((blocks.toLong + blocksPerGroup - 1) / blocksPerGroup).toInt

  def groupOf(block: Int): Int = {
    require(block >= 0 && block < blocks, s"block $block outside 0 until $blocks")
    block / blocksPerGroup
  }

  /** The blocks of `group`, in order. */
  def blocksOf(group: Int): Range = {
    require(group >= 0 && group < groups, s"group $group outside 0 until $groups")
    (group * blocksPerGroup) until math.min(blocks, (group + 1) * blocksPerGroup)
  }
}

object ReductionGroups {

  /** Groups for `blocks` blocks whose partials take `partialBytes` each: one block per group while every partial fits
    * the driver budget together (G = 1, today's single-level fold, bit for bit), otherwise the fewest blocks per
    * group with which the groups' partials fit.
    */
  def plan(blocks: Int, partialBytes: Long, options: BlockOptions): ReductionGroups = {
    require(partialBytes >= 1L, s"a partial takes at least one byte, got $partialBytes")
    DriverGuards.requireWithinBudget("One block's partial", partialBytes, options)
    val maxGroups = options.driverBudgetBytes / partialBytes
    val perGroup =
      if (blocks.toLong <= maxGroups) 1 else ((blocks.toLong + maxGroups - 1) / maxGroups).toInt
    ReductionGroups(blocks, perGroup)
  }
}

/** Keys that place one group in each partition (step1-reduction.md §3). */
object PartitionKeys {

  /** For each of `partitions` partitions g, a long that Spark's hash partitioning into `partitions` partitions sends
    * to partition g. The keys come from Spark's own `hash`, the Murmur3 hash that hash partitioning applies, so they
    * follow the running Spark. Under any other partitioning a group would still never be split, since its key is a
    * function of the group; only the balance of groups over partitions would suffer.
    */
  def forPartitions(spark: SparkSession, partitions: Int): Array[Long] = {
    require(partitions >= 1, s"at least one partition is required, got $partitions")
    if (partitions == 1) Array(0L)
    else {
      var span = 32L * partitions
      var keys: Option[Array[Long]] = None
      var attempts = 0
      while (keys.isEmpty) {
        val found = spark
          .range(0L, span)
          .select(pmod(hash(col("id")), lit(partitions)).as("partition"), col("id"))
          .groupBy("partition")
          .agg(min("id").as("key"))
          .collectAs("layout.partition-keys")
        if (found.length == partitions) {
          val array = new Array[Long](partitions)
          found.foreach(r => array(r.getInt(0)) = r.getLong(1))
          keys = Some(array)
        } else {
          attempts += 1
          if (attempts >= 4)
            throw new IllegalStateException(
              s"no key found for some of $partitions partitions in 0 until $span"
            )
          span *= 8L
        }
      }
      keys.get
    }
  }
}

/** Folds a partition's blocks per reduction group, in block order (step1-reduction.md §4): blocks arrive sorted by
  * block identifier, and each group's accumulator receives its blocks in that order. A block or a group out of
  * order fails the pass rather than change the order of summation.
  */
object GroupFold {

  def apply[T, A](blocks: Iterator[T], blockId: T => Int, groups: ReductionGroups)(empty: => A)(
      add: (A, T) => Unit
  ): Iterator[(Int, A)] = {
    val source = blocks.buffered
    new Iterator[(Int, A)] {
      private var lastBlock = -1
      private var lastGroup = -1

      def hasNext: Boolean = source.hasNext

      def next(): (Int, A) = {
        val group = groups.groupOf(blockId(source.head))
        if (group <= lastGroup)
          throw new IllegalStateException(
            s"group $group after group $lastGroup: blocks out of order"
          )
        lastGroup = group
        val accumulator = empty
        while (source.hasNext && groups.groupOf(blockId(source.head)) == group) {
          val block = source.next()
          val id = blockId(block)
          if (id <= lastBlock)
            throw new IllegalStateException(
              s"block $id after block $lastBlock: blocks out of order"
            )
          lastBlock = id
          add(accumulator, block)
        }
        (group, accumulator)
      }
    }
  }
}
