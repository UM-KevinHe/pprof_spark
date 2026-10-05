package pprof.spark.engine.backend

import java.util.SplittableRandom

import org.apache.spark.storage.StorageLevel

import pprof.spark.testkit.SparkSuite

/** One input row of the toy workload; `rowId` is the stable row identifier of DIST-6. */
final case class ToyRow(blockId: Int, rowId: Long, x: Double)

/** A logical block: one block's rows as primitive arrays in canonical order (§6.5). */
final case class ToyBlock(blockId: Int, rowIds: Array[Long], xs: Array[Double])

/** The partial statistic that one kernel invocation returns for one block (§6.8). */
final case class ToyPartial(blockId: Int, value: Double)

/** Stand-ins for the working-set builder, a kernel and the reduction. They are top-level
  * functions without captured state (§6.4, §13), so closures calling them serialize cleanly.
  */
object ToyKernels {

  /** Builds a block in canonical order: rows sorted by their stable identifier (DIST-6). */
  def toBlock(blockId: Int, rows: Iterator[ToyRow]): ToyBlock = {
    val sorted = rows.toArray.sortBy(_.rowId)
    ToyBlock(blockId, sorted.map(_.rowId), sorted.map(_.x))
  }

  /** Deliberately order-sensitive: weights each value by its position, summed left to right. */
  def partial(block: ToyBlock): ToyPartial = {
    var acc = 0.0
    var i = 0
    while (i < block.xs.length) {
      acc += block.xs(i) * (i + 1)
      i += 1
    }
    ToyPartial(block.blockId, acc)
  }

  /** Combines partials in block-identifier order, never in task-completion order (§6.8). */
  def reduce(partials: Seq[ToyPartial]): Double =
    partials.sortBy(_.blockId).foldLeft(0.0)(_ + _.value)
}

/** Exercises the Dataset-only backend shape of DIST-3 end to end: one `groupByKey` builds
  * logical blocks, a persisted `Dataset` of blocks feeds `mapPartitions` kernels, and partials are
  * reduced on the driver in block order. Results must be bitwise identical whatever the physical
  * partitioning and input row order (NN-4, R0).
  */
class BackendShapeSuite extends SparkSuite {
  import BackendShapeSuite._

  private def run(input: Seq[ToyRow], partitions: Int): Double = {
    import spark.implicits._
    val blocks = spark
      .createDataset(input)
      .repartition(partitions)
      .groupByKey((row: ToyRow) => row.blockId)
      .mapGroups((blockId: Int, rows: Iterator[ToyRow]) => ToyKernels.toBlock(blockId, rows))
      .persist(StorageLevel.MEMORY_ONLY)
    try {
      val partials = blocks
        .mapPartitions((it: Iterator[ToyBlock]) => it.map(ToyKernels.partial))
        .collect()
      ToyKernels.reduce(partials.toSeq)
    } finally {
      blocks.unpersist(blocking = true)
    }
  }

  testAnsiOnAndOff("results are bitwise identical across partition counts and input orders") {
    val reference = run(Rows, partitions = 1)
    Seq("original" -> Rows, "shuffled" -> ShuffledRows).foreach { case (label, input) =>
      PartitionCounts.foreach { partitions =>
        val result = run(input, partitions)
        assertEquals(bits(result), bits(reference), s"input=$label partitions=$partitions")
      }
    }
  }

  test("the Spark result equals a Spark-free evaluation of the same kernels") {
    assertEquals(bits(run(Rows, partitions = 3)), bits(local(Rows)))
  }

  test("negative control: this data distinguishes reduction orders and in-block orders") {
    val partials = localPartials(Rows)
    val backward = partials.sortBy(p => -p.blockId).foldLeft(0.0)(_ + _.value)
    assertNotEquals(bits(backward), bits(ToyKernels.reduce(partials)), "reduction order")
    val block = localBlocks(Rows).filter(_.xs.length > 1).head
    val reversed = block.copy(rowIds = block.rowIds.reverse, xs = block.xs.reverse)
    assertNotEquals(
      bits(ToyKernels.partial(reversed).value),
      bits(ToyKernels.partial(block).value),
      "in-block order"
    )
  }
}

object BackendShapeSuite {
  val RowCount: Int = 4000
  val BlockCount: Int = 37
  val PartitionCounts: Seq[Int] = Seq(1, 3, 8)

  def bits(x: Double): Long = java.lang.Double.doubleToRawLongBits(x)

  /** Synthetic rows. Three single-row anchor blocks make reduction order matter by construction:
    * in block order, 1.0 is absorbed by 2^60 and the anchors then cancel exactly; in reverse order
    * the other blocks' sum is absorbed instead. The other rows span magnitudes 2^-40 to 2^40.
    */
  val Rows: Seq[ToyRow] = {
    val big = Math.scalb(1.0, 60)
    val anchors = Seq(ToyRow(0, 0L, 1.0), ToyRow(1, 1L, big), ToyRow(2, 2L, -big))
    val rng = new SplittableRandom(20261003L)
    val others = (anchors.size until RowCount).map { i =>
      val rowId = i.toLong
      val blockId = anchors.size + Math.floorMod(rowId * 2654435761L, BlockCount - anchors.size)
      val x = Math.scalb(rng.nextDouble() - 0.5, rng.nextInt(-40, 41))
      ToyRow(blockId.toInt, rowId, x)
    }
    anchors ++ others
  }

  val ShuffledRows: Seq[ToyRow] = new scala.util.Random(7L).shuffle(Rows)

  def localBlocks(rows: Seq[ToyRow]): Seq[ToyBlock] =
    rows.groupBy(_.blockId).toSeq.sortBy(_._1).map { case (blockId, members) =>
      ToyKernels.toBlock(blockId, members.iterator)
    }

  def localPartials(rows: Seq[ToyRow]): Seq[ToyPartial] = localBlocks(rows).map(ToyKernels.partial)

  def local(rows: Seq[ToyRow]): Double = ToyKernels.reduce(localPartials(rows))
}
