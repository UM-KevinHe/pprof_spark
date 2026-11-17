package pprof.spark.engine.layout

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, spark_partition_id}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

import pprof.spark.engine.backend.{BlockOptions, DriverLimitExceededException}
import pprof.spark.testkit.SparkSuite

/** Reduction groups and partition keys (docs/spec/performance/step1-reduction.md §2 and §3). */
class ReductionGroupsSuite extends SparkSuite {

  private val budget = BlockOptions(driverBudgetBytes = 1000L)

  test("one block per group while every partial fits the budget together (G = 1)") {
    val g = ReductionGroups.plan(blocks = 10, partialBytes = 100L, budget)
    assertEquals((g.blocksPerGroup, g.groups), (1, 10))
    assertEquals((0 until 10).map(g.groupOf), 0 until 10)
  }

  test("beyond the budget, the fewest blocks per group that fit, in groups of consecutive blocks") {
    val g = ReductionGroups.plan(blocks = 25, partialBytes = 100L, budget)
    assertEquals((g.blocksPerGroup, g.groups), (3, 9))
    assertEquals(g.blocksOf(0), 0 until 3)
    assertEquals(g.blocksOf(8), 24 until 25)
    assertEquals(g.groupOf(23), 7)
    assertEquals((0 until g.groups).flatMap(g.blocksOf), 0 until 25)
  }

  test(
    "for any number of blocks, the groups' partials fit the budget, with the fewest blocks per group"
  ) {
    for (blocks <- 1 to 400; partial <- Seq(1L, 7L, 100L, 333L, 1000L)) {
      val g = ReductionGroups.plan(blocks, partial, budget)
      assert(
        g.groups * partial <= budget.driverBudgetBytes,
        s"$blocks blocks of $partial bytes: $g"
      )
      if (g.blocksPerGroup > 1)
        assert(
          ReductionGroups(blocks, g.blocksPerGroup - 1).groups * partial > budget.driverBudgetBytes,
          s"$blocks blocks of $partial bytes: $g is not the fewest"
        )
      assertEquals((0 until g.groups).flatMap(g.blocksOf), 0 until blocks)
    }
  }

  test("a partial larger than the budget is refused") {
    intercept[DriverLimitExceededException](
      ReductionGroups.plan(blocks = 5, partialBytes = 2000L, budget)
    )
  }

  test(
    "partition keys: Spark's hash partitioning sends each group's key to the group's own partition"
  ) {
    for (partitions <- Seq(1, 2, 7, 64, 509)) {
      val keys = PartitionKeys.forPartitions(spark, partitions)
      assertEquals(keys.length, partitions)
      val schema = StructType(
        Seq(StructField("group", IntegerType, false), StructField("key", LongType, false))
      )
      val rows = keys.toSeq.zipWithIndex.map { case (k, g) => Row(g, k) }
      val placed = spark
        .createDataFrame(rows.asJava, schema)
        .repartition(partitions, col("key"))
        .select(col("group"), spark_partition_id().as("partition"))
        .collect()
      assertEquals(placed.length, partitions)
      assert(
        placed.forall(r => r.getInt(0) == r.getInt(1)),
        placed.filter(r => r.getInt(0) != r.getInt(1)).mkString
      )
    }
  }

  test(
    "GroupFold folds each group's blocks in block order, and fails on a block or a group out of order"
  ) {
    val groups = ReductionGroups(blocks = 7, blocksPerGroup = 3)
    val folded = GroupFold((0 until 7).iterator, (b: Int) => b, groups)(Vector.newBuilder[Int])(
      (acc, b) => acc += b
    ).map { case (g, acc) => g -> acc.result() }.toVector
    assertEquals(folded, Vector(0 -> Vector(0, 1, 2), 1 -> Vector(3, 4, 5), 2 -> Vector(6)))
    intercept[IllegalStateException](
      GroupFold(Iterator(0, 2, 1), (b: Int) => b, groups)(0)((_, _) => ()).toVector
    )
    intercept[IllegalStateException](
      GroupFold(Iterator(3, 4, 0), (b: Int) => b, groups)(0)((_, _) => ()).toVector
    )
  }
}
