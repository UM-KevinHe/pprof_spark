package pprof.spark.engine.skeleton

import java.nio.file.Files
import java.util.SplittableRandom

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}

import pprof.spark.engine.backend.{
  BlockBuilder,
  BlockOptions,
  DriverLimitExceededException,
  PlacedRow
}
import pprof.spark.engine.data.InputSpec
import pprof.spark.engine.layout.{GroupKey, LayoutPlan}
import pprof.spark.testkit.{SparkSuite, Tolerances}

/** The platform skeleton end to end (D-11): bitwise invariance to input order and partitioning
  * (R0), agreement with a Spark-free evaluation, layout invariance within T-part (R1), guards,
  * persistence (PERS-1) and the result schema. Runs under Classic Spark (T3) and Spark Connect (T8).
  */
class BlockMomentsSuite extends SparkSuite {
  import BlockMomentsSuite._

  private def frame(rows: Seq[Row], schema: StructType): DataFrame =
    spark.createDataFrame(rows.asJava, schema)

  private def fit(rows: Seq[Row], options: BlockOptions = SmallBlocks): BlockMomentsResult =
    BlockMoments.fit(frame(rows, Schema), Spec, options)

  testAnsiOnAndOff("results are bitwise identical across input orders and partition counts") {
    val reference = fit(Rows)
    Seq("original" -> Rows, "shuffled" -> ShuffledRows).foreach { case (label, rows) =>
      Seq(1, 4).foreach { partitions =>
        val result =
          BlockMoments.fit(frame(rows, Schema).repartition(partitions), Spec, SmallBlocks)
        assertEquals(summaryBits(result), summaryBits(reference), s"$label, $partitions partitions")
        assertEquals(tableBits(result), tableBits(reference), s"$label, $partitions partitions")
      }
    }
  }

  test("the Spark result equals a Spark-free evaluation of the same plan and kernels") {
    val result = fit(Rows)
    val (combined, groups, plan) = local(Rows, integralKey, SmallBlocks)
    assertEquals(result.summary.layout.blockCount, plan.blockCount)
    assert(plan.blockCount > 5, s"the test needs several blocks, got ${plan.blockCount}")
    assertEquals(
      summaryBits(result),
      Seq(combined.rows, plan.groupCount.toLong, combined.fingerprint) ++
        combined.sums.toSeq.map(bits) ++ combined.crossProducts.toSeq.map(bits)
    )
    val expectedTable = plan.placements.zip(groups).map { case (placement, group) =>
      Seq[Any](GroupKey.value(placement.key), group.rows) ++ group.means.toSeq.map(bits)
    }
    assertEquals(tableBits(result), expectedTable)
  }

  test("integer-valued features give exact sums") {
    val result = fit(Rows)
    val x3 = Rows.map(_.getInt(4).toLong)
    assertEquals(result.summary.columnSums(2), x3.sum.toDouble)
    assertEquals(result.summary.crossProducts(5), x3.map(v => v * v).sum.toDouble)
  }

  test("a different block size changes the layout, but results stay within T-part") {
    val small = fit(Rows)
    val single = fit(Rows, BlockOptions())
    assertEquals(single.summary.layout.blockCount, 1)
    assertNotEquals(small.summary.layout.blockCount, 1)
    assertEquals(single.summary.fingerprint, small.summary.fingerprint)
    assertEquals(tableBits(single), tableBits(small), "group means are computed within groups")
    val tolerance = Tolerances("T-part.parameters")
    val pairs = single.summary.columnSums.zip(small.summary.columnSums) ++
      single.summary.crossProducts.zip(small.summary.crossProducts)
    pairs.foreach { case (a, b) => assert(tolerance.accepts(a, b), s"$a vs $b") }
  }

  test("string keys and oversized groups") {
    val rows = TextRows
    val result = BlockMoments.fit(frame(rows, TextSchema), Spec, SmallBlocks)
    val (combined, groups, plan) = local(rows, textKey, SmallBlocks)
    assertEquals(plan.oversizedGroups, 1)
    assertEquals(result.summary.layout.oversizedGroups, 1)
    assertEquals(result.summary.fingerprint, combined.fingerprint)
    assertEquals(result.summary.columnSums.toSeq.map(bits), combined.sums.toSeq.map(bits))
    val keys = result.groupTable.collect().toSeq.map(_.getString(0))
    assertEquals(keys, plan.placements.map(_.key).collect { case GroupKey.Text(v) => v })
    assertEquals(keys, Seq("A", "b", "\u00e9", "\uffff"))
    assertEquals(groups.map(_.rows), Seq(50L, 40L, 200L, 30L))
  }

  test("size guards fail with messages that name the option to change") {
    val groups =
      intercept[DriverLimitExceededException](fit(Rows, BlockOptions(maxGroupsOnDriver = 3)))
    assert(groups.getMessage.contains("BlockOptions.maxGroupsOnDriver"), groups.getMessage)
    val budget =
      intercept[DriverLimitExceededException](fit(Rows, SmallBlocks.copy(driverBudgetBytes = 100)))
    assert(budget.getMessage.contains("BlockOptions.driverBudgetBytes"), budget.getMessage)
  }

  test("save and load round-trip bit for bit, and never overwrite") {
    val result = fit(Rows)
    val path = Files.createTempDirectory("pprof-moments").resolve("model").toString
    BlockMomentsIO.save(result, path)
    val loaded = BlockMomentsIO.load(spark, path)
    assertEquals(summaryBits(loaded), summaryBits(result))
    assertEquals(loaded.summary.software, result.summary.software)
    assertEquals(loaded.summary.spec, result.summary.spec)
    assertEquals(loaded.summary.options, result.summary.options)
    assertEquals(loaded.summary.layout, result.summary.layout)
    assertEquals(tableBits(loaded), tableBits(result))
    intercept[Exception](BlockMomentsIO.save(result, path))
  }

  test("the group table has the documented schema, sorted by key") {
    val table = fit(Rows).groupTable
    assertEquals(table.schema, BlockMoments.resultSchema(Spec, keyIsText = false))
    assertEquals(table.columns.toSeq, Seq("provider", "rows", "mean_x1", "mean_x2", "mean_x3"))
    val keys = table.collect().toSeq.map(_.getLong(0))
    assertEquals(keys, keys.sorted)
  }
}

object BlockMomentsSuite {

  def bits(x: Double): Long = java.lang.Double.doubleToRawLongBits(x)

  val Spec: InputSpec = InputSpec("provider", Seq("x1", "x2", "x3"), Some("id"))

  /** 60 rows per block for three features (8 * (3 + 2) bytes per row). */
  val SmallBlocks: BlockOptions = BlockOptions(targetBlockBytes = 8L * 5 * 60)

  val Schema: StructType = StructType(
    Seq(
      StructField("provider", LongType, nullable = false),
      StructField("id", LongType, nullable = false),
      StructField("x1", DoubleType, nullable = false),
      StructField("x2", DoubleType, nullable = false),
      StructField("x3", IntegerType, nullable = false)
    )
  )

  val TextSchema: StructType = StructType(
    StructField("provider", StringType, nullable = false) +: Schema.fields.tail.toSeq
  )

  private def features(rng: SplittableRandom): (Double, Double, Int) =
    (
      1.0 + rng.nextDouble(),
      Math.scalb(1.0 + rng.nextDouble(), rng.nextInt(-10, 11)),
      rng.nextInt(10)
    )

  /** 600 rows in 17 providers with keys from -40 to 72; positive features, so sums do not cancel. */
  val Rows: Seq[Row] = {
    val rng = new SplittableRandom(20261005L)
    (0 until 600).map { i =>
      val (x1, x2, x3) = features(rng)
      Row(rng.nextInt(17) * 7L - 40L, i.toLong, x1, x2, x3)
    }
  }

  val ShuffledRows: Seq[Row] = new scala.util.Random(9L).shuffle(Rows)

  /** Four string-keyed providers; one of them has more rows than a block holds. */
  val TextRows: Seq[Row] = {
    val rng = new SplittableRandom(7L)
    val sizes = Seq("b" -> 40, "\u00e9" -> 200, "A" -> 50, "\uffff" -> 30)
    sizes.flatMap { case (key, n) => Seq.fill(n)(key) }.zipWithIndex.map { case (key, i) =>
      val (x1, x2, x3) = features(rng)
      Row(key, i.toLong, x1, x2, x3)
    }
  }

  def integralKey(row: Row): GroupKey = GroupKey.Integral(row.getLong(0))
  def textKey(row: Row): GroupKey = GroupKey.Text(row.getString(0))

  /** Every reported number of a result, as bits. */
  def summaryBits(result: BlockMomentsResult): Seq[Long] = {
    val s = result.summary
    Seq(s.rowCount, s.groupCount.toLong, s.fingerprint) ++
      s.columnSums.toSeq.map(bits) ++ s.crossProducts.toSeq.map(bits)
  }

  /** The group table, with doubles as bits. */
  def tableBits(result: BlockMomentsResult): Seq[Seq[Any]] =
    result.groupTable
      .collect()
      .toSeq
      .map(_.toSeq.map[Any] {
        case d: Double => bits(d)
        case other     => other
      })

  /** The same plan, blocks, kernels and reduction, evaluated without Spark. */
  def local(
      rows: Seq[Row],
      key: Row => GroupKey,
      options: BlockOptions
  ): (MomentsKernel.Combined, Seq[GroupMoments], LayoutPlan) = {
    val p = 3
    val keyed = rows.map(r =>
      (key(r), r.getLong(1), Array(r.getDouble(2), r.getDouble(3), r.getInt(4).toDouble))
    )
    val sizes = keyed.groupBy(_._1).toSeq.map { case (k, members) => k -> members.size.toLong }
    val plan = LayoutPlan.create(sizes, options.targetRowsPerBlock(p))
    val placementOf = plan.placements.map(g => g.key -> g).toMap
    val placed = keyed.map { case (k, id, x) =>
      val g = placementOf(k)
      PlacedRow(g.blockId, g.groupIndex, id, x)
    }
    val blocks = placed.groupBy(_.blockId).toSeq.sortBy(_._1).map { case (blockId, members) =>
      BlockBuilder.build(blockId, members.iterator, p)
    }
    val groups = blocks.flatMap(MomentsKernel.groupMeans).sortBy(_.groupIndex)
    (MomentsKernel.combine(blocks.map(MomentsKernel.partial), plan, p), groups, plan)
  }
}
