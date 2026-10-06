package pprof.spark.engine.skeleton

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

import pprof.spark.engine.backend.{
  BlockOptions,
  BlockRecord,
  DriverGuards,
  OrderedReduction,
  WorkingSet
}
import pprof.spark.engine.data.{InputSpec, Validation}
import pprof.spark.engine.layout.{GroupKey, GroupSizes, LayoutPlan}
import pprof.spark.engine.metadata.SoftwareInfo
import pprof.spark.numerics.kernels.{Fingerprint, Moments}

/** Partial statistics of one block (§6.8). */
final case class MomentsPartial(
    blockId: Int,
    rows: Long,
    fingerprint: Long,
    sums: Array[Double],
    crossProducts: Array[Double]
)

/** Results for one group, computed within its block. */
final case class GroupMoments(groupIndex: Int, rows: Long, means: Array[Double])

/** The shape of the layout plan, recorded with the result (§6.10). */
final case class LayoutSummary(
    targetRowsPerBlock: Int,
    blockCount: Int,
    oversizedGroups: Int,
    largestBlockRows: Long,
    smallestBlockRows: Long
)

object LayoutSummary {
  def of(plan: LayoutPlan): LayoutSummary = {
    val rows = plan.blockRows
    LayoutSummary(
      plan.targetRowsPerBlock,
      plan.blockCount,
      plan.oversizedGroups,
      rows.max,
      rows.min
    )
  }
}

/** Driver-side results of [[BlockMoments.fit]] with their reproducibility metadata (NN-11, §6.10).
  * `columnSums` has one entry per feature; `crossProducts` is the packed upper triangle of the sums
  * of `x_i * x_j` (see `Moments.packedIndex`).
  */
final case class BlockMomentsSummary(
    software: SoftwareInfo,
    spec: InputSpec,
    options: BlockOptions,
    rowCount: Long,
    groupCount: Int,
    fingerprint: Long,
    layout: LayoutSummary,
    columnSums: Array[Double],
    crossProducts: Array[Double]
)

/** The summary and the group table: one row per group, sorted by key, with the columns of
  * [[BlockMoments.resultSchema]] (schema version [[BlockMoments.ResultSchemaVersion]]). The table
  * is built on the driver, so it holds no reference to the training data (ARCH-4).
  */
final case class BlockMomentsResult(summary: BlockMomentsSummary, groupTable: DataFrame)

/** The platform skeleton's statistics-free computation (D-11, ADR-0004): per-group means plus
  * global column sums and cross products, through the whole engine path. It is not a pprof_py
  * feature, and it is Experimental (NN-12).
  */
object BlockMoments {

  val ResultSchemaVersion: Int = 1
  val RowsColumn: String = "rows"

  def meanColumn(feature: String): String = s"mean_$feature"

  /** Columns of the group table: the group key, the row count, one mean per feature. */
  def resultSchema(spec: InputSpec, keyIsText: Boolean): StructType = StructType(
    StructField(spec.groupCol, if (keyIsText) StringType else LongType, nullable = false) +:
      StructField(RowsColumn, LongType, nullable = false) +:
      spec.featureCols.map(name => StructField(meanColumn(name), DoubleType, nullable = false))
  )

  /** Validates `df`, plans the layout from group sizes, builds the working set, applies the
    * kernels and reduces the partials in block order. The driver receives group sizes, one partial
    * per block and one result row per group, each under a size guard (DIST-1); rows never reach it.
    */
  def fit(
      df: DataFrame,
      spec: InputSpec,
      options: BlockOptions = BlockOptions()
  ): BlockMomentsResult = {
    val spark = df.sparkSession
    val input = Validation.validate(df, spec)
    val p = spec.featureCols.size
    val plan =
      LayoutPlan.create(GroupSizes.collectGroupSizes(input, options), options.targetRowsPerBlock(p))
    DriverGuards.requireWithinBudget(
      s"${plan.blockCount} block partials",
      plan.blockCount.toLong * 8L * (3L + p + Moments.packedLength(p)),
      options
    )
    DriverGuards.requireWithinBudget(
      s"results for ${plan.groupCount} groups",
      plan.groupCount.toLong * 8L * (2L + p),
      options
    )
    val workingSet = WorkingSet.build(input, plan, options.resolvedStorageLevel)
    try {
      val combined = MomentsKernel.combine(collectPartials(workingSet).toSeq, plan, p)
      if (combined.rows != input.rowCount)
        throw new IllegalStateException(
          s"the blocks hold ${combined.rows} rows; the input has ${input.rowCount}"
        )
      val summary = BlockMomentsSummary(
        SoftwareInfo.capture(spark),
        spec,
        options,
        combined.rows,
        plan.groupCount,
        combined.fingerprint,
        LayoutSummary.of(plan),
        combined.sums,
        combined.crossProducts
      )
      val groups = collectGroupMoments(workingSet).toSeq
      BlockMomentsResult(summary, groupTable(spark, spec, input.groupKeyIsText, plan, groups))
    } finally workingSet.release()
  }

  private def collectPartials(workingSet: WorkingSet): Array[MomentsPartial] =
    workingSet.blocks
      .map((block: BlockRecord) => MomentsKernel.partial(block))(Encoders.product[MomentsPartial])
      .collect()

  private def collectGroupMoments(workingSet: WorkingSet): Array[GroupMoments] =
    workingSet.blocks
      .flatMap((block: BlockRecord) => MomentsKernel.groupMeans(block))(
        Encoders.product[GroupMoments]
      )
      .collect()

  private def groupTable(
      spark: SparkSession,
      spec: InputSpec,
      keyIsText: Boolean,
      plan: LayoutPlan,
      groups: Seq[GroupMoments]
  ): DataFrame = {
    val byIndex = groups.sortBy(_.groupIndex)
    require(
      byIndex.iterator.map(_.groupIndex).sameElements(plan.placements.iterator.map(_.groupIndex)),
      "every group must have exactly one result"
    )
    val rows = plan.placements.zip(byIndex).map { case (placement, group) =>
      val cells: Vector[Any] = Vector[Any](GroupKey.value(placement.key), group.rows) ++ group.means
      Row.fromSeq(cells)
    }
    spark.createDataFrame(rows.asJava, resultSchema(spec, keyIsText))
  }
}

/** The kernels of [[BlockMoments]]: pure functions of one block, run on executors (§6.4). */
object MomentsKernel {

  /** Reduced totals of all blocks. */
  final case class Combined(
      rows: Long,
      fingerprint: Long,
      sums: Array[Double],
      crossProducts: Array[Double]
  )

  def partial(block: BlockRecord): MomentsPartial = {
    val p = block.p
    var fingerprint = 0L
    var k = 0
    while (k < block.groupCount) {
      var r = block.groupStart(k)
      while (r < block.groupStart(k + 1)) {
        fingerprint += Fingerprint.row(block.groupIndex(k), block.rowId(r), block.values, r * p, p)
        r += 1
      }
      k += 1
    }
    MomentsPartial(
      block.blockId,
      block.rowCount.toLong,
      fingerprint,
      Moments.columnSums(block.values, p, 0, block.rowCount),
      Moments.crossProducts(block.values, p, 0, block.rowCount)
    )
  }

  def groupMeans(block: BlockRecord): Seq[GroupMoments] =
    (0 until block.groupCount).map { k =>
      val from = block.groupStart(k)
      val until = block.groupStart(k + 1)
      val sums = Moments.columnSums(block.values, block.p, from, until)
      GroupMoments(block.groupIndex(k), (until - from).toLong, sums.map(_ / (until - from)))
    }

  /** Reduces one partial per block in block order (§6.8); fails unless every block reported. */
  def combine(partials: Seq[MomentsPartial], plan: LayoutPlan, p: Int): Combined = {
    require(
      partials.map(_.blockId).sorted == (0 until plan.blockCount),
      s"expected one partial for each of ${plan.blockCount} blocks"
    )
    Combined(
      partials.iterator.map(_.rows).sum,
      partials.iterator.map(_.fingerprint).foldLeft(0L)(_ + _),
      OrderedReduction.reduce(partials.map(partial => partial.blockId -> partial.sums), p),
      OrderedReduction.reduce(
        partials.map(partial => partial.blockId -> partial.crossProducts),
        Moments.packedLength(p)
      )
    )
  }
}
