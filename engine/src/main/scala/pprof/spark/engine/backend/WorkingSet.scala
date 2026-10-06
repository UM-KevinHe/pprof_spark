package pprof.spark.engine.backend

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import pprof.spark.engine.data.{ValidatedInput, Validation}
import pprof.spark.engine.layout.{GroupKey, LayoutPlan}

/** The materialized working set of a computation: its logical blocks, persisted with an explicit
  * storage level (§6.5, §10.3). Call [[release]] in a `finally` block.
  */
final class WorkingSet private (val blocks: Dataset[BlockRecord], val plan: LayoutPlan) {
  def release(): Unit = {
    blocks.unpersist(blocking = false)
    ()
  }
}

object WorkingSet {

  /** Builds and materializes the blocks of `input` under `plan` with one shuffle of the rows
    * (§10.2). The plan reaches executors through a broadcast join (§6.7); blocks are assembled on
    * executors; only the block count reaches the driver. Connect-compatible.
    */
  def build(input: ValidatedInput, plan: LayoutPlan, storageLevel: StorageLevel): WorkingSet = {
    val p = input.spec.featureCols.size
    val placements = PlanFrames.placements(input.frame.sparkSession, plan, input.groupKeyIsText)
    val placed = input.frame
      .join(broadcast(placements), Seq(Validation.GroupColumn))
      .select(
        col(PlanFrames.BlockIdColumn).as("blockId"),
        col(PlanFrames.GroupIndexColumn).as("groupIndex"),
        col(Validation.RowIdColumn).as("rowId"),
        col(Validation.FeaturesColumn).as("x")
      )
      .as[PlacedRow](Encoders.product[PlacedRow])
    val blocks = placed
      .groupByKey((row: PlacedRow) => row.blockId)(Encoders.scalaInt)
      .mapGroups((blockId: Int, rows: Iterator[PlacedRow]) => BlockBuilder.build(blockId, rows, p))(
        Encoders.product[BlockRecord]
      )
      .persist(storageLevel)
    try {
      val built = blocks.count()
      if (built != plan.blockCount)
        throw new IllegalStateException(s"built $built blocks; the plan has ${plan.blockCount}")
      new WorkingSet(blocks, plan)
    } catch {
      case e: Throwable =>
        blocks.unpersist(blocking = false)
        throw e
    }
  }
}

/** Driver-built tables that carry a layout plan to executors through broadcast joins (§6.7). */
object PlanFrames {

  val GroupIndexColumn: String = "__pprof_group_index"
  val BlockIdColumn: String = "__pprof_block_id"

  /** One row per group: its key, group index and block identifier. */
  def placements(spark: SparkSession, plan: LayoutPlan, keyIsText: Boolean): DataFrame = {
    val schema = StructType(
      Seq(
        StructField(
          Validation.GroupColumn,
          if (keyIsText) StringType else LongType,
          nullable = false
        ),
        StructField(GroupIndexColumn, IntegerType, nullable = false),
        StructField(BlockIdColumn, IntegerType, nullable = false)
      )
    )
    val rows = plan.placements.map(g => Row(GroupKey.value(g.key), g.groupIndex, g.blockId))
    spark.createDataFrame(rows.asJava, schema)
  }
}
