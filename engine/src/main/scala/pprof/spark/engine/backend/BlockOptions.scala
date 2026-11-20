package pprof.spark.engine.backend

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/** Layout and driver limits of a blocked computation (DIST-2, DIST-4). Every value is recorded
  * with the result, so that a run can be reproduced exactly.
  *
  * @param targetBlockBytes approximate size of one logical block's arrays; it fixes the rows per
  *   block and therefore the block count (DIST-4)
  * @param maxGroupsOnDriver most groups whose sizes and results may be collected to the driver
  *   (DIST-1)
  * @param driverBudgetBytes most bytes collected to the driver by one pass (§6.8)
  * @param storageLevel name of the storage level of the working set and of the other tables a
  *   fit persists (§10.3); `OFF_HEAP` by default (D-77): off-heap memory where the session has
  *   it, `DISK_ONLY` where it has none (Spark's default), never the JVM heap
  */
final case class BlockOptions(
    targetBlockBytes: Long = BlockOptions.DefaultTargetBlockBytes,
    maxGroupsOnDriver: Int = BlockOptions.DefaultMaxGroupsOnDriver,
    driverBudgetBytes: Long = BlockOptions.DefaultDriverBudgetBytes,
    storageLevel: String = BlockOptions.DefaultStorageLevel
) {
  require(targetBlockBytes > 0, "targetBlockBytes must be positive")
  require(maxGroupsOnDriver > 0, "maxGroupsOnDriver must be positive")
  require(driverBudgetBytes > 0, "driverBudgetBytes must be positive")

  /** The storage level that `storageLevel` names; fails for an unknown name. */
  def namedStorageLevel: StorageLevel = StorageLevel.fromString(storageLevel)

  /** Name of the level to persist at in `spark`: `storageLevel`, except that an off-heap level
    * becomes `DISK_ONLY` where the session has no off-heap memory (D-77, amended in round 170).
    * Spark would put the blocks on disk anyway, after logging three warnings for each (OI-121).
    */
  def storageLevelNameIn(spark: SparkSession): String =
    BlockOptions.levelName(storageLevel, BlockOptions.hasOffHeapMemory(spark))

  /** The level to persist at in `spark` ([[storageLevelNameIn]]). */
  def storageLevelIn(spark: SparkSession): StorageLevel =
    StorageLevel.fromString(storageLevelNameIn(spark))

  /** Rows per block when each row holds `p` values, a row identifier and a group index. */
  def targetRowsPerBlock(p: Int): Int =
    math.min(math.max(1L, targetBlockBytes / (8L * (p + 2))), Int.MaxValue.toLong).toInt
}

object BlockOptions {
  val DefaultTargetBlockBytes: Long = 4L * 1024 * 1024
  val DefaultMaxGroupsOnDriver: Int = 2000000
  val DefaultDriverBudgetBytes: Long = 64L * 1024 * 1024

  /** The default storage level (D-77): off-heap memory, or disk where the session has none. */
  val DefaultStorageLevel: String = "OFF_HEAP"

  /** Whether `spark` has off-heap memory for cached blocks. Spark sizes that memory by
    * `spark.memory.offHeap.size`, whatever `spark.memory.offHeap.enabled` says, and requires a
    * positive size when the flag is on, so either setting can show it (round 170's probe).
    */
  def hasOffHeapMemory(spark: SparkSession): Boolean =
    offHeapMemory(
      spark.conf.getOption("spark.memory.offHeap.enabled"),
      spark.conf.getOption("spark.memory.offHeap.size")
    )

  /** Off-heap memory from the two settings: the flag on, or a positive size. A size that does not
    * parse counts as positive, so that the engine never forgoes memory Spark may have.
    */
  private[engine] def offHeapMemory(enabled: Option[String], size: Option[String]): Boolean =
    enabled.exists(_.trim.equalsIgnoreCase("true")) || size.exists { value =>
      value match {
        case ByteSize(digits, _) => digits.exists(_ != '0')
        case _                   => true
      }
    }

  /** A byte size as Spark reads it: digits, then an optional unit. */
  private val ByteSize = "(?i)\\s*(\\d+)\\s*(b|k|kb|m|mb|g|gb|t|tb|p|pb)?\\s*".r

  /** `name`, or `DISK_ONLY` for an off-heap level without off-heap memory; fails for an unknown
    * name.
    */
  private[engine] def levelName(name: String, offHeapMemory: Boolean): String =
    if (StorageLevel.fromString(name).useOffHeap && !offHeapMemory) "DISK_ONLY" else name
}

/** A guarded transfer to the driver would exceed its limit (DIST-1, DIST-2). */
final class DriverLimitExceededException(message: String) extends IllegalStateException(message)

/** Size guards for data collected to the driver; messages name the option to change. */
object DriverGuards {
  def requireWithinBudget(what: String, bytes: Long, options: BlockOptions): Unit =
    if (bytes > options.driverBudgetBytes)
      throw new DriverLimitExceededException(
        s"$what take about $bytes bytes on the driver, more than BlockOptions.driverBudgetBytes " +
          s"(${options.driverBudgetBytes}); raise targetBlockBytes for fewer blocks, or the budget"
      )
}
