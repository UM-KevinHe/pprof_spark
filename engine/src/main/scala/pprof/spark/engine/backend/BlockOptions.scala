package pprof.spark.engine.backend

import org.apache.spark.storage.StorageLevel

/** Layout and driver limits of a blocked computation (DIST-2, DIST-4). Every value is recorded
  * with the result, so that a run can be reproduced exactly.
  *
  * @param targetBlockBytes approximate size of one logical block's arrays; it fixes the rows per
  *   block and therefore the block count (DIST-4)
  * @param maxGroupsOnDriver most groups whose sizes and results may be collected to the driver
  *   (DIST-1)
  * @param driverBudgetBytes most bytes collected to the driver by one pass (§6.8)
  * @param storageLevel name of the working set's storage level (§10.3)
  */
final case class BlockOptions(
    targetBlockBytes: Long = BlockOptions.DefaultTargetBlockBytes,
    maxGroupsOnDriver: Int = BlockOptions.DefaultMaxGroupsOnDriver,
    driverBudgetBytes: Long = BlockOptions.DefaultDriverBudgetBytes,
    storageLevel: String = "MEMORY_AND_DISK"
) {
  require(targetBlockBytes > 0, "targetBlockBytes must be positive")
  require(maxGroupsOnDriver > 0, "maxGroupsOnDriver must be positive")
  require(driverBudgetBytes > 0, "driverBudgetBytes must be positive")

  /** The working set's storage level; fails for an unknown name. */
  def resolvedStorageLevel: StorageLevel = StorageLevel.fromString(storageLevel)

  /** Rows per block when each row holds `p` values, a row identifier and a group index. */
  def targetRowsPerBlock(p: Int): Int =
    math.min(math.max(1L, targetBlockBytes / (8L * (p + 2))), Int.MaxValue.toLong).toInt
}

object BlockOptions {
  val DefaultTargetBlockBytes: Long = 4L * 1024 * 1024
  val DefaultMaxGroupsOnDriver: Int = 2000000
  val DefaultDriverBudgetBytes: Long = 64L * 1024 * 1024
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
