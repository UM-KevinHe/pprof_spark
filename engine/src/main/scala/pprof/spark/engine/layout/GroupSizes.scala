package pprof.spark.engine.layout

import org.apache.spark.sql.DataFrame

import pprof.spark.engine.backend.{BlockOptions, DriverLimitExceededException}
import pprof.spark.engine.data.{ValidatedInput, Validation}

object GroupSizes {

  /** Collects the row count of every group to the driver: m-scale data, guarded by
    * `BlockOptions.maxGroupsOnDriver` (DIST-1). One Spark job; Connect-compatible.
    */
  def collectGroupSizes(input: ValidatedInput, options: BlockOptions): Vector[(GroupKey, Long)] =
    collectGroupSizes(input.frame, options)

  /** The same for any validated frame that carries [[Validation.GroupColumn]]. */
  def collectGroupSizes(frame: DataFrame, options: BlockOptions): Vector[(GroupKey, Long)] = {
    val limit = math.min(options.maxGroupsOnDriver.toLong + 1, Int.MaxValue.toLong).toInt
    val rows = frame.groupBy(Validation.GroupColumn).count().limit(limit).collect()
    if (rows.length > options.maxGroupsOnDriver)
      throw new DriverLimitExceededException(
        s"the input has more than ${options.maxGroupsOnDriver} groups, the limit set by " +
          "BlockOptions.maxGroupsOnDriver; raise it if the driver has the memory"
      )
    rows.iterator.map(row => GroupKey.of(row.get(0)) -> row.getLong(1)).toVector
  }
}
