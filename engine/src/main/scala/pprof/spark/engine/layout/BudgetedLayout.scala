package pprof.spark.engine.layout

import pprof.spark.engine.backend.BlockOptions

/** The layout plan of a blocked computation whose block partials are collected to the driver, with
  * the block size grown until those partials fit the driver budget (issue L5, D-49).
  *
  * At the requested `targetBlockBytes` the plan has `blockCount` blocks whose partials take
  * `partialBytes` each on the driver. When they would exceed `driverBudgetBytes`, the block size
  * doubles, up to [[MaxGrownBlockBytes]], until they fit. A plan that fits at the requested size is
  * returned unchanged, so growth happens only where the budget check would otherwise fail; when even
  * the largest blocks do not fit, the plan at that size is returned and the caller's budget check
  * fails as before. The plan depends only on the group sizes and the options, so a fit and every
  * later call that rebuilds its working set find the same plan.
  */
object BudgetedLayout {

  /** The largest block size growth reaches; an explicit larger `targetBlockBytes` is kept. */
  val MaxGrownBlockBytes: Long = 256L * 1024 * 1024

  /** @param width the value count passed to `BlockOptions.targetRowsPerBlock`
    * @param partialBytes the bytes one block's partial takes on the driver
    */
  def plan(
      groupSizes: Seq[(GroupKey, Long)],
      options: BlockOptions,
      width: Int,
      partialBytes: Long
  ): LayoutPlan = {
    def at(bytes: Long): LayoutPlan =
      LayoutPlan.create(
        groupSizes,
        options.copy(targetBlockBytes = bytes).targetRowsPerBlock(width)
      )
    def fits(plan: LayoutPlan): Boolean =
      plan.blockCount.toLong * partialBytes <= options.driverBudgetBytes
    var bytes = options.targetBlockBytes
    var current = at(bytes)
    while (!fits(current) && bytes < MaxGrownBlockBytes) {
      bytes = math.min(bytes * 2, MaxGrownBlockBytes)
      current = at(bytes)
    }
    current
  }
}
