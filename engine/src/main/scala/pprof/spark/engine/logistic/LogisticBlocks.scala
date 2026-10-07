package pprof.spark.engine.logistic

import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.storage.StorageLevel
import pprof.spark.engine.backend.PlanFrames
import pprof.spark.engine.data.Validation
import pprof.spark.engine.layout.LayoutPlan
import pprof.spark.numerics.kernels.{LogisticFE => Kernel}

/** One input row placed in its block (§6.6). */
final case class LogisticRow(
    blockId: Int,
    groupIndex: Int,
    rowId: Long,
    y: Double,
    n: Double,
    x: Array[Double]
)

/** Whole providers in canonical order (Phase 2a specification §11, DIST-6), covariates row-major.
  * Provider `g` holds rows `groupStart(g)` until the next start. `gamma` holds the providers'
  * effects for one pass; it is empty in the persisted working set.
  */
final case class LogisticBlock(
    blockId: Int,
    p: Int,
    groupIndex: Array[Int],
    groupStart: Array[Int],
    rowId: Array[Long],
    y: Array[Double],
    n: Array[Double],
    x: Array[Double],
    gamma: Array[Double]
) {
  def rows: Kernel.Rows = Kernel.Rows(groupStart, y, n, x, p)
}

/** Per block: counts, Σ n x, the fingerprint, and records, events and trials of each provider. */
final case class LogisticSummary(
    blockId: Int,
    rows: Long,
    events: Double,
    trials: Double,
    weightedX: Array[Double],
    fingerprint: Long,
    records: Array[Long],
    providerEvents: Array[Double],
    providerTrials: Array[Double]
)

final case class LogisticSchurPartial(
    blockId: Int,
    packed: Array[Double],
    rhs: Array[Double],
    scoreTerm: Double,
    loglik: Double
)

final case class LogisticTrialPartial(
    blockId: Int,
    deltaGamma: Array[Double],
    loglik: Array[Double]
)

final case class LogisticVariancePartial(blockId: Int, packed: Array[Double], loglik: Double)

final case class LogisticProviderPartial(
    blockId: Int,
    varGamma: Array[Double],
    varCaseMix: Array[Double]
)

object LogisticBlockBuilder {

  /** Provider, row identifier, then covariate values, events and trials. */
  val canonicalOrder: Ordering[LogisticRow] = new Ordering[LogisticRow] {
    def compare(a: LogisticRow, b: LogisticRow): Int = {
      val byGroup = Integer.compare(a.groupIndex, b.groupIndex)
      if (byGroup != 0) byGroup
      else {
        val byRowId = java.lang.Long.compare(a.rowId, b.rowId)
        if (byRowId != 0) byRowId
        else {
          var i = 0
          var result = 0
          while (result == 0 && i < math.min(a.x.length, b.x.length)) {
            result = java.lang.Double.compare(a.x(i), b.x(i))
            i += 1
          }
          if (result != 0) result
          else {
            val byEvents = java.lang.Double.compare(a.y, b.y)
            if (byEvents != 0) byEvents else java.lang.Double.compare(a.n, b.n)
          }
        }
      }
    }
  }

  /** Sorts a block's rows canonically after normalizing negative zeros (ADR-0004). */
  def build(blockId: Int, rows: Iterator[LogisticRow], p: Int): LogisticBlock = {
    val sorted = rows
      .map(r => r.copy(y = r.y + 0.0, n = r.n + 0.0, x = r.x.map(v => if (v == 0.0) 0.0 else v)))
      .toArray
      .sorted(canonicalOrder)
    val count = sorted.length
    val rowIds = new Array[Long](count)
    val y = new Array[Double](count)
    val n = new Array[Double](count)
    val x = new Array[Double](count * p)
    val groups = Array.newBuilder[Int]
    val starts = Array.newBuilder[Int]
    var r = 0
    while (r < count) {
      val row = sorted(r)
      require(row.x.length == p, s"expected $p covariates, got ${row.x.length}")
      if (r == 0 || row.groupIndex != sorted(r - 1).groupIndex) {
        groups += row.groupIndex
        starts += r
      }
      rowIds(r) = row.rowId
      y(r) = row.y
      n(r) = row.n
      System.arraycopy(row.x, 0, x, r * p, p)
      r += 1
    }
    LogisticBlock(
      blockId,
      p,
      groups.result(),
      starts.result(),
      rowIds,
      y,
      n,
      x,
      Array.emptyDoubleArray
    )
  }
}

/** The persisted logistic working set (§6.7). */
final class LogisticWorkingSet private (val blocks: Dataset[LogisticBlock], val plan: LayoutPlan) {
  def release(): Unit = {
    blocks.unpersist(blocking = false)
    ()
  }
}

object LogisticWorkingSet {

  /** Joins the plan to the rows (dropping screened providers), builds one block per plan block
    * with one shuffle, and persists the blocks at `storageLevel`.
    */
  def build(
      input: LogisticInput,
      plan: LayoutPlan,
      storageLevel: StorageLevel
  ): LogisticWorkingSet = {
    val p = input.spec.features.size
    val placements = PlanFrames.placements(input.frame.sparkSession, plan, input.providerKeyIsText)
    val placed = input.frame
      .join(broadcast(placements), Seq(Validation.GroupColumn))
      .select(
        col(PlanFrames.BlockIdColumn).as("blockId"),
        col(PlanFrames.GroupIndexColumn).as("groupIndex"),
        col(Validation.RowIdColumn).as("rowId"),
        col(LogisticValidation.OutcomeColumn).as("y"),
        col(LogisticValidation.TrialsColumn).as("n"),
        col(Validation.FeaturesColumn).as("x")
      )
      .as[LogisticRow](Encoders.product[LogisticRow])
    val blocks = placed
      .groupByKey((row: LogisticRow) => row.blockId)(Encoders.scalaInt)
      .mapGroups((blockId: Int, rows: Iterator[LogisticRow]) =>
        LogisticBlockBuilder.build(blockId, rows, p)
      )(
        Encoders.product[LogisticBlock]
      )
      .persist(storageLevel)
    try {
      val built = blocks.count()
      if (built != plan.blockCount)
        throw new IllegalStateException(s"built $built blocks; the plan has ${plan.blockCount}")
      new LogisticWorkingSet(blocks, plan)
    } catch {
      case e: Throwable =>
        blocks.unpersist(blocking = false)
        throw e
    }
  }
}
