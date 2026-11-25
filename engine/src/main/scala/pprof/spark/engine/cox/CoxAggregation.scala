package pprof.spark.engine.cox

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.{array, col, explode}

import pprof.spark.engine.backend.OrderedReduction
import pprof.spark.engine.backend.Operations._
import pprof.spark.engine.data.Validation
import pprof.spark.engine.layout.{GroupFold, GroupKey}
import pprof.spark.numerics.{Evaluation, NeumaierSum, NeumaierVector}
import pprof.spark.numerics.kernels.{CoxAggregateEvaluation, CoxAggregates, CoxTimeTotals, Moments}

/** One record of pass 1 (docs/spec/cox/per-time-aggregates.md §4): `kind` 0 carries a reduction group's partial of
  * the strata it sweeps; `kind` 1 carries one chunk of an aggregated stratum's per-time totals, `ChunkTimes` grid times
  * from `chunk · ChunkTimes`, the stratum's events in chunk 0 only.
  */
final case class CoxPassOne(
    kind: Int,
    group: Int,
    stratum: Int,
    chunk: Int,
    value: Double,
    score: Array[Double],
    information: Array[Double],
    net: Array[Double],
    eventWeight: Array[Double],
    eventCount: Array[Long],
    tied: Array[Double],
    events: Array[Double]
)

/** A reduction group's sum of pass 2's Gram terms over the aggregated strata of its blocks. */
final case class CoxGramPartial(blockId: Int, values: Array[Double])

/** The strata a fit evaluates from per-time aggregates (per-time-aggregates.md, D-81): each stratum's group index and
  * its grid of exit and entry times, ascending.
  */
private[cox] final case class CoxAggregation(grids: Map[Int, Array[Double]]) {
  def isEmpty: Boolean = grids.isEmpty

  /** The aggregated strata in group order. */
  val strata: Vector[Int] = grids.keys.toVector.sorted

  /** Each aggregated stratum's position in `strata`. */
  val ordinal: Map[Int, Int] = strata.zipWithIndex.toMap
}

private[cox] object CoxAggregation {

  val Empty: CoxAggregation = CoxAggregation(Map.empty)

  /** Grid times per record of pass 1. */
  val ChunkTimes: Int = 256

  /** Numbers per grid time on the driver (§2): Δ₀, Δ₁, D and d, and under Efron T₀ and T₁; with `tables`, for the
    * residuals and robust variance (§6), at least the 4p + 5 of a stratum's residual tables.
    */
  def width(p: Int, efron: Boolean, tables: Boolean = false): Long = {
    val totals = if (efron) 2L * p + 4L else p + 3L
    if (tables) math.max(totals, 4L * p + 5L) else totals
  }

  /** The strata to aggregate among `candidates`, the strata with more rows than a block, and their grids (§2). One job
    * counts each candidate's distinct exit and entry times; strata are then taken in key order while the per-time
    * totals of all those taken fit `budgetBytes`; a second job collects the chosen strata's grids.
    */
  def choose(
      input: CoxInput,
      candidates: Seq[(GroupKey, Long)],
      width: Long,
      budgetBytes: Long
  ): Map[GroupKey, Array[Double]] =
    if (candidates.isEmpty) Map.empty
    else {
      val times = input.frame
        .filter(col(Validation.GroupColumn).isin(candidates.map(c => GroupKey.value(c._1)): _*))
        .select(
          col(Validation.GroupColumn),
          explode(array(col(CoxValidation.TimeColumn), col(CoxValidation.EntryColumn))).as("t")
        )
        .distinct()
      val counts = times
        .groupBy(Validation.GroupColumn)
        .count()
        .collectAs("cox.grid-counts")
        .map(row => GroupKey.of(row.get(0)) -> row.getLong(1))
        .toMap
      var used = 0L
      val chosen = candidates.map(_._1).sorted.filter { key =>
        val bytes = counts(key) * width * 8L
        val fits = used + bytes <= budgetBytes
        if (fits) used += bytes
        fits
      }
      if (chosen.isEmpty) Map.empty
      else
        times
          .filter(col(Validation.GroupColumn).isin(chosen.map(GroupKey.value): _*))
          .collectAs("cox.grid")
          .groupBy(row => GroupKey.of(row.get(0)))
          .map { case (key, rows) => key -> rows.map(_.getDouble(1)).sorted }
    }

  /** A reduction group's pass-1 sums: a partial of the strata it sweeps and the per-time totals of each aggregated
    * stratum it holds, both added in block order.
    */
  private final class PassOneSum(p: Int, efron: Boolean, grids: Map[Int, Array[Double]]) {
    val swept = new EvaluationSum(p)
    private val totals = scala.collection.mutable.Map.empty[Int, CoxTimeTotals]

    def totalsOf(stratum: Int): CoxTimeTotals =
      totals.getOrElseUpdate(stratum, new CoxTimeTotals(grids(stratum), p, efron))

    def records(group: Int): Iterator[CoxPassOne] = {
      val partial = swept.partial(group)
      val none = Array.emptyDoubleArray
      val head = Iterator.single(
        CoxPassOne(
          0,
          group,
          -1,
          0,
          partial.value,
          partial.score,
          partial.information,
          none,
          none,
          Array.emptyLongArray,
          none,
          none
        )
      )
      head ++ totals.keys.toVector.sorted.iterator.flatMap { stratum =>
        val t = totals(stratum)
        val (net, eventWeight, tied, events) =
          (t.net.values, t.eventWeight.values, t.tied.values, t.events.values)
        val width = p + 1
        (0 until (t.times + ChunkTimes - 1) / ChunkTimes).iterator.map { chunk =>
          val from = chunk * ChunkTimes
          val until = math.min(t.times, from + ChunkTimes)
          CoxPassOne(
            1,
            group,
            stratum,
            chunk,
            0.0,
            none,
            none,
            net.slice(from * width, until * width),
            eventWeight.slice(from, until),
            t.eventCount.slice(from, until),
            if (efron) tied.slice(from * width, until * width) else none,
            if (chunk == 0) events else none
          )
        }
      }
    }
  }

  /** Folds the parts of one key in reduction-group order: a partial passes through; a chunk's totals add up. */
  private def fold(parts: Vector[CoxPassOne]): CoxPassOne =
    if (parts.head.kind == 0) parts.head
    else {
      val first = parts.head
      def sum(arrays: Vector[Array[Double]]): Array[Double] = {
        val s = new NeumaierVector(arrays.head.length)
        arrays.foreach(s.add)
        s.values
      }
      val counts = new Array[Long](first.eventCount.length)
      parts.foreach(part => part.eventCount.indices.foreach(k => counts(k) += part.eventCount(k)))
      first.copy(
        group = -1,
        net = sum(parts.map(_.net)),
        eventWeight = sum(parts.map(_.eventWeight)),
        eventCount = counts,
        tied = sum(parts.map(_.tied)),
        events = sum(parts.map(_.events))
      )
    }

  /** Each aggregated stratum's per-time totals, in group order, from the folded chunks of pass 1. */
  private def assemble(
      folded: Array[CoxPassOne],
      aggregation: CoxAggregation,
      p: Int,
      efron: Boolean
  ): Vector[(Int, CoxTimeTotals)] = {
    val chunks = folded.filter(_.kind == 1).groupBy(_.stratum)
    aggregation.strata.map { stratum =>
      val grid = aggregation.grids(stratum)
      val parts = chunks.getOrElse(stratum, Array.empty[CoxPassOne]).sortBy(_.chunk)
      val expected = (grid.length + ChunkTimes - 1) / ChunkTimes
      if (parts.length != expected || parts.indices.exists(i => parts(i).chunk != i))
        throw new IllegalStateException(
          s"aggregated stratum $stratum returned ${parts.length} chunks of per-time totals; its grid needs $expected"
        )
      val totals = new CoxTimeTotals(grid, p, efron)
      totals.addValues(
        Array.concat(parts.map(_.net).toSeq: _*),
        Array.concat(parts.map(_.eventWeight).toSeq: _*),
        Array.concat(parts.map(_.eventCount).toSeq: _*),
        Array.concat(parts.map(_.tied).toSeq: _*),
        parts.head.events
      )
      stratum -> totals
    }
  }

  /** The aggregated strata's per-time totals at `beta` with covariates centred by `center`, for the outputs of §6: the
    * per-time records of pass 1 alone, folded by the same shuffle.
    */
  def totals(
      prepared: CoxPH.Prepared,
      beta: Array[Double],
      center: Array[Double],
      efron: Boolean,
      operation: String
  ): Vector[(Int, CoxTimeTotals)] = {
    val p = prepared.center.length
    val b = beta.clone()
    val c = center.clone()
    val groups = prepared.workingSet.groups
    val grids = prepared.aggregation.grids
    val folded = prepared.workingSet.records
      .mapPartitions((records: Iterator[CoxChunk]) =>
        GroupFold(CoxBlocks.assemble(records), (block: CoxBlock) => block.blockId, groups)(
          new PassOneSum(p, efron, grids)
        ) { (sum, block) =>
          var g = 0
          while (g < block.groupCount) {
            val stratum = block.groupIndex(g)
            if (grids.contains(stratum))
              sum.totalsOf(stratum).add(block.rows, block.groupStart(g), block.groupEnd(g), b, c)
            g += 1
          }
        }.flatMap { case (group, sum) => sum.records(group).filter(_.kind == 1) }
      )(Encoders.product[CoxPassOne])
      .groupByKey((r: CoxPassOne) => (r.kind, r.stratum, r.chunk))(
        Encoders.tuple(Encoders.scalaInt, Encoders.scalaInt, Encoders.scalaInt)
      )
      .mapGroups((_: (Int, Int, Int), parts: Iterator[CoxPassOne]) =>
        fold(parts.toVector.sortBy(_.group))
      )(
        Encoders.product[CoxPassOne]
      )
      .collectAs(operation)
    assemble(folded, prepared.aggregation, p, efron)
  }

  /** ℓ, U and I at `beta` with aggregated strata (§4): pass 1 with its shuffle, the driver's step for each aggregated
    * stratum, pass 2's Gram, then the sum: the swept strata's partials in group order, then the aggregated strata in
    * group order, their information G − M.
    */
  def evaluate(prepared: CoxPH.Prepared, beta: Array[Double], efron: Boolean): Evaluation = {
    val p = prepared.center.length
    val q = Moments.packedLength(p)
    val b = beta.clone()
    val c = prepared.center.clone()
    val groups = prepared.workingSet.groups
    val aggregation = prepared.aggregation
    val grids = aggregation.grids
    val folded = prepared.workingSet.records
      .mapPartitions((records: Iterator[CoxChunk]) =>
        GroupFold(CoxBlocks.assemble(records), (block: CoxBlock) => block.blockId, groups)(
          new PassOneSum(p, efron, grids)
        )((sum, block) =>
          sum.swept.add(
            CoxKernel.partialAggregated(block, b, c, efron, grids.contains, sum.totalsOf)
          )
        ).flatMap { case (group, sum) => sum.records(group) }
      )(Encoders.product[CoxPassOne])
      .groupByKey((r: CoxPassOne) => (r.kind, if (r.kind == 0) r.group else r.stratum, r.chunk))(
        Encoders.tuple(Encoders.scalaInt, Encoders.scalaInt, Encoders.scalaInt)
      )
      .mapGroups((_: (Int, Int, Int), parts: Iterator[CoxPassOne]) =>
        fold(parts.toVector.sortBy(_.group))
      )(
        Encoders.product[CoxPassOne]
      )
      .collectAs("cox.evaluate")
    val swept = folded.filter(_.kind == 0).sortBy(_.group).toSeq
    val evaluations: Vector[(Int, CoxAggregateEvaluation)] =
      assemble(folded, aggregation, p, efron).map { case (stratum, totals) =>
        stratum -> CoxAggregates.evaluate(totals)
      }
    val forGram = evaluations.map { case (stratum, e) =>
      stratum -> e.copy(subtractive = Array.emptyDoubleArray)
    }.toMap
    val grams = prepared.workingSet.records
      .mapPartitions((records: Iterator[CoxChunk]) =>
        GroupFold(CoxBlocks.assemble(records), (block: CoxBlock) => block.blockId, groups)(
          new NeumaierVector(q)
        )((sum, block) => sum.add(CoxKernel.gram(block, b, c, forGram))).map { case (group, sum) =>
          CoxGramPartial(group, sum.values)
        }
      )(Encoders.product[CoxGramPartial])
      .collectAs("cox.gram")
      .sortBy(_.blockId)
      .toSeq
    val value = new NeumaierSum
    swept.foreach(part => value.add(part.value))
    evaluations.foreach { case (_, e) => value.add(e.value) }
    val score = new NeumaierVector(p)
    score.add(OrderedReduction.reduce(swept.map(part => part.group -> part.score), p))
    evaluations.foreach { case (_, e) => score.add(e.score) }
    val information = new NeumaierVector(q)
    information.add(OrderedReduction.reduce(swept.map(part => part.group -> part.information), q))
    information.add(OrderedReduction.reduce(grams.map(g => g.blockId -> g.values), q))
    evaluations.foreach { case (_, e) => information.add(e.subtractive.map(-_)) }
    Evaluation(value.value, score.values, information.values)
  }
}
