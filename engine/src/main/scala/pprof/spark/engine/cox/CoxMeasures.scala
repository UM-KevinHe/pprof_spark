package pprof.spark.engine.cox

import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.functions.{broadcast, col, lit}

import pprof.spark.engine.backend.PlanFrames
import pprof.spark.engine.data.Validation
import pprof.spark.numerics.kernels.{CoxMeasures => MeasureKernels}

/** The standardization of a provider's events (Phase 1d specification §1). */
sealed abstract class Standardization(val name: String) extends Product with Serializable

object Standardization {

  /** Oⱼ/Eⱼ: observed over expected at the national baseline. */
  case object Indirect extends Standardization("indirect")

  /** E⁽ʲ⁾/O: events expected if every patient had provider j's baseline, over all observed. */
  case object Direct extends Standardization("direct")
}

/** Indirect and direct standardized ratios per provider: two-stage SMR and SHR with a fit
  * stratified by provider, or the pooled model's with an unstratified fit (Phase 1d specification
  * §1, §3, §4), with pprof_py's conventions: Breslow baselines whatever the ties, no case weights.
  */
object CoxMeasures {

  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  /** One distributed table per requested kind, keyed by the `provider` column of `df`, which must
    * be the data `fit` was fitted to (API-3). `providers`, when given, filters the output only.
    */
  def standardized(
      df: DataFrame,
      fit: CoxFit,
      provider: String,
      kinds: Seq[Standardization] = Seq(Standardization.Indirect),
      providers: Option[Seq[Any]] = None
  ): Map[Standardization, DataFrame] = {
    require(kinds.nonEmpty, "at least one standardization is required")
    val sameLayout = fit.spec.strata.contains(provider)
    if (!sameLayout) CoxPH.requireFittedData(df, fit)
    CoxPH.withWorkingSet(df, fit.spec.copy(strata = Some(provider)), fit.options) { prepared =>
      if (sameLayout && prepared.fingerprint != fit.fingerprint)
        throw new IllegalArgumentException(
          "the data differ from the data the model was fitted to (fingerprint mismatch, API-3)"
        )
      val beta = fit.estimates.toArray
      val blocks = prepared.workingSet.blocks
      val maxEta = blocks
        .map((block: CoxBlock) => CoxKernel.maxEta(block, beta))(Encoders.scalaDouble)
        .collect()
        .max
      val limit = fit.options.blocks.maxGroupsOnDriver
      val totals = blocks
        .flatMap((block: CoxBlock) => CoxKernel.timeTotals(block, beta, maxEta))(
          Encoders.product[CoxTimeTotal]
        )
        .groupByKey((t: CoxTimeTotal) => t.time)(Encoders.scalaDouble)
        .mapGroups((time: Double, parts: Iterator[CoxTimeTotal]) =>
          CoxKernel.combineTimeTotals(time, parts)
        )(
          Encoders.product[CoxTimeTotal]
        )
        .limit(limit + 1)
        .collect()
        .sortBy(_.time)
      if (totals.length > limit)
        throw new IllegalArgumentException(
          s"more than $limit distinct entry and exit times; raise BlockOptions.maxGroupsOnDriver"
        )
      val (eventTimes, cumulative, riskSets) = MeasureKernels.national(
        totals.map(_.time),
        totals.map(_.exitSum),
        totals.map(_.entrySum),
        totals.map(_.events)
      )
      val rows = blocks.flatMap((block: CoxBlock) =>
        CoxKernel.providerMeasures(block, beta, maxEta, eventTimes, cumulative, riskSets)
      )(Encoders.product[CoxProviderRow])
      val keys = PlanFrames
        .placements(df.sparkSession, prepared.plan, prepared.strataKeyIsText)
        .select(
          col(PlanFrames.GroupIndexColumn).as("groupIndex"),
          col(Validation.GroupColumn).as(provider)
        )
      val table = rows.join(broadcast(keys), Seq("groupIndex"))
      val shown = providers.fold(table)(list => table.filter(col(provider).isin(list: _*)))
      val observedTotal = prepared.events.toDouble
      val results = kinds.distinct.map {
        case Standardization.Indirect =>
          Standardization.Indirect -> shown.select(
            col(provider),
            col("indirectRatio").as("indirect_ratio"),
            col("observed"),
            col("expected"),
            col("personTime").as("person_time")
          )
        case Standardization.Direct =>
          Standardization.Direct -> shown.select(
            col(provider),
            (col("directExpected") / lit(observedTotal)).as("direct_ratio"),
            lit(observedTotal).as("observed"),
            col("directExpected").as("expected"),
            lit(prepared.rows).as("n_pop")
          )
      }.toMap
      results.values.foreach { t =>
        t.persist(fit.options.blocks.resolvedStorageLevel)
        t.count()
      }
      val unexpected = rows.filter((r: CoxProviderRow) => r.expected == 0.0).count()
      if (unexpected > 0)
        log.warn(
          s"$unexpected providers have no expected events, so their indirect ratio is infinite or undefined"
        )
      results
    }
  }
}
