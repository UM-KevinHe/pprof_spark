package pprof.spark.engine.logistic

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{
  coalesce,
  count,
  lit,
  shiftrightunsigned,
  sum,
  when,
  xxhash64
}
import org.apache.spark.sql.types.{DoubleType, NumericType, StringType}
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.data.{InputProblem, InvalidInputException, Validation}
import pprof.spark.engine.layout.GroupKey
import pprof.spark.engine.metadata.SoftwareInfo

/** Column roles (docs/spec/logistic/fe-random-cluster.md §2): `outcome` is the likelihood's outcome in [0, 1];
  * `observed`, when given, is the outcome the tests count.
  */
final case class LogisticFERandomClusterSpec(
    outcome: String,
    features: Seq[String],
    provider: String,
    cluster: String,
    observed: Option[String] = None,
    rowId: Option[String] = None
)

/** Where β, σ and the start of γ come from (§2): given, or a `LogisticFE` fit and a three-stage stage-2 fit. */
sealed trait LogisticFERandomClusterSource
object LogisticFERandomClusterSource {
  final case class Given(beta: Seq[Double], sigma: Double, start: Seq[Double])
      extends LogisticFERandomClusterSource
  final case class Stages(stage1: LogisticFit, stage2: ThreeStageStage2)
      extends LogisticFERandomClusterSource
}

/** Options (§3): pprof_py's defaults; `he2013` is slice 4c-2's (OI-94). */
final case class LogisticFERandomClusterOptions(
    nNodes: Int = 20,
    maxIter: Int = 10000,
    tol: Double = 1e-5,
    bound: Double = 10.0,
    boundMode: String = "relative",
    estimator: String = "marginal",
    blocks: BlockOptions = BlockOptions(),
    path: String = "auto",
    convergenceCriterion: String = "max_delta_gamma"
) {
  require(
    Seq("marginal", "he2013").contains(estimator),
    s"estimator must be marginal or he2013, got $estimator"
  )
  require(
    Seq("max_delta_gamma", "relative").contains(convergenceCriterion),
    s"convergenceCriterion must be max_delta_gamma or relative, got $convergenceCriterion"
  )
  def stage3: ThreeStageStage3Options =
    ThreeStageStage3Options(nNodes, maxIter, tol, bound, boundMode, blocks, path)
}

/** A fit (§4): stage 3's result on the model's records, with β, σ, the records prepared as stage 3 reads them and
  * their cells, for the inference of §5.
  */
final case class LogisticFERandomClusterFit(
    stage3: ThreeStageStage3,
    beta: Vector[Double],
    sigma: Double,
    records: DataFrame,
    cells: ThreeStageCells,
    stage1: Option[LogisticFit],
    spec: LogisticFERandomClusterSpec,
    options: LogisticFERandomClusterOptions,
    warnings: Vector[String] = Vector.empty,
    rows: Long = 0L,
    fingerprint: Long = 0L,
    software: Option[SoftwareInfo] = None
) {
  def gamma: Array[Double] = stage3.gamma
  def providers: Vector[GroupKey] = stage3.providers
  private[logistic] def threeStageSpec: ThreeStageSpec =
    ThreeStageSpec(
      LogisticFERandomCluster.ObservedColumn,
      spec.features,
      spec.provider,
      spec.cluster,
      spec.rowId
    )
}

/** pprof_py's `LogisticFERandomClusterModel` (spec 4c-1) over the three-stage model's stage 3 and its inference. */
object LogisticFERandomCluster {

  private[logistic] val ObservedColumn = "__pprof_observed"

  def fit(
      df: DataFrame,
      spec: LogisticFERandomClusterSpec,
      source: LogisticFERandomClusterSource,
      options: LogisticFERandomClusterOptions = LogisticFERandomClusterOptions()
  ): LogisticFERandomClusterFit = {
    val (beta, sigma, stage1) = source match {
      case LogisticFERandomClusterSource.Given(b, s, _) =>
        require(
          b.length == spec.features.size,
          s"beta has ${b.length} values for ${spec.features.size} features"
        )
        require(s >= 0.0 && !s.isInfinite, s"sigma must be finite and non-negative, got $s")
        (b.toVector, s, None)
      case LogisticFERandomClusterSource.Stages(s1, s2) =>
        val names = s1.spec.features
        val unknown = spec.features.filterNot(names.contains)
        require(
          unknown.isEmpty && names.size == spec.features.size,
          s"stage1 was fit on features ${names.mkString(", ")}, not ${spec.features.mkString(", ")}"
        )
        val byName = names.zip(s1.estimates).toMap
        (spec.features.map(byName).toVector, s2.sigmaCluster, Some(s1))
    }
    val (records, cells, rows, fingerprint) = prepare(df, spec, beta, options)
    val start = source match {
      case LogisticFERandomClusterSource.Given(_, _, s) =>
        require(
          s.length == cells.providers.size,
          s"the start has ${s.length} values for ${cells.providers.size} providers"
        )
        s.toArray
      case LogisticFERandomClusterSource.Stages(_, s2) =>
        val byKey = s2.providers.map(k => GroupKey.value(k).toString).zip(s2.blupProviders).toMap
        val missing = cells.providers.map(k => GroupKey.value(k).toString).filterNot(byKey.contains)
        require(
          missing.isEmpty,
          s"stage2 has no effect for ${missing.size} provider(s), e.g. ${missing.take(5).mkString(", ")}"
        )
        cells.providers.map(k => byKey(GroupKey.value(k).toString) + s2.intercept).toArray
    }
    val (stage3, warnings) =
      if (options.estimator == "he2013")
        He2013.run(records, spec, cells, sigma, start, options) // slice 4c-2
      else (ThreeStage.stage3(cells, sigma, start, options.stage3), Vector.empty[String])
    LogisticFERandomClusterFit(
      stage3,
      beta,
      sigma,
      records,
      cells,
      stage1,
      spec,
      options,
      warnings,
      rows,
      fingerprint,
      Some(SoftwareInfo.capture(df.sparkSession))
    )
  }

  /** Validates `df` (§2), counts its rows and fingerprints them (spec 4c-3 §2) in one pass, and prepares the records
    * and cells as stage 3 reads them with `beta`; the fit and `LogisticFERandomClusterFitIO.attach` share it.
    */
  private[logistic] def prepare(
      df: DataFrame,
      spec: LogisticFERandomClusterSpec,
      beta: Vector[Double],
      options: LogisticFERandomClusterOptions
  ): (DataFrame, ThreeStageCells, Long, Long) = {
    val (rows, fingerprint) = validate(df, spec)
    val xbeta = spec.features.zip(beta).foldLeft(lit(0.0): Column) { case (acc, (name, b)) =>
      acc + Validation.column(name).cast(DoubleType) * lit(b)
    }
    val observed = Validation.column(spec.observed.getOrElse(spec.outcome)).cast(DoubleType)
    val records = df
      .withColumn("y_adj", Validation.column(spec.outcome).cast(DoubleType))
      .withColumn("stage1_offset", xbeta)
      .withColumn(ObservedColumn, observed)
      .persist(options.blocks.resolvedStorageLevel)
    val threeStage =
      ThreeStageSpec(ObservedColumn, spec.features, spec.provider, spec.cluster, spec.rowId)
    (records, ThreeStage.compress(records, threeStage, options.stage3), rows, fingerprint)
  }

  /** Missing or non-finite values fail with counts per column (X-038's rule); outcomes must lie in [0, 1]. */
  private def validate(df: DataFrame, spec: LogisticFERandomClusterSpec): (Long, Long) = {
    val types = df.schema.fields.map(f => f.name -> f.dataType).toMap
    val numeric = (spec.outcome +: spec.features) ++ spec.observed
    val structural = (numeric ++ Seq(spec.provider, spec.cluster)).distinct.flatMap { name =>
      types.get(name) match {
        case None => Some(InputProblem.MissingColumn(name))
        case Some(t) if numeric.contains(name) && !t.isInstanceOf[NumericType] =>
          Some(InputProblem.UnsupportedType(name, t.simpleString, "a numeric type"))
        case Some(t) if !numeric.contains(name) && !(t == StringType || Validation.isIntegral(t)) =>
          Some(InputProblem.UnsupportedType(name, t.simpleString, "an integral or string type"))
        case _ => None
      }
    }
    if (structural.nonEmpty) throw new InvalidInputException(structural)
    def countWhere(c: Column): Column = coalesce(sum(when(c, 1L).otherwise(0L)), lit(0L))
    val checks = numeric.distinct.flatMap { name =>
      val v = Validation.column(name)
      Seq(countWhere(v.isNull), countWhere(Validation.nonFinite(v, types(name))))
    } ++ Seq(spec.provider, spec.cluster).map(name => countWhere(Validation.column(name).isNull)) ++
      (spec.outcome +: spec.observed.toSeq).map { name =>
        val v = Validation.column(name).cast(DoubleType)
        countWhere(v < lit(0.0) || v > lit(1.0))
      } ++ {
        // the fingerprint (spec 4c-3 §2): a 64-bit hash of each row's model columns, summed in two 32-bit halves
        val hashed = ((Seq(
          spec.provider,
          spec.cluster,
          spec.outcome
        ) ++ spec.observed ++ spec.features ++ spec.rowId).distinct)
          .map(Validation.column)
        val h = xxhash64(hashed: _*)
        Seq(
          coalesce(sum(h.bitwiseAND(lit(0xffffffffL))), lit(0L)),
          coalesce(sum(shiftrightunsigned(h, 32)), lit(0L))
        )
      } :+ count(lit(1))
    val row = df.agg(checks.head, checks.tail: _*).collect().head
    var i = 0
    def next(): Long = { val value = row.getLong(i); i += 1; value }
    val problems = Vector.newBuilder[InputProblem]
    numeric.distinct.foreach { name =>
      val (nulls, nonFinite) = (next(), next())
      if (nulls > 0 || nonFinite > 0) problems += InputProblem.InvalidValues(name, nulls, nonFinite)
    }
    Seq(spec.provider, spec.cluster).foreach { name =>
      val nulls = next()
      if (nulls > 0) problems += InputProblem.InvalidValues(name, nulls, 0L)
    }
    (spec.outcome +: spec.observed.toSeq).foreach { name =>
      val outside = next()
      if (outside > 0) problems += InputProblem.OutsideUnitInterval(name, outside)
    }
    val (low, high) = (next(), next())
    val rows = next()
    if (rows == 0L) problems += InputProblem.EmptyInput
    val found = problems.result()
    if (found.nonEmpty) throw new InvalidInputException(found)
    (rows, low + (high << 32))
  }

  /** `summary` (§5): stage 1's Wald tests of β, which must be the β the fit used (10⁻¹⁰ relative, 10⁻¹² absolute). */
  def summary(
      fit: LogisticFERandomClusterFit,
      stage1: Option[LogisticFit] = None,
      level: Double = 0.95,
      nullValue: Double = 0.0,
      alternative: String = "two_sided"
  ): Vector[LogisticTest] =
    summaryOf(fit.spec.features, fit.beta, stage1.orElse(fit.stage1), level, nullValue, alternative)

  /** `summary` from the β a fit used and its features, as a saved fit holds them (spec 4c-3 §6). */
  def summaryOf(
      features: Seq[String],
      beta: Seq[Double],
      stage1: Option[LogisticFit],
      level: Double = 0.95,
      nullValue: Double = 0.0,
      alternative: String = "two_sided"
  ): Vector[LogisticTest] = {
    val s1 = stage1.getOrElse(
      throw new IllegalArgumentException(
        "summary needs the stage 1 fit whose beta the model used (stage1)"
      )
    )
    val byName = s1.spec.features.zip(s1.estimates).toMap
    val same = features.zip(beta).forall { case (name, b) =>
      byName.get(name).exists(v => math.abs(v - b) <= 1e-12 + 1e-10 * math.abs(b))
    }
    require(same, "stage1's beta differs from the beta this model was fit with")
    LogisticFE.waldTests(s1, nullValue, alternative, level)
  }

  /** `test` (§5): stage 3's tests, counting the observed outcome. */
  def test(
      fit: LogisticFERandomClusterFit,
      method: String = "exact",
      reference: EffectReference = EffectReference.Median,
      alternative: String = "two_sided",
      level: Double = 0.95,
      critical: Option[Double] = None,
      providers: Option[Seq[Any]] = None,
      nResample: Int = 10000,
      seed: Long = 0L
  ): DataFrame =
    ThreeStagePipeline.test(
      fit.records,
      fit.threeStageSpec,
      fit.stage3,
      method,
      reference,
      alternative,
      level,
      critical,
      providers,
      nResample,
      seed
    )

  /** `calculate_standardized_measures` (§5). */
  def measures(
      fit: LogisticFERandomClusterFit,
      kinds: Seq[String] = Seq("indirect"),
      reference: EffectReference = EffectReference.Median
  ): Map[String, DataFrame] =
    ThreeStagePipeline.measures(
      fit.records,
      fit.cells,
      fit.threeStageSpec,
      fit.stage3,
      kinds,
      reference
    )

  /** `calculate_confidence_intervals` (§5): option `gamma`, or `SM`. */
  def intervals(
      fit: LogisticFERandomClusterFit,
      option: String = "SM",
      kinds: Seq[String] = Seq("indirect"),
      measure: Seq[String] = Seq("rate", "ratio"),
      alternative: String = "two_sided",
      level: Double = 0.95,
      method: String = "exact",
      reference: EffectReference = EffectReference.Median
  ): Map[String, DataFrame] =
    ThreeStagePipeline.intervals(
      fit.records,
      fit.cells,
      fit.threeStageSpec,
      fit.stage3,
      option,
      kinds,
      measure,
      alternative,
      level,
      method,
      reference
    )

  /** The records with `fitted`: expit(γ̂ⱼ + E[αₕ | y] + xᵢᵀβ) (§4). */
  def fitted(fit: LogisticFERandomClusterFit): DataFrame =
    ThreeStage.fitted(fit.records, fit.threeStageSpec, fit.stage3)
}
