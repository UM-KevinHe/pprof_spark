package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.functions.{count, lit, sum}
import org.apache.spark.sql.types.{
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import pprof.spark.engine.data.Validation
import pprof.spark.engine.layout.GroupKey
import pprof.spark.numerics.{
  ClusteredPoissonBinomial,
  NeumaierVector,
  Normal,
  PoissonBinomial,
  Serbin,
  TaylorBins
}
import pprof.spark.numerics.kernels.ThreeStageKernel

/** The pipeline's options: preparation and stage 1, stage 2, stage 3. */
final case class ThreeStageFitOptions(
    preparation: ThreeStageOptions = ThreeStageOptions(),
    stage2: ThreeStageStage2Options = ThreeStageStage2Options(),
    stage3: ThreeStageStage3Options = ThreeStageStage3Options()
)

/** The three-stage model fitted (docs/spec/logistic/three-stage-pipeline.md §1). */
final case class ThreeStageFit(
    preparation: ThreeStagePreparation,
    cells: ThreeStageCells,
    stage2: ThreeStageStage2,
    stage3: ThreeStageStage3,
    spec: ThreeStageSpec,
    options: ThreeStageFitOptions
)

/** One record for stage 3's tests: provider and cluster indices, raw outcome, offset. */
final case class ThreeStageTestRecord(provider: Int, cluster: Int, y: Double, offset: Double)

/** The three-stage pipeline and stage 3's tests, measures and intervals (Phase 2f-4a). */
object ThreeStagePipeline {

  val Methods: Seq[String] = Seq("exact", "poibin_exact", "resampling")

  def fit(
      df: DataFrame,
      spec: ThreeStageSpec,
      options: ThreeStageFitOptions = ThreeStageFitOptions()
  ): ThreeStageFit = {
    val prepared = ThreeStage.prepare(df, spec, options.preparation)
    val cells = ThreeStage.compress(prepared.data, spec, options.stage3)
    val stage2 = ThreeStage.stage2(cells, options.stage2)
    val stage3 = ThreeStage.stage3(cells, stage2.sigmaCluster, stage2.start, options.stage3)
    ThreeStageFit(prepared, cells, stage2, stage3, spec, options)
  }

  private def sigmoid(x: Double): Double = 1.0 / (1.0 + StrictMath.exp(-x))

  private def index(keys: Vector[GroupKey]): Map[String, Int] =
    keys.map(k => GroupKey.value(k).toString).zipWithIndex.toMap

  /** Raw events and records per provider (in provider key order). */
  private def counts(
      records: DataFrame,
      spec: ThreeStageSpec,
      providers: Vector[GroupKey]
  ): (Array[Double], Array[Long]) = {
    val at = index(providers)
    val events = new Array[Double](providers.size)
    val sizes = new Array[Long](providers.size)
    records
      .groupBy(Validation.column(spec.provider).cast(StringType).as("p"))
      .agg(
        sum(Validation.column(spec.outcome).cast(LongType)).as("events"),
        count(lit(1)).as("records")
      )
      .collect()
      .foreach { r =>
        val j = at(r.getString(0))
        events(j) = r.getLong(1).toDouble
        sizes(j) = r.getLong(2)
      }
    (events, sizes)
  }

  /** γ₀ for the tests: pprof_py's `reference_effect` (median; record-weighted mean; a number). */
  private def testReference(
      stage3: ThreeStageStage3,
      sizes: Array[Long],
      reference: EffectReference
  ): Double = reference match {
    case EffectReference.Median => Serbin.median(stage3.gamma)
    case EffectReference.Mean   =>
      stage3.gamma.indices.map(j => sizes(j) * stage3.gamma(j)).sum / sizes.sum.toDouble
    case EffectReference.Value(v) => v
  }

  /** γ₀ for the measures: median, the unweighted mean (pprof_py's measures), or a number (spec §3). */
  private def measureReference(stage3: ThreeStageStage3, reference: EffectReference): Double =
    reference match {
      case EffectReference.Median   => Serbin.median(stage3.gamma)
      case EffectReference.Mean     => stage3.gamma.sum / stage3.gamma.length
      case EffectReference.Value(v) => v
    }

  /** Stage 3's provider tests (spec §2): `exact`, `poibin_exact` or `resampling`, with slice 2c's table. */
  def test(
      records: DataFrame,
      spec: ThreeStageSpec,
      stage3: ThreeStageStage3,
      method: String = "exact",
      reference: EffectReference = EffectReference.Median,
      alternative: String = "two_sided",
      level: Double = 0.95,
      critical: Option[Double] = None,
      providers: Option[Seq[Any]] = None,
      nResample: Int = 10000,
      seed: Long = 0L,
      offsetColumn: String = "stage1_offset"
  ): DataFrame = {
    require(
      Methods.contains(method),
      s"method must be one of ${Methods.mkString(", ")}, got $method"
    )
    require(
      LogisticProviderTests.Alternatives.contains(alternative),
      s"unknown alternative $alternative"
    )
    require(level > 0.0 && level < 1.0, s"level must lie strictly between 0 and 1, got $level")
    val spark = records.sparkSession
    val (_, sizes) = counts(records, spec, stage3.providers)
    val g0 = testReference(stage3, sizes, reference)
    val alpha = 1.0 - level
    val c = critical.getOrElse(
      Normal.upperQuantile(if (alternative == "two_sided") alpha / 2.0 else alpha)
    )
    val wanted = providers.map(_.map(_.toString).toSet)
    val tested =
      stage3.providers.map(k => wanted.forall(_.contains(GroupKey.value(k).toString))).toArray
    val pIndex = index(stage3.providers)
    val hIndex = index(stage3.clusters)
    val gamma = stage3.gamma.clone()
    val mean = stage3.alphaMean.clone()
    val variance = stage3.alphaVar.clone()
    val rows = records
      .select(
        Validation.column(spec.provider).cast(StringType),
        Validation.column(spec.cluster).cast(StringType),
        Validation.column(spec.outcome).cast(DoubleType),
        Validation.column(offsetColumn).cast(DoubleType)
      )
      .as(
        Encoders.tuple(Encoders.STRING, Encoders.STRING, Encoders.scalaDouble, Encoders.scalaDouble)
      )
      .map { case (p, h, y, o) => ThreeStageTestRecord(pIndex(p), hIndex(h), y, o) }(
        Encoders.product[ThreeStageTestRecord]
      )
      .filter((r: ThreeStageTestRecord) => tested(r.provider))
      .groupByKey((r: ThreeStageTestRecord) => r.provider)(Encoders.scalaInt)
      .mapGroups { (j: Int, group: Iterator[ThreeStageTestRecord]) =>
        val sorted = group.toArray.sortBy(r => (r.cluster, r.offset, r.y))
        val offsets = sorted.map(_.offset)
        val clusters = sorted.map(_.cluster)
        val observed = sorted.map(_.y).sum.toInt
        def zOf(t: PoissonBinomial.Tails, floor: Double) = PoissonBinomial.z(t, alternative, floor)
        def zAt(g: Double): Double = method match {
          case "exact" =>
            zOf(
              ClusteredPoissonBinomial.tails(
                ClusteredPoissonBinomial.clusteredPmf(offsets.map(_ + g), clusters, mean, variance),
                observed
              ),
              PoissonBinomial.TailFloor
            )
          case _ =>
            val p = sorted.indices.map(i => sigmoid(g + mean(clusters(i)) + offsets(i))).toArray
            zOf(
              PoissonBinomial.tails(p, Array.fill(p.length)(1), observed),
              PoissonBinomial.TailFloor
            )
        }
        val (z, lower, upper) =
          if (method == "resampling") {
            val floor = 0.5 / nResample
            val eta = offsets.map(_ + g0)
            val t = ClusteredPoissonBinomial.resampledTails(
              observed,
              eta,
              clusters.map(mean),
              clusters.map(variance),
              nResample,
              seed,
              j.toLong
            )
            val atFloor = alternative match {
              case "two_sided" => math.min(t.upperMid, t.lowerMid) <= floor
              case "greater"   => t.atLeast <= floor
              case _           => t.atMost <= floor
            }
            val zz =
              if (!atFloor) zOf(t, floor)
              else {
                val p = ClusteredPoissonBinomial.integratedProbabilities(
                  sorted.indices.map(i => g0 + mean(clusters(i)) + offsets(i)).toArray,
                  clusters.map(variance)
                )
                zOf(
                  PoissonBinomial.tails(p, Array.fill(p.length)(1), observed),
                  PoissonBinomial.TailFloor
                )
              }
            (zz, Double.NaN, Double.NaN)
          } else {
            val lo =
              if (alternative == "less") Double.NegativeInfinity
              else PoissonBinomial.invertDecreasing(zAt, gamma(j), c)
            val hi =
              if (alternative == "greater") Double.PositiveInfinity
              else PoissonBinomial.invertDecreasing(zAt, gamma(j), -c)
            (zAt(g0), lo, hi)
          }
        val (p, flag) = LogisticProviderTests.decide(z, alternative, c)
        val expected = sorted.indices.map(i => sigmoid(g0 + mean(clusters(i)) + offsets(i))).sum
        LogisticProviderTestRow(
          j,
          gamma(j),
          Double.NaN,
          z,
          p,
          flag,
          lower,
          upper,
          observed.toDouble,
          expected,
          sorted.length.toDouble,
          sorted.length.toLong
        )
      }(Encoders.product[LogisticProviderTestRow])
      .collect()
      .sortBy(_.groupIndex)
    val text = stage3.providers.headOption.exists(_.isInstanceOf[GroupKey.Text])
    val schema = StructType(
      Seq(StructField(spec.provider, if (text) StringType else LongType, nullable = false)) ++
        Seq(
          "estimate",
          "se",
          "null_value",
          "transformed",
          "se_transformed",
          "null_transformed",
          "z_raw",
          "null_mean",
          "null_sd"
        )
          .map(StructField(_, DoubleType, nullable = false)) ++
        Seq(
          StructField("null_group", IntegerType, nullable = true),
          StructField("z_adjusted", DoubleType, nullable = false),
          StructField("p_value", DoubleType, nullable = false),
          StructField("flag", IntegerType, nullable = false),
          StructField("ci_lower", DoubleType, nullable = false),
          StructField("ci_upper", DoubleType, nullable = false),
          StructField("observed", DoubleType, nullable = false),
          StructField("expected", DoubleType, nullable = false),
          StructField("trials", DoubleType, nullable = false),
          StructField("records", LongType, nullable = false)
        )
    )
    spark.createDataFrame(
      rows.toSeq.map { r =>
        Row(
          GroupKey.value(stage3.providers(r.groupIndex)),
          r.estimate,
          r.se,
          g0,
          Double.NaN,
          Double.NaN,
          g0,
          r.zRaw,
          0.0,
          1.0,
          null,
          r.zRaw,
          r.pValue,
          r.flag,
          r.lower,
          r.upper,
          r.observed,
          r.expected,
          r.trials,
          r.records
        )
      }.asJava,
      schema
    )
  }

  /** Each cluster's cells pooled into one set of offset-bin moments (the clusters share E_post[aₕ]). */
  private def pooled(cells: ThreeStageCells): Array[(Array[Long], Array[Array[Double]])] = {
    val k = TaylorBins.Order + 1
    val byCluster =
      Array.fill(cells.clusters.size)(scala.collection.mutable.TreeMap.empty[Long, NeumaierVector])
    cells.cells.foreach { c =>
      c.bins.indices.foreach { b =>
        byCluster(c.cluster)
          .getOrElseUpdate(c.bins(b), new NeumaierVector(k))
          .add(c.moments.slice(b * k, b * k + k))
      }
    }
    byCluster.map(m => (m.keys.toArray, m.values.map(_.values).toArray))
  }

  private def own(
      cells: ThreeStageCells,
      stage3: ThreeStageStage3,
      g: Array[Double]
  ): Array[Double] = {
    val out = new Array[Double](stage3.providers.size)
    cells.cells.foreach { c =>
      out(
        c.provider
      ) += (if (g(c.provider).isNaN) Double.NaN
            else ThreeStageKernel.sums(c, g(c.provider) + stage3.alphaMean(c.cluster))._1)
    }
    out
  }

  private def everyone(
      pools: Array[(Array[Long], Array[Array[Double]])],
      stage3: ThreeStageStage3,
      g: Double
  ): Double =
    if (g.isNaN) Double.NaN
    else
      pools.indices
        .map(h => TaylorBins.sums(g + stage3.alphaMean(h), pools(h)._1, pools(h)._2)._1)
        .sum

  /** pprof_py's `calculate_standardized_measures` for stage 3 (spec §3): tables keyed `indirect` and `direct`. */
  def measures(
      records: DataFrame,
      cells: ThreeStageCells,
      spec: ThreeStageSpec,
      stage3: ThreeStageStage3,
      kinds: Seq[String] = Seq("indirect", "direct"),
      reference: EffectReference = EffectReference.Median
  ): Map[String, DataFrame] = {
    require(
      kinds.nonEmpty && kinds.forall(Set("indirect", "direct")),
      "kinds must be indirect and/or direct"
    )
    val spark = records.sparkSession
    val (observed, sizes) = counts(records, spec, stage3.providers)
    val n = sizes.sum.toDouble
    val total = observed.sum
    val m = stage3.providers.size
    val g0 = measureReference(stage3, reference)
    val text = stage3.providers.headOption.exists(_.isInstanceOf[GroupKey.Text])
    def frame(columns: Seq[String], values: Int => Seq[Double]): DataFrame =
      spark.createDataFrame(
        (0 until m).map(j => Row.fromSeq(GroupKey.value(stage3.providers(j)) +: values(j))).asJava,
        StructType(
          StructField(spec.provider, if (text) StringType else LongType, nullable = false) +:
            columns.map(StructField(_, DoubleType, nullable = false))
        )
      )
    kinds.map {
      case "indirect" =>
        val expected = own(cells, stage3, Array.fill(m)(g0))
        val rate = total / n * 100.0
        "indirect" -> frame(
          Seq("indirect_ratio", "indirect_rate", "observed", "expected"),
          j => {
            val ratio = if (expected(j) > 0.0) observed(j) / expected(j) else Double.NaN
            Seq(ratio, math.min(math.max(ratio * rate, 0.0), 100.0), observed(j), expected(j))
          }
        )
      case _ =>
        val pools = pooled(cells)
        val predicted = stage3.gamma.map(g => everyone(pools, stage3, g))
        "direct" -> frame(
          Seq("direct_ratio", "direct_rate", "observed", "expected"),
          j => {
            val ratio = if (total > 0.0) predicted(j) / total else Double.NaN
            Seq(
              ratio,
              math.min(math.max(predicted(j) / n * 100.0, 0.0), 100.0),
              total,
              predicted(j)
            )
          }
        )
    }.toMap
  }

  /** pprof_py's `calculate_confidence_intervals` for stage 3 (spec §3): `gamma`, or `SM` for the measures. */
  def intervals(
      records: DataFrame,
      cells: ThreeStageCells,
      spec: ThreeStageSpec,
      stage3: ThreeStageStage3,
      option: String = "SM",
      kinds: Seq[String] = Seq("indirect"),
      measure: Seq[String] = Seq("rate", "ratio"),
      alternative: String = "two_sided",
      level: Double = 0.95,
      method: String = "exact",
      reference: EffectReference = EffectReference.Median
  ): Map[String, DataFrame] = {
    require(option == "gamma" || option == "SM", s"option must be gamma or SM, got $option")
    require(
      method == "exact" || method == "poibin_exact",
      "intervals need the exact or poibin_exact test"
    )
    val tested = test(records, spec, stage3, method, reference, alternative, level)
      .orderBy(spec.provider)
      .collect()
    val lower = tested.map(_.getAs[Double]("ci_lower"))
    val upper = tested.map(_.getAs[Double]("ci_upper"))
    val spark = records.sparkSession
    val text = stage3.providers.headOption.exists(_.isInstanceOf[GroupKey.Text])
    val keyField = StructField(spec.provider, if (text) StringType else LongType, nullable = false)
    if (option == "gamma") {
      val m = stage3.providers.size
      Map(
        "gamma_ci" -> spark.createDataFrame(
          (0 until m)
            .map(j => Row(GroupKey.value(stage3.providers(j)), stage3.gamma(j), lower(j), upper(j)))
            .asJava,
          StructType(
            keyField +: Seq("gamma", "gamma_lower", "gamma_upper").map(
              StructField(_, DoubleType, nullable = false)
            )
          )
        )
      )
    } else {
      val tables = measures(records, cells, spec, stage3, kinds, reference)
      val (observed, sizes) = counts(records, spec, stage3.providers)
      val rate = observed.sum / sizes.sum.toDouble * 100.0
      val total = observed.sum
      val pools = pooled(cells)
      kinds.flatMap { kind =>
        val base = tables(kind).orderBy(spec.provider)
        val (rl, ru) =
          if (kind == "indirect") {
            val expected = base.select("expected").collect().map(_.getDouble(0))
            def ratio(g: Array[Double]) = own(cells, stage3, g).indices
              .map(j =>
                if (expected(j) > 0.0) own(cells, stage3, g)(j) / expected(j) else Double.NaN
              )
              .toArray
            (ratio(lower), ratio(upper))
          } else {
            def ratio(g: Array[Double]) =
              g.map(v => if (total > 0.0) everyone(pools, stage3, v) / total else Double.NaN)
            (ratio(lower), ratio(upper))
          }
        def withColumns(
            df: DataFrame,
            names: (String, String),
            values: (Array[Double], Array[Double])
        ): DataFrame = {
          val rows = df.collect().zipWithIndex.map { case (r, j) =>
            Row.fromSeq(r.toSeq :+ values._1(j) :+ values._2(j))
          }
          spark.createDataFrame(
            rows.toSeq.asJava,
            StructType(
              df.schema.fields ++ Seq(
                StructField(names._1, DoubleType, nullable = true),
                StructField(names._2, DoubleType, nullable = true)
              )
            )
          )
        }
        def clip(v: Double) = if (v.isNaN) v else math.min(math.max(v * rate, 0.0), 100.0)
        measure.map {
          case "ratio" =>
            s"${kind}_ratio" -> withColumns(base, ("ci_ratio_lower", "ci_ratio_upper"), (rl, ru))
          case _ =>
            s"${kind}_rate" -> withColumns(
              base,
              ("ci_rate_lower", "ci_rate_upper"),
              (rl.map(clip), ru.map(clip))
            )
        }
      }.toMap
    }
  }
}
