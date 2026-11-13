package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions.{
  coalesce,
  count,
  lit,
  shiftrightunsigned,
  sum,
  when,
  xxhash64
}
import org.apache.spark.sql.types.{
  DoubleType,
  IntegerType,
  LongType,
  NumericType,
  StringType,
  StructField,
  StructType
}
import pprof.spark.engine.backend.{BlockOptions, DriverGuards}
import pprof.spark.engine.data.{InputProblem, InvalidInputException, Validation}
import pprof.spark.engine.layout.GroupKey
import pprof.spark.engine.metadata.SoftwareInfo
import pprof.spark.numerics.{ClusteredPoissonBinomial, Normal, PoissonBinomial}
import pprof.spark.numerics.kernels.{LogisticREClusters => Kernel}

/** Column roles of the logistic model with cluster factors (docs/spec/logistic/random-effect-clusters.md). */
final case class LogisticREClusterSpec(
    outcome: String,
    features: Seq[String],
    provider: String,
    clusters: Seq[String],
    offset: Option[String] = None,
    rowId: Option[String] = None
) {
  require(
    clusters.nonEmpty,
    "a fit with cluster factors needs at least one cluster column; 4b's LogisticRE fits without"
  )
  def factors: Seq[String] = provider +: clusters
}

final case class LogisticREClusterOptions(
    includeIntercept: Boolean = true,
    sigmaUpper: Double = Double.PositiveInfinity,
    maxIterations: Int = 500,
    blocks: BlockOptions = BlockOptions()
) {
  require(sigmaUpper > 0.0, s"sigmaUpper must be positive, got $sigmaUpper")
}

/** The fit (§5): β with Var(β̂), σ̂ by factor, each factor's table (key, `records`, `blup`, `variance`, `se`), the
  * deviances and the rows on the driver for the inference (§6).
  */
final case class LogisticREClusterFit(
    coefficients: Vector[LogisticRECoefficient],
    covariance: Vector[Double],
    factors: Vector[String],
    sigma: Vector[Double],
    effects: Map[String, DataFrame],
    deviance: Double,
    loglik: Double,
    aic: Double,
    bic: Double,
    stage1Deviance: Double,
    converged: Boolean,
    boundary: Vector[String],
    warnings: Vector[String],
    spec: LogisticREClusterSpec,
    options: LogisticREClusterOptions,
    software: SoftwareInfo,
    fingerprint: Long,
    private[logistic] val rows: Kernel.Rows,
    private[logistic] val keys: Vector[Vector[GroupKey]],
    private[logistic] val kernel: Kernel.Fit
) {
  def estimates: Vector[Double] = coefficients.map(_.estimate)
  def standardErrors: Vector[Double] = coefficients.map(_.standardError)
}

/** pprof_py's `LogisticRandomEffectModel(...).fit(..., cluster_vars=...)` (slice 4d-2) with its rows on the driver
  * (the driver path of §3; larger fits fail, naming OI-102).
  */
object LogisticREClusters {

  val InterceptName: String = "(Intercept)"
  val Methods: Seq[String] = Seq("wald", "poibin_exact", "resampling", "exact")

  private def keyColumn(df: DataFrame, name: String): Column = {
    val c = Validation.column(name)
    if (df.schema(name).dataType == StringType) c else c.cast(LongType)
  }

  private def validate(df: DataFrame, spec: LogisticREClusterSpec): (Long, Long) = {
    val types = df.schema.fields.map(f => f.name -> f.dataType).toMap
    val numeric = (spec.outcome +: spec.features) ++ spec.offset
    val keys = spec.factors
    val problems = Vector.newBuilder[InputProblem]
    (numeric ++ keys ++ spec.rowId).distinct.foreach { name =>
      types.get(name) match {
        case None => problems += InputProblem.MissingColumn(name)
        case Some(t) if numeric.contains(name) && !t.isInstanceOf[NumericType] =>
          problems += InputProblem.UnsupportedType(name, t.simpleString, "a numeric type")
        case Some(t) if keys.contains(name) && !(t == StringType || Validation.isIntegral(t)) =>
          problems += InputProblem.UnsupportedType(
            name,
            t.simpleString,
            "an integral or string type"
          )
        case _ => ()
      }
    }
    val first = problems.result()
    if (first.nonEmpty) throw new InvalidInputException(first)
    require(
      keys.distinct.size == keys.size,
      s"the provider and cluster columns must differ: ${keys.mkString(", ")}"
    )
    def countWhere(c: Column): Column = coalesce(sum(when(c, 1L).otherwise(0L)), lit(0L))
    val hashed = xxhash64((keys ++ numeric ++ spec.rowId).distinct.map(Validation.column): _*)
    val y = Validation.column(spec.outcome).cast(DoubleType)
    val checks: Seq[Column] = numeric.distinct.flatMap { name =>
      val v = Validation.column(name)
      Seq(countWhere(v.isNull), countWhere(Validation.nonFinite(v, types(name))))
    } ++ keys.map(name => countWhere(Validation.column(name).isNull)) ++ Seq(
      countWhere(y < lit(0.0) || y > lit(1.0)),
      coalesce(sum(hashed.bitwiseAND(lit(0xffffffffL))), lit(0L)),
      coalesce(sum(shiftrightunsigned(hashed, 32)), lit(0L)),
      count(lit(1))
    )
    val row = df.agg(checks.head, checks.tail: _*).collect().head
    var i = 0
    def next(): Long = { val v = row.getLong(i); i += 1; v }
    numeric.distinct.foreach { name =>
      val (nulls, nonFinite) = (next(), next())
      if (nulls > 0 || nonFinite > 0) problems += InputProblem.InvalidValues(name, nulls, nonFinite)
    }
    keys.foreach(name => {
      val nulls = next(); if (nulls > 0) problems += InputProblem.InvalidValues(name, nulls, 0L)
    })
    val outside = next()
    if (outside > 0) problems += InputProblem.OutsideUnitInterval(spec.outcome, outside)
    val (low, high) = (next(), next())
    val rows = next()
    if (rows == 0L) problems += InputProblem.EmptyInput
    val found = problems.result()
    if (found.nonEmpty) throw new InvalidInputException(found)
    (rows, low + (high << 32))
  }

  /** The rows on the driver, in canonical order (the row identifier, else the levels and values), as kernel rows. */
  private def collectRows(
      df: DataFrame,
      spec: LogisticREClusterSpec,
      options: LogisticREClusterOptions,
      rows: Long
  ): (Kernel.Rows, Vector[Vector[GroupKey]]) = {
    val p = spec.features.size + (if (options.includeIntercept) 1 else 0)
    val k = spec.factors.size
    DriverGuards.requireWithinBudget(
      s"$rows rows on the driver for the cluster-factor fit (OI-102)",
      8L * rows * (p + k + 3L) * 2L,
      options.blocks
    )
    val columns = spec.factors.map(f => keyColumn(df, f)) ++ Seq(
      spec.rowId.fold(lit(0L))(r => Validation.column(r).cast(LongType)),
      Validation.column(spec.outcome).cast(DoubleType),
      spec.offset.fold(lit(0.0))(o => Validation.column(o).cast(DoubleType))
    ) ++ spec.features.map(f => Validation.column(f).cast(DoubleType))
    val raw = df.select(columns: _*).collect()
    val keys = spec.factors.indices.map { f =>
      val values = raw.map(_.get(f)).distinct.toVector
      if (values.forall(_.isInstanceOf[String]))
        values.map(_.asInstanceOf[String]).sorted.map(GroupKey.of)
      else values.map(_.asInstanceOf[Long]).sorted.map(GroupKey.of)
    }.toVector
    val index = keys.map(_.zipWithIndex.toMap)
    def bits(v: Double): Long = java.lang.Double.doubleToRawLongBits(v)
    val ordered = raw
      .map { r =>
        val levels = (0 until k).map(f => index(f)(GroupKey.of(r.get(f))))
        val values = (k + 1 until r.length).map(z => r.getDouble(z))
        (levels, r.getLong(k), values, r)
      }
      .sortWith { (a, b) =>
        if (spec.rowId.isDefined) a._2 < b._2
        else {
          val c = a._1.zip(b._1).find { case (x, y) => x != y }
          c.map { case (x, y) => x < y }
            .getOrElse(a._3.map(bits).zip(b._3.map(bits)).find { case (x, y) => x != y }.exists {
              case (x, y) => x < y
            })
        }
      }
    val n = ordered.length
    val x = new Array[Double](n * p)
    ordered.indices.foreach { i =>
      val v = ordered(i)._3
      var j = 0
      if (options.includeIntercept) { x(i * p) = 1.0; j = 1 }
      spec.features.indices.foreach(f => x(i * p + j + f) = v(2 + f))
    }
    val kernelRows = Kernel.Rows(
      n,
      p,
      x,
      ordered.map(_._3(0)),
      ordered.map(_._3(1)),
      Array.tabulate(k)(f => ordered.map(_._1(f))),
      keys.map(_.size).toArray
    )
    (kernelRows, keys)
  }

  private def assemble(
      df: DataFrame,
      spec: LogisticREClusterSpec,
      options: LogisticREClusterOptions,
      fingerprint: Long,
      rows: Kernel.Rows,
      keys: Vector[Vector[GroupKey]],
      f: Kernel.Fit
  ): LogisticREClusterFit = {
    val p = rows.p
    val names = (if (options.includeIntercept) Seq(InterceptName) else Seq.empty) ++ spec.features
    val coefficients = names.indices.map { j =>
      LogisticRECoefficient(
        names(j),
        f.beta(j),
        StrictMath.sqrt(math.max(f.covariance(j * p + j), 0.0))
      )
    }.toVector
    val spark = df.sparkSession
    val offsets = rows.offsets
    val effects = spec.factors.indices.map { k =>
      val counts = new Array[Long](rows.levels(k))
      rows.index(k).foreach(l => counts(l) += 1)
      val text = keys(k).headOption.exists(key => GroupKey.value(key).isInstanceOf[String])
      val schema = StructType(
        Seq(
          StructField(spec.factors(k), if (text) StringType else LongType, nullable = false),
          StructField("records", LongType, nullable = false),
          StructField("blup", DoubleType, nullable = false),
          StructField("variance", DoubleType, nullable = false),
          StructField("se", DoubleType, nullable = false)
        )
      )
      val table = keys(k).indices.map { l =>
        val z = offsets(k) + l
        Row(
          GroupKey.value(keys(k)(l)),
          counts(l),
          f.blups(z),
          f.posteriorVariance(z),
          StrictMath.sqrt(math.max(f.posteriorVariance(z), 0.0))
        )
      }
      spec.factors(k) -> spark.createDataFrame(table.asJava, schema)
    }.toMap
    val parameters = p + spec.factors.size
    val boundary = spec.factors.indices.filter(k => f.sigma(k) == 0.0).map(spec.factors).toVector
    val warnings = boundary.map(name =>
      s"the SD of $name's random intercept is estimated at 0 (a boundary fit)"
    ) ++
      (if (f.converged) Vector.empty
       else Vector(s"the search did not converge (projected gradient ${f.projectedGradient})"))
    LogisticREClusterFit(
      coefficients,
      f.covariance.toVector,
      spec.factors.toVector,
      f.sigma.toVector,
      effects,
      f.deviance,
      f.logLik,
      f.deviance + 2.0 * parameters,
      f.deviance + StrictMath.log(rows.n.toDouble) * parameters,
      f.stage1Deviance,
      f.converged,
      boundary,
      warnings,
      spec,
      options,
      SoftwareInfo.capture(spark),
      fingerprint,
      rows,
      keys,
      f
    )
  }

  def fit(
      df: DataFrame,
      spec: LogisticREClusterSpec,
      options: LogisticREClusterOptions = LogisticREClusterOptions()
  ): LogisticREClusterFit = {
    val (n, fingerprint) = validate(df, spec)
    val (rows, keys) = collectRows(df, spec, options, n)
    val ybar = rows.y.sum / rows.n
    val start = Array.tabulate(rows.p)(j =>
      if (options.includeIntercept && j == 0)
        StrictMath.log(math.max(ybar, 1e-6) / math.max(1.0 - ybar, 1e-6))
      else 0.0
    )
    val f = Kernel.fit(rows, start, options.sigmaUpper, options.maxIterations)
    assemble(df, spec, options, fingerprint, rows, keys, f)
  }

  /** The fit's quantities at given (σ, β), for lockstep checks (as 4b's `fitAt`). */
  def fitAt(
      df: DataFrame,
      spec: LogisticREClusterSpec,
      options: LogisticREClusterOptions,
      sigma: Seq[Double],
      beta: Seq[Double]
  ): LogisticREClusterFit = {
    val (n, fingerprint) = validate(df, spec)
    val (rows, keys) = collectRows(df, spec, options, n)
    require(
      sigma.length == spec.factors.size && beta.length == rows.p,
      "sigma needs one value per factor, beta one per coefficient"
    )
    assemble(
      df,
      spec,
      options,
      fingerprint,
      rows,
      keys,
      Kernel.at(rows, sigma.toArray, beta.toArray)
    )
  }

  /** pprof_py's `summary`: Wald z for β on the normal. */
  def summary(fit: LogisticREClusterFit, level: Double = 0.95): Vector[LogisticRETest] = {
    require(level > 0.0 && level < 1.0, s"level must lie in (0, 1), got $level")
    val q = Normal.upperQuantile((1.0 - level) / 2.0)
    fit.coefficients.map { c =>
      val z = c.estimate / c.standardError
      LogisticRETest(
        c.feature,
        c.estimate,
        c.standardError,
        z,
        Normal.twoSidedPValue(z),
        c.estimate - q * c.standardError,
        c.estimate + q * c.standardError
      )
    }
  }

  private def sigmoid(t: Double): Double =
    if (t >= 0.0) 1.0 / (1.0 + StrictMath.exp(-t)) else { val e = StrictMath.exp(t); e / (1.0 + e) }

  /** pprof_py's `test` with cluster factors (§6), in slice 2c's table, providers in key order; `reference` 0 by default, as
    * pprof_py's.
    */
  def test(
      fit: LogisticREClusterFit,
      method: String = "wald",
      reference: EffectReference = EffectReference.Value(0.0),
      alternative: String = "two_sided",
      level: Double = 0.95,
      critical: Option[Double] = None,
      nResample: Int = 10000,
      seed: Long = 0L
  ): DataFrame = {
    require(
      Methods.contains(method),
      s"method must be one of ${Methods.mkString(", ")}, got $method"
    )
    require(
      Seq("two_sided", "greater", "less").contains(alternative),
      s"alternative must be two_sided, greater or less"
    )
    require(level > 0.0 && level < 1.0, s"level must lie in (0, 1), got $level")
    if (method == "exact")
      require(
        fit.spec.clusters.size == 1,
        s"test_method 'exact' needs exactly one cluster factor besides the provider; this fit has ${fit.spec.clusters.size}"
      )
    val r = fit.rows
    val k = r.factors
    val m = r.levels(0)
    val f = fit.kernel
    val blups = Array.tabulate(m)(j => f.blups(j))
    val se = Array.tabulate(m)(j => StrictMath.sqrt(math.max(f.posteriorVariance(j), 0.0)))
    val records = new Array[Long](m)
    r.index(0).foreach(j => records(j) += 1)
    val g0 = reference match {
      case EffectReference.Value(v) => v
      case EffectReference.Median   =>
        val s = blups.sorted; if (m % 2 == 1) s(m / 2) else 0.5 * (s(m / 2 - 1) + s(m / 2))
      case _ =>
        val s = new pprof.spark.numerics.NeumaierSum
        (0 until m).foreach(j => s.add(records(j).toDouble * blups(j)))
        s.value / records.sum.toDouble
    }
    val alpha = 1.0 - level
    val c = critical.getOrElse(
      Normal.upperQuantile(if (alternative == "two_sided") alpha / 2.0 else alpha)
    )
    // each row's offset and xβ, and its cluster factors' BLUP sum and posterior variance sum
    val base = Array.tabulate(r.n) { i =>
      val s = new pprof.spark.numerics.NeumaierSum
      s.add(r.offset(i)); (0 until r.p).foreach(j => s.add(r.x(i * r.p + j) * f.beta(j))); s.value
    }
    val mean =
      Array.tabulate(r.n)(i => (1 until k).map(kk => f.blups(r.offsets(kk) + r.index(kk)(i))).sum)
    val vari = Array.tabulate(r.n)(i =>
      (1 until k).map(kk => f.posteriorVariance(r.offsets(kk) + r.index(kk)(i))).sum
    )
    val byProvider = (0 until r.n).groupBy(i => r.index(0)(i))
    val rowsOut = (0 until m).map { j =>
      if (method == "wald") {
        val s = math.max(se(j), 1e-15)
        val z = (blups(j) - g0) / s
        val (pv, flag) = LogisticProviderTests.decide(z, alternative, c)
        val lower = if (alternative == "less") Double.NegativeInfinity else blups(j) - c * s
        val upper = if (alternative == "greater") Double.PositiveInfinity else blups(j) + c * s
        (j, se(j), z, pv, flag, lower, upper)
      } else {
        val idx = byProvider(j).toArray
        val observed = idx.map(i => r.y(i)).sum.round.toInt
        def zOf(t: PoissonBinomial.Tails, floor: Double) = PoissonBinomial.z(t, alternative, floor)
        def zAt(g: Double): Double = method match {
          case "exact" =>
            val clusters = idx.map(i => r.index(1)(i))
            val cm = Array.tabulate(r.levels(1))(l => f.blups(r.offsets(1) + l))
            val cv = Array.tabulate(r.levels(1))(l => f.posteriorVariance(r.offsets(1) + l))
            zOf(
              ClusteredPoissonBinomial.tails(
                ClusteredPoissonBinomial.clusteredPmf(idx.map(i => base(i) + g), clusters, cm, cv),
                observed
              ),
              PoissonBinomial.TailFloor
            )
          case _ =>
            val pr = idx.map(i => sigmoid(g + mean(i) + base(i)))
            zOf(
              PoissonBinomial.tails(pr, Array.fill(pr.length)(1), observed),
              PoissonBinomial.TailFloor
            )
        }
        val (z, lower, upper) =
          if (method == "resampling") {
            val floor = 0.5 / nResample
            val t = ClusteredPoissonBinomial.resampledTails(
              observed,
              idx.map(i => base(i) + g0),
              idx.map(mean),
              idx.map(vari),
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
                val pr = ClusteredPoissonBinomial.integratedProbabilities(
                  idx.map(i => g0 + mean(i) + base(i)),
                  idx.map(vari)
                )
                zOf(
                  PoissonBinomial.tails(pr, Array.fill(pr.length)(1), observed),
                  PoissonBinomial.TailFloor
                )
              }
            (zz, Double.NaN, Double.NaN)
          } else {
            val lo =
              if (alternative == "less") Double.NegativeInfinity
              else PoissonBinomial.invertDecreasing(zAt, blups(j), c)
            val hi =
              if (alternative == "greater") Double.PositiveInfinity
              else PoissonBinomial.invertDecreasing(zAt, blups(j), -c)
            (zAt(g0), lo, hi)
          }
        val (pv, flag) = LogisticProviderTests.decide(z, alternative, c)
        (j, Double.NaN, z, pv, flag, lower, upper)
      }
    }
    val text = fit.keys(0).headOption.exists(key => GroupKey.value(key).isInstanceOf[String])
    val schema = StructType(
      Seq(
        StructField(fit.spec.provider, if (text) StringType else LongType, nullable = false),
        StructField("estimate", DoubleType, nullable = false),
        StructField("se", DoubleType, nullable = false),
        StructField("null_value", DoubleType, nullable = false),
        StructField("z_raw", DoubleType, nullable = false),
        StructField("p_value", DoubleType, nullable = false),
        StructField("flag", IntegerType, nullable = true),
        StructField("ci_lower", DoubleType, nullable = false),
        StructField("ci_upper", DoubleType, nullable = false),
        StructField("records", LongType, nullable = false)
      )
    )
    val out = rowsOut.map { case (j, s, z, pv, flag, lo, hi) =>
      Row(GroupKey.value(fit.keys(0)(j)), blups(j), s, g0, z, pv, Int.box(flag), lo, hi, records(j))
    }
    fit.effects(fit.spec.provider).sparkSession.createDataFrame(out.asJava, schema)
  }
}
