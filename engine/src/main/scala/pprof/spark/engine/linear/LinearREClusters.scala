package pprof.spark.engine.linear

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Column, DataFrame, Encoders, Row}
import org.apache.spark.sql.functions.{
  broadcast,
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
import pprof.spark.numerics.NeumaierSum
import pprof.spark.numerics.kernels.{LinearREClusters => Kernel}

/** Column roles of the linear model with cluster factors (docs/spec/linear/random-effect-clusters.md §6, §10). */
final case class LinearREClusterSpec(
    outcome: String,
    features: Seq[String],
    provider: String,
    clusters: Seq[String],
    offset: Option[String] = None,
    weights: Option[String] = None,
    rowId: Option[String] = None
) {
  require(
    clusters.nonEmpty,
    "a fit with cluster factors needs at least one cluster column; 4a's LinearRE fits without"
  )
  def factors: Seq[String] = provider +: clusters
}

final case class LinearREClusterOptions(
    reml: Boolean = true,
    includeIntercept: Boolean = true,
    thetaUpper: Double = Double.PositiveInfinity,
    maxIterations: Int = 200,
    blocks: BlockOptions = BlockOptions()
) {
  require(thetaUpper > 0.0, s"thetaUpper must be positive, got $thetaUpper")
}

final case class LinearREClusterSearch(
    iterations: Int,
    evaluations: Int,
    projectedGradient: Double,
    converged: Boolean,
    boundary: Vector[String]
)

/** The fit (§5): β with Var(β̂) (`covariance`, p × p row-major), θ̂ by factor (`factors`, the provider first), σ̂,
  * and each factor's BLUP table (key, `records`, `weight`, `blup`).
  */
final case class LinearREClusterFit(
    coefficients: Vector[LinearCoefficient],
    covariance: Vector[Double],
    factors: Vector[String],
    theta: Vector[Double],
    sigma: Double,
    effects: Map[String, DataFrame],
    criterion: String,
    rows: Long,
    deviance: Double,
    loglik: Double,
    aic: Double,
    bic: Double,
    pwrss: Double,
    logDetH: Double,
    search: LinearREClusterSearch,
    warnings: Vector[String],
    spec: LinearREClusterSpec,
    options: LinearREClusterOptions,
    software: SoftwareInfo,
    fingerprint: Long
) {
  def estimates: Vector[Double] = coefficients.map(_.estimate)
  def standardErrors: Vector[Double] = coefficients.map(_.standardError)
  def sigmaAlpha: Vector[Double] = theta.map(_ * sigma)
}

/** One provider × cluster-levels cell's sums. */
final case class LinearREClusterCell(
    levels: Array[Int],
    count: Long,
    w: Double,
    wx: Array[Double],
    wy: Double
)

/** One provider's sums, its cells sorted by their levels. */
final case class LinearREClusterPartial(
    provider: Int,
    count: Long,
    w: Double,
    wx: Array[Double],
    wy: Double,
    xx: Array[Double],
    xy: Array[Double],
    yy: Double,
    logW: Double,
    cells: Array[LinearREClusterCell]
)

/** pprof_py's `LinearRandomEffectModel(...).fit(..., cluster_vars=...)` (slice 4d-1): one pass of sufficient
  * statistics by provider, each provider's rows in canonical order, then the kernel's search on the driver.
  */
object LinearREClusters {

  val InterceptName: String = LinearREValidation.InterceptName

  private def validate(df: DataFrame, spec: LinearREClusterSpec): (Long, Long, Double) = {
    val types = df.schema.fields.map(f => f.name -> f.dataType).toMap
    val numeric = (spec.outcome +: spec.features) ++ spec.offset ++ spec.weights
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
    val w = spec.weights.fold(lit(1.0))(n => Validation.column(n).cast(DoubleType))
    val o = spec.offset.fold(lit(0.0))(n => Validation.column(n).cast(DoubleType))
    val hashed = xxhash64((keys ++ numeric ++ spec.rowId).distinct.map(Validation.column): _*)
    val checks: Seq[Column] = numeric.distinct.flatMap { name =>
      val v = Validation.column(name)
      Seq(countWhere(v.isNull), countWhere(Validation.nonFinite(v, types(name))))
    } ++ keys.map(name => countWhere(Validation.column(name).isNull)) ++
      spec.weights.toSeq.map(n =>
        countWhere(Validation.column(n).cast(DoubleType) <= lit(0.0))
      ) ++ Seq(
        coalesce(sum(hashed.bitwiseAND(lit(0xffffffffL))), lit(0L)),
        coalesce(sum(shiftrightunsigned(hashed, 32)), lit(0L)),
        sum(w),
        sum(w * (Validation.column(spec.outcome).cast(DoubleType) - o)),
        count(lit(1))
      )
    val row = df.agg(checks.head, checks.tail: _*).collect().head
    var i = 0
    def nextLong(): Long = { val v = row.getLong(i); i += 1; v }
    def nextDouble(): Double = { val v = if (row.isNullAt(i)) 0.0 else row.getDouble(i); i += 1; v }
    numeric.distinct.foreach { name =>
      val (nulls, nonFinite) = (nextLong(), nextLong())
      if (nulls > 0 || nonFinite > 0) problems += InputProblem.InvalidValues(name, nulls, nonFinite)
    }
    keys.foreach { name =>
      val nulls = nextLong()
      if (nulls > 0) problems += InputProblem.InvalidValues(name, nulls, 0L)
    }
    spec.weights.foreach { name =>
      val bad = nextLong()
      if (bad > 0) problems += InputProblem.NonPositiveValues(name, bad)
    }
    val (low, high) = (nextLong(), nextLong())
    val (sumW, sumWy) = (nextDouble(), nextDouble())
    val rows = nextLong()
    if (rows == 0L) problems += InputProblem.EmptyInput
    val found = problems.result()
    if (found.nonEmpty) throw new InvalidInputException(found)
    (rows, low + (high << 32), sumWy / sumW)
  }

  private def keyColumn(df: DataFrame, name: String): Column = {
    val c = Validation.column(name)
    if (df.schema(name).dataType == StringType) c else c.cast(LongType)
  }

  private def sortedKeys(df: DataFrame, name: String): Vector[GroupKey] = {
    val values = df.select(keyColumn(df, name)).distinct().collect().map(_.get(0)).toVector
    val keys = values.map(GroupKey.of)
    if (values.forall(_.isInstanceOf[String]))
      keys.sortBy(k => GroupKey.value(k).asInstanceOf[String])
    else keys.sortBy(k => GroupKey.value(k).asInstanceOf[Long])
  }

  def fit(
      df: DataFrame,
      spec: LinearREClusterSpec,
      options: LinearREClusterOptions = LinearREClusterOptions()
  ): LinearREClusterFit = {
    val (rows, fingerprint, mean) = validate(df, spec)
    val yCentre = if (options.includeIntercept) mean else 0.0
    val keys = spec.factors.map(f => sortedKeys(df, f)).toVector
    val indices = keys.map(_.zipWithIndex.toMap)
    val p = spec.features.size + (if (options.includeIntercept) 1 else 0)
    val k = spec.clusters.size
    val ordered = spec.rowId.isDefined
    val intercept = options.includeIntercept
    val columns: Seq[Column] = spec.factors.map(f => keyColumn(df, f)) ++ Seq(
      spec.rowId.fold(lit(0L))(r => Validation.column(r).cast(LongType)),
      Validation.column(spec.outcome).cast(DoubleType),
      spec.offset.fold(lit(0.0))(n => Validation.column(n).cast(DoubleType)),
      spec.weights.fold(lit(1.0))(n => Validation.column(n).cast(DoubleType))
    ) ++ spec.features.map(f => Validation.column(f).cast(DoubleType))
    val partials = df
      .select(columns: _*)
      .groupByKey((r: Row) => indices(0)(GroupKey.of(r.get(0))))(Encoders.scalaInt)
      .mapGroups { (provider: Int, it: Iterator[Row]) =>
        val raw = it.map { r =>
          val levels = Array.tabulate(k)(c => indices(c + 1)(GroupKey.of(r.get(c + 1))))
          val x = Array.tabulate(p)(j =>
            if (intercept && j == 0) 1.0 else r.getDouble(k + 5 + j - (if (intercept) 1 else 0))
          )
          (
            levels,
            r.getLong(k + 1),
            r.getDouble(k + 2) - r.getDouble(k + 3) - yCentre,
            r.getDouble(k + 4),
            x
          )
        }.toArray
        def bits(v: Double): Long = java.lang.Double.doubleToRawLongBits(v)
        val sorted = raw.sortWith { (a, b) =>
          if (ordered) a._2 < b._2
          else {
            val la = a._1.toSeq; val lb = b._1.toSeq
            val c = la.zip(lb).find { case (x, y) => x != y }.map { case (x, y) => x < y }
            c.getOrElse {
              val va = Seq(bits(a._3), bits(a._4)) ++ a._5.map(bits)
              val vb = Seq(bits(b._3), bits(b._4)) ++ b._5.map(bits)
              va.zip(vb).find { case (x, y) => x != y }.exists { case (x, y) => x < y }
            }
          }
        }
        val w = new NeumaierSum; val wy = new NeumaierSum; val yy = new NeumaierSum;
        val lw = new NeumaierSum
        val wx = Array.fill(p)(new NeumaierSum); val xy = Array.fill(p)(new NeumaierSum);
        val xx = Array.fill(p * p)(new NeumaierSum)
        val cells =
          scala.collection.mutable.LinkedHashMap.empty[Seq[Int], (Array[NeumaierSum], Array[Long])]
        sorted.foreach { case (levels, _, y, wt, x) =>
          w.add(wt); wy.add(wt * y); yy.add(wt * y * y); lw.add(StrictMath.log(wt))
          var a = 0
          while (a < p) {
            wx(a).add(wt * x(a)); xy(a).add(wt * x(a) * y)
            var b = 0
            while (b < p) { xx(a * p + b).add(wt * x(a) * x(b)); b += 1 }
            a += 1
          }
          val cell =
            cells.getOrElseUpdate(levels.toSeq, (Array.fill(p + 2)(new NeumaierSum), Array(0L)))
          cell._1(0).add(wt); cell._1(1).add(wt * y)
          a = 0
          while (a < p) { cell._1(a + 2).add(wt * x(a)); a += 1 }
          cell._2(0) += 1
        }
        val cellRows = cells.toSeq
          .sortWith { (a, b) =>
            a._1.zip(b._1).find { case (x, y) => x != y }.exists { case (x, y) => x < y }
          }
          .map { case (levels, (s, n)) =>
            LinearREClusterCell(
              levels.toArray,
              n(0),
              s(0).value,
              s.drop(2).map(_.value),
              s(1).value
            )
          }
          .toArray
        LinearREClusterPartial(
          provider,
          sorted.length.toLong,
          w.value,
          wx.map(_.value),
          wy.value,
          xx.map(_.value),
          xy.map(_.value),
          yy.value,
          lw.value,
          cellRows
        )
      }(Encoders.product[LinearREClusterPartial])
      .collect()
      .sortBy(_.provider)
    val cellCount = partials.iterator.map(_.cells.length.toLong).sum
    DriverGuards.requireWithinBudget(
      s"$cellCount provider × cluster cells and ${keys.drop(1).map(_.size).sum} cluster levels (OI-59)",
      8L * (cellCount * (p + 4L + k) + partials.length.toLong * (p * p + 2L * p + 6L)) +
        { val c = keys.drop(1).map(_.size).sum.toLong; 8L * c * c },
      options.blocks
    )
    // the kernel's data, summed in provider order and, within a provider, in cell order
    val factorSums = keys.indices.map { f =>
      val n = keys(f).size
      (
        Array.fill(n)(new NeumaierSum),
        Array.fill(n * p)(new NeumaierSum),
        Array.fill(n)(new NeumaierSum),
        new Array[Long](n)
      )
    }
    val pairSums = (for (a <- 0 to k; b <- a + 1 to k)
      yield (a, b) -> scala.collection.mutable.TreeMap.empty[(Int, Int), NeumaierSum]).toMap
    val xx = Array.fill(p * p)(new NeumaierSum); val xy = Array.fill(p)(new NeumaierSum);
    val yy = new NeumaierSum; val lw = new NeumaierSum
    partials.foreach { part =>
      val (w0, wx0, wy0, n0) = factorSums(0)
      w0(part.provider).add(part.w); wy0(part.provider).add(part.wy);
      n0(part.provider) += part.count
      (0 until p).foreach(j => wx0(part.provider * p + j).add(part.wx(j)))
      (0 until p * p).foreach(z => xx(z).add(part.xx(z)))
      (0 until p).foreach(j => xy(j).add(part.xy(j)))
      yy.add(part.yy); lw.add(part.logW)
      part.cells.foreach { cell =>
        (1 to k).foreach { f =>
          val level = cell.levels(f - 1)
          val (wf, wxf, wyf, nf) = factorSums(f)
          wf(level).add(cell.w); wyf(level).add(cell.wy); nf(level) += cell.count
          (0 until p).foreach(j => wxf(level * p + j).add(cell.wx(j)))
          pairSums((0, f)).getOrElseUpdate((part.provider, level), new NeumaierSum).add(cell.w)
          ((f + 1) to k).foreach { g =>
            pairSums((f, g))
              .getOrElseUpdate((level, cell.levels(g - 1)), new NeumaierSum)
              .add(cell.w)
          }
        }
      }
    }
    val data = Kernel.Data(
      rows,
      p,
      factorSums.map { case (w, wx, wy, _) =>
        Kernel.Factor(w.length, w.map(_.value), wx.map(_.value), wy.map(_.value))
      }.toArray,
      pairSums.toSeq
        .sortBy(_._1)
        .map { case ((a, b), sums) =>
          val entries = sums.toSeq
          Kernel.Pair(
            a,
            b,
            entries.map(_._1._1).toArray,
            entries.map(_._1._2).toArray,
            entries.map(_._2.value).toArray
          )
        }
        .toArray,
      xx.map(_.value),
      xy.map(_.value),
      yy.value,
      lw.value,
      yCentre,
      if (intercept) 0 else -1
    )
    val result =
      try Kernel.fit(data, options.reml, options.thetaUpper, options.maxIterations)
      catch {
        case e: ArithmeticException =>
          throw new IllegalArgumentException(
            s"the β system is singular: the features are aliased (X-039); ${e.getMessage}"
          )
      }
    val e = result.evaluation
    val names = (if (intercept) Seq(InterceptName) else Seq.empty) ++ spec.features
    val coefficients = names.indices.map { j =>
      LinearCoefficient(names(j), e.beta(j), StrictMath.sqrt(e.sigma2 * e.sInverse(j * p + j)))
    }.toVector
    val spark = df.sparkSession
    val offsets = keys.map(_.size).scanLeft(0)(_ + _)
    val effects = spec.factors.indices.map { f =>
      val (w, _, _, n) = factorSums(f)
      val text = keys(f).headOption.exists(key => GroupKey.value(key).isInstanceOf[String])
      val schema = StructType(
        Seq(
          StructField(spec.factors(f), if (text) StringType else LongType, nullable = false),
          StructField("records", LongType, nullable = false),
          StructField("weight", DoubleType, nullable = false),
          StructField("blup", DoubleType, nullable = false)
        )
      )
      val tableRows = keys(f).indices.map { l =>
        Row(GroupKey.value(keys(f)(l)), n(l), w(l).value, result.theta(f) * e.u(offsets(f) + l))
      }
      spec.factors(f) -> spark.createDataFrame(tableRows.asJava, schema)
    }.toMap
    val parameters = p + spec.factors.size + 1
    val boundary =
      spec.factors.indices.filter(f => result.theta(f) == 0.0).map(spec.factors).toVector
    val warnings = Vector.newBuilder[String]
    boundary.foreach(f =>
      warnings += s"the variance of $f's random intercept is estimated at 0 (a boundary fit)"
    )
    if (!result.converged)
      warnings += s"the search did not converge (projected gradient ${result.projectedGradient})"
    LinearREClusterFit(
      coefficients,
      (0 until p * p).map(z => e.sigma2 * e.sInverse(z)).toVector,
      spec.factors.toVector,
      result.theta.toVector,
      StrictMath.sqrt(e.sigma2),
      effects,
      if (options.reml) "REML" else "ML",
      rows,
      e.criterion,
      -0.5 * e.criterion,
      e.criterion + 2.0 * parameters,
      e.criterion + StrictMath.log(rows.toDouble) * parameters,
      e.pwrss,
      e.logDetH,
      LinearREClusterSearch(
        result.iterations,
        result.evaluations,
        result.projectedGradient,
        result.converged,
        boundary
      ),
      warnings.result(),
      spec,
      options,
      SoftwareInfo.capture(spark),
      fingerprint
    )
  }

  /** The rows with `xbeta`, `eta` (the offset, xβ and the BLUPs of `factors`, 0 for unseen levels) — `fitted` uses every
    * factor, as pprof_py's `fitted_values`; `predict` the factors `reVars` names, or none.
    */
  private def withEta(df: DataFrame, fit: LinearREClusterFit, factors: Seq[String]): DataFrame = {
    val b = fit.estimates
    val names = fit.coefficients.map(_.feature)
    val xbeta = names.indices.foldLeft(lit(0.0): Column) { (acc, j) =>
      if (names(j) == InterceptName) acc + lit(b(j))
      else acc + Validation.column(names(j)).cast(DoubleType) * lit(b(j))
    }
    val offset = fit.spec.offset
      .filter(df.columns.contains)
      .fold(lit(0.0))(n => Validation.column(n).cast(DoubleType))
    var out = df.withColumn("xbeta", xbeta)
    var eta = offset + Validation.column("xbeta")
    factors.zipWithIndex.foreach { case (f, i) =>
      val effect = fit.effects(f)
      val keyName = s"__pprof_key_$i"
      val blupName = s"__pprof_blup_$i"
      val table = broadcast(effect.select(effect(f).as(keyName), effect("blup").as(blupName)))
      val left =
        if (effect.schema(f).dataType == StringType) Validation.column(f).cast(StringType)
        else Validation.column(f).cast(LongType)
      out = out.join(table, left === table(keyName), "left").drop(keyName)
      eta = eta + coalesce(Validation.column(blupName), lit(0.0))
    }
    out.withColumn("eta", eta).drop(factors.indices.map(i => s"__pprof_blup_$i"): _*)
  }

  def fitted(df: DataFrame, fit: LinearREClusterFit): DataFrame =
    withEta(df, fit, fit.factors).withColumnRenamed("eta", "fitted")

  def predict(
      df: DataFrame,
      fit: LinearREClusterFit,
      reVars: Seq[String] = Seq.empty
  ): DataFrame = {
    val unknown = reVars.filterNot(fit.factors.contains)
    require(
      unknown.isEmpty,
      s"unknown grouping variables ${unknown.mkString(", ")}; fitted: ${fit.factors.mkString(", ")}"
    )
    withEta(df, fit, reVars).withColumnRenamed("eta", "prediction")
  }
}
