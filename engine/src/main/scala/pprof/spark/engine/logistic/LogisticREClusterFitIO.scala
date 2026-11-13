package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.layout.GroupKey
import pprof.spark.engine.metadata.SoftwareInfo
import pprof.spark.numerics.kernels.{LogisticREClusters => Kernel}

/** A saved logistic cluster-factor fit: everything but the records, which `LogisticREClusterFitIO.attach` re-collects. */
final case class LogisticREClusterSaved(
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
    iterations: Int,
    evaluations: Int,
    projectedGradient: Double,
    converged: Boolean,
    boundary: Vector[String],
    warnings: Vector[String],
    spec: LogisticREClusterSpec,
    options: LogisticREClusterOptions,
    software: SoftwareInfo,
    rows: Long,
    fingerprint: Long,
    keys: Vector[Vector[GroupKey]],
    u: Array[Double],
    blups: Array[Double],
    posteriorVariance: Array[Double]
) {
  def estimates: Vector[Double] = coefficients.map(_.estimate)
}

/** Persistence of the logistic cluster-factor fit (docs/spec/random-effects/cluster-terms-job.md §1.2), kind
  * `LogisticREClusterFit`, format version 1: `metadata` (one JSON line, every double as its bit pattern) and one Parquet
  * table per factor under `effects/<factor index>` (key, `records`, `blup`, `variance`, `se`, `u`). Written once.
  */
object LogisticREClusterFitIO {

  val FormatVersion = 1
  val Kind = "LogisticREClusterFit"

  private def bits(v: Double): Long = java.lang.Double.doubleToRawLongBits(v)
  private def double(v: JsonNode): Double = java.lang.Double.longBitsToDouble(v.asLong())

  def save(spark: SparkSession, fit: LogisticREClusterFit, path: String): Unit = {
    val out = path.stripSuffix("/")
    val mapper = new ObjectMapper()
    val m = mapper.createObjectNode()
    m.put("formatVersion", FormatVersion)
    m.put("kind", Kind)
    val c = m.putObject("columns")
    c.put("outcome", fit.spec.outcome)
    val features = c.putArray("features"); fit.spec.features.foreach(f => features.add(f))
    c.put("provider", fit.spec.provider)
    val clusters = c.putArray("clusters"); fit.spec.clusters.foreach(f => clusters.add(f))
    fit.spec.offset.foreach(v => c.put("offset", v))
    fit.spec.rowId.foreach(v => c.put("rowId", v))
    val o = m.putObject("options")
    o.put("includeIntercept", fit.options.includeIntercept)
    o.put("sigmaUpperBits", bits(fit.options.sigmaUpper))
    o.put("maxIterations", fit.options.maxIterations)
    o.put("targetBlockBytes", fit.options.blocks.targetBlockBytes)
    o.put("maxGroupsOnDriver", fit.options.blocks.maxGroupsOnDriver)
    o.put("driverBudgetBytes", fit.options.blocks.driverBudgetBytes)
    o.put("storageLevel", fit.options.blocks.storageLevel)
    val coefficients = m.putArray("coefficients")
    fit.coefficients.foreach { k =>
      val node = coefficients.addObject()
      node.put("feature", k.feature); node.put("estimateBits", bits(k.estimate));
      node.put("seBits", bits(k.standardError))
    }
    val cov = m.putArray("covarianceBits"); fit.covariance.foreach(v => cov.add(bits(v)))
    val factors = m.putArray("factors"); fit.factors.foreach(f => factors.add(f))
    val sigma = m.putArray("sigmaBits"); fit.sigma.foreach(v => sigma.add(bits(v)))
    val levels = m.putArray("levels"); fit.rows.levels.foreach(l => levels.add(l))
    Seq(
      "deviance" -> fit.deviance,
      "loglik" -> fit.loglik,
      "aic" -> fit.aic,
      "bic" -> fit.bic,
      "stage1Deviance" -> fit.stage1Deviance
    ).foreach { case (k, v) => m.put(s"${k}Bits", bits(v)) }
    m.put("iterations", fit.kernel.iterations)
    m.put("evaluations", fit.kernel.evaluations)
    m.put("projectedGradientBits", bits(fit.kernel.projectedGradient))
    m.put("converged", fit.converged)
    val boundary = m.putArray("boundary"); fit.boundary.foreach(b => boundary.add(b))
    val warnings = m.putArray("warnings"); fit.warnings.foreach(w => warnings.add(w))
    val software = m.putObject("software")
    fit.software.toFields.foreach { case (k, v) => software.put(k, v) }
    m.put("rows", fit.rows.n.toLong)
    m.put("fingerprint", fit.fingerprint)
    m.put("status", "complete")
    spark
      .createDataset(Seq(mapper.writeValueAsString(m)))(Encoders.STRING)
      .coalesce(1)
      .write
      .mode("errorifexists")
      .text(
        s"$out/metadata"
      ) // the first write: an existing fit is found here, and nothing is overwritten
    fit.factors.indices.foreach { k =>
      val text = fit.keys(k).headOption.exists(key => GroupKey.value(key).isInstanceOf[String])
      val schema = StructType(
        StructField(fit.factors(k), if (text) StringType else LongType, nullable = false) +:
          (StructField("records", LongType, nullable = false) +:
            Seq("blup", "variance", "se", "u").map(StructField(_, DoubleType, nullable = false)))
      )
      val counts = new Array[Long](fit.rows.levels(k))
      fit.rows.index(k).foreach(l => counts(l) += 1)
      val table = fit.keys(k).indices.map { l =>
        val z = fit.rows.offsets(k) + l
        val variance = fit.kernel.posteriorVariance(z)
        Row(
          GroupKey.value(fit.keys(k)(l)),
          counts(l),
          fit.kernel.blups(z),
          variance,
          StrictMath.sqrt(math.max(variance, 0.0)),
          fit.kernel.u(z)
        )
      }
      spark
        .createDataFrame(table.asJava, schema)
        .coalesce(1)
        .write
        .mode("errorifexists")
        .parquet(s"$out/effects/$k")
    }
  }

  def load(spark: SparkSession, path: String): LogisticREClusterSaved = {
    val out = path.stripSuffix("/")
    val m =
      new ObjectMapper().readTree(spark.read.text(s"$out/metadata").collect().head.getString(0))
    val kind = m.path("kind").asText("")
    require(kind == Kind, s"$out holds a $kind, not a $Kind")
    val version = m.path("formatVersion").asInt(-1)
    require(
      version == FormatVersion,
      s"$out has format version $version; this release reads format version $FormatVersion"
    )
    def strings(n: JsonNode): Seq[String] = n.elements().asScala.map(_.asText()).toSeq
    def opt(n: JsonNode, k: String): Option[String] =
      Option(n.get(k)).filterNot(_.isNull).map(_.asText())
    val c = m.get("columns")
    val spec = LogisticREClusterSpec(
      c.get("outcome").asText(),
      strings(c.get("features")),
      c.get("provider").asText(),
      strings(c.get("clusters")),
      opt(c, "offset"),
      opt(c, "rowId")
    )
    val o = m.get("options")
    val options = LogisticREClusterOptions(
      o.get("includeIntercept").asBoolean(),
      double(o.get("sigmaUpperBits")),
      o.get("maxIterations").asInt(),
      BlockOptions(
        o.get("targetBlockBytes").asLong(),
        o.get("maxGroupsOnDriver").asInt(),
        o.get("driverBudgetBytes").asLong(),
        o.get("storageLevel").asText()
      )
    )
    val factors = strings(m.get("factors")).toVector
    val levels = m.get("levels").elements().asScala.map(_.asInt()).toVector
    val tables = factors.indices.map { k =>
      val rows = spark.read.parquet(s"$out/effects/$k").collect()
      require(
        rows.length == levels(k),
        s"$out holds ${rows.length} levels of ${factors(k)}; its metadata says ${levels(k)}"
      )
      val text = rows.headOption.exists(_.get(0).isInstanceOf[String])
      if (text) rows.sortBy(_.getString(0)) else rows.sortBy(_.getLong(0))
    }
    val keys = tables.map(_.map(r => GroupKey.of(r.get(0))).toVector).toVector
    def column(name: String): Array[Double] =
      tables.flatMap(_.map(r => r.getAs[Double](name))).toArray
    val effects = factors.indices.map { k =>
      factors(k) -> spark.read
        .parquet(s"$out/effects/$k")
        .select(factors(k), "records", "blup", "variance", "se")
        .orderBy(factors(k))
    }.toMap
    LogisticREClusterSaved(
      m.get("coefficients")
        .elements()
        .asScala
        .map(n =>
          LogisticRECoefficient(
            n.get("feature").asText(),
            double(n.get("estimateBits")),
            double(n.get("seBits"))
          )
        )
        .toVector,
      m.get("covarianceBits").elements().asScala.map(double).toVector,
      factors,
      m.get("sigmaBits").elements().asScala.map(double).toVector,
      effects,
      double(m.get("devianceBits")),
      double(m.get("loglikBits")),
      double(m.get("aicBits")),
      double(m.get("bicBits")),
      double(m.get("stage1DevianceBits")),
      m.get("iterations").asInt(),
      m.get("evaluations").asInt(),
      double(m.get("projectedGradientBits")),
      m.get("converged").asBoolean(),
      strings(m.get("boundary")).toVector,
      strings(m.get("warnings")).toVector,
      spec,
      options,
      SoftwareInfo.fromFields(
        m.get("software").properties().asScala.map(e => e.getKey -> e.getValue.asText()).toSeq
      ),
      m.get("rows").asLong(),
      m.get("fingerprint").asLong(),
      keys,
      column("u"),
      column("blup"),
      column("variance")
    )
  }

  /** The saved fit with `df`, its training data, re-collected on the driver and checked: the same rows, fingerprint and
    * level keys (API-3); its kernel state rebuilt from the saved arrays.
    */
  def attach(df: DataFrame, saved: LogisticREClusterSaved): LogisticREClusterFit = {
    val (n, fingerprint) = LogisticREClusters.validate(df, saved.spec)
    if (n != saved.rows || fingerprint != saved.fingerprint)
      throw new IllegalArgumentException(
        s"these are not the fit's training data: $n rows with fingerprint $fingerprint, where the fit read ${saved.rows} with ${saved.fingerprint}"
      )
    val (rows, keys) = LogisticREClusters.collectRows(df, saved.spec, saved.options, n)
    require(keys == saved.keys, "the training data's levels differ from the saved fit's")
    val kernel = Kernel.Fit(
      saved.sigma.toArray,
      saved.estimates.toArray,
      saved.u,
      saved.blups,
      saved.posteriorVariance,
      saved.covariance.toArray,
      saved.deviance,
      saved.loglik,
      saved.stage1Deviance,
      saved.iterations,
      saved.evaluations,
      saved.projectedGradient,
      saved.converged
    )
    LogisticREClusterFit(
      saved.coefficients,
      saved.covariance,
      saved.factors,
      saved.sigma,
      saved.effects,
      saved.deviance,
      saved.loglik,
      saved.aic,
      saved.bic,
      saved.stage1Deviance,
      saved.converged,
      saved.boundary,
      saved.warnings,
      saved.spec,
      saved.options,
      saved.software,
      saved.fingerprint,
      rows,
      keys,
      kernel
    )
  }
}
