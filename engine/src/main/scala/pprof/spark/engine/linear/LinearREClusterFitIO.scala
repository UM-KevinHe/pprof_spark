package pprof.spark.engine.linear

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.{Encoders, SparkSession}
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.metadata.SoftwareInfo

/** Persistence of the linear cluster-factor fit (docs/spec/random-effects/cluster-terms-job.md §1.1), kind
  * `LinearREClusterFit`, format version 1: `metadata` (one JSON line, every double as its bit pattern) and one
  * Parquet table of BLUPs per factor under `effects/<factor index>`. Written once; a loaded fit needs no records.
  */
object LinearREClusterFitIO {

  val FormatVersion = 1
  val Kind = "LinearREClusterFit"

  private def bits(v: Double): Long = java.lang.Double.doubleToRawLongBits(v)
  private def double(v: JsonNode): Double = java.lang.Double.longBitsToDouble(v.asLong())

  def save(spark: SparkSession, fit: LinearREClusterFit, path: String): Unit = {
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
    fit.spec.weights.foreach(v => c.put("weights", v))
    fit.spec.rowId.foreach(v => c.put("rowId", v))
    val o = m.putObject("options")
    o.put("reml", fit.options.reml)
    o.put("includeIntercept", fit.options.includeIntercept)
    o.put("thetaUpperBits", bits(fit.options.thetaUpper))
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
    val theta = m.putArray("thetaBits"); fit.theta.foreach(v => theta.add(bits(v)))
    val levels = m.putArray("levels"); fit.factors.foreach(f => levels.add(fit.effects(f).count()))
    m.put("sigmaBits", bits(fit.sigma))
    m.put("criterion", fit.criterion)
    m.put("rows", fit.rows)
    Seq(
      "deviance" -> fit.deviance,
      "loglik" -> fit.loglik,
      "aic" -> fit.aic,
      "bic" -> fit.bic,
      "pwrss" -> fit.pwrss,
      "logDetH" -> fit.logDetH
    ).foreach { case (k, v) => m.put(s"${k}Bits", bits(v)) }
    val s = m.putObject("search")
    s.put("iterations", fit.search.iterations)
    s.put("evaluations", fit.search.evaluations)
    s.put("projectedGradientBits", bits(fit.search.projectedGradient))
    s.put("converged", fit.search.converged)
    val boundary = s.putArray("boundary"); fit.search.boundary.foreach(b => boundary.add(b))
    val warnings = m.putArray("warnings"); fit.warnings.foreach(w => warnings.add(w))
    val software: ObjectNode = m.putObject("software")
    fit.software.toFields.foreach { case (k, v) => software.put(k, v) }
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
    fit.factors.zipWithIndex.foreach { case (f, k) =>
      fit.effects(f).orderBy(f).coalesce(1).write.mode("errorifexists").parquet(s"$out/effects/$k")
    }
  }

  def load(spark: SparkSession, path: String): LinearREClusterFit = {
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
    val spec = LinearREClusterSpec(
      c.get("outcome").asText(),
      strings(c.get("features")),
      c.get("provider").asText(),
      strings(c.get("clusters")),
      opt(c, "offset"),
      opt(c, "weights"),
      opt(c, "rowId")
    )
    val o = m.get("options")
    val options = LinearREClusterOptions(
      o.get("reml").asBoolean(),
      o.get("includeIntercept").asBoolean(),
      double(o.get("thetaUpperBits")),
      o.get("maxIterations").asInt(),
      BlockOptions(
        o.get("targetBlockBytes").asLong(),
        o.get("maxGroupsOnDriver").asInt(),
        o.get("driverBudgetBytes").asLong(),
        o.get("storageLevel").asText()
      )
    )
    val factors = strings(m.get("factors")).toVector
    val levels = m.get("levels").elements().asScala.map(_.asLong()).toVector
    val effects = factors.zipWithIndex.map { case (f, k) =>
      val table = spark.read.parquet(s"$out/effects/$k").orderBy(f)
      val count = table.count()
      require(count == levels(k), s"$out holds $count levels of $f; its metadata says ${levels(k)}")
      f -> table
    }.toMap
    val s = m.get("search")
    LinearREClusterFit(
      m.get("coefficients")
        .elements()
        .asScala
        .map(n =>
          LinearCoefficient(
            n.get("feature").asText(),
            double(n.get("estimateBits")),
            double(n.get("seBits"))
          )
        )
        .toVector,
      m.get("covarianceBits").elements().asScala.map(double).toVector,
      factors,
      m.get("thetaBits").elements().asScala.map(double).toVector,
      double(m.get("sigmaBits")),
      effects,
      m.get("criterion").asText(),
      m.get("rows").asLong(),
      double(m.get("devianceBits")),
      double(m.get("loglikBits")),
      double(m.get("aicBits")),
      double(m.get("bicBits")),
      double(m.get("pwrssBits")),
      double(m.get("logDetHBits")),
      LinearREClusterSearch(
        s.get("iterations").asInt(),
        s.get("evaluations").asInt(),
        double(s.get("projectedGradientBits")),
        s.get("converged").asBoolean(),
        strings(s.get("boundary")).toVector
      ),
      strings(m.get("warnings")).toVector,
      spec,
      options,
      SoftwareInfo.fromFields(
        m.get("software").properties().asScala.map(e => e.getKey -> e.getValue.asText()).toSeq
      ),
      m.get("fingerprint").asLong()
    )
  }
}
