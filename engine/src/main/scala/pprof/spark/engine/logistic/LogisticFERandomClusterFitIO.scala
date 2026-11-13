package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{
  BooleanType,
  DoubleType,
  LongType,
  StringType,
  StructField,
  StructType
}
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.layout.GroupKey
import pprof.spark.engine.metadata.SoftwareInfo

/** A saved random-cluster fit: everything but the records, which `LogisticFERandomClusterFitIO.attach` re-joins. */
final case class LogisticFERandomClusterSaved(
    stage3: ThreeStageStage3,
    beta: Vector[Double],
    sigma: Double,
    spec: LogisticFERandomClusterSpec,
    options: LogisticFERandomClusterOptions,
    warnings: Vector[String],
    withStage1: Boolean,
    rows: Long,
    fingerprint: Long,
    software: Option[SoftwareInfo]
)

/** Persistence of random-cluster fits (docs/spec/logistic/fe-random-cluster-job.md §3), format version 1: `metadata`
  * (one JSON line, every double as its 64-bit pattern), `providers` and `clusters` (Parquet). Written once.
  */
object LogisticFERandomClusterFitIO {

  val FormatVersion = 1
  val Kind = "LogisticFERandomClusterFit"

  private def bits(v: Double): Long = java.lang.Double.doubleToRawLongBits(v)
  private def double(v: JsonNode): Double = java.lang.Double.longBitsToDouble(v.asLong())

  private def keyed(
      spark: SparkSession,
      keys: Vector[GroupKey],
      name: String,
      columns: Seq[(String, Int => Any)],
      types: Seq[org.apache.spark.sql.types.DataType]
  ): DataFrame = {
    val values = keys.map(GroupKey.value)
    val text = values.exists(_.isInstanceOf[String])
    val schema = StructType(
      StructField(name, if (text) StringType else LongType, nullable = false) +:
        columns.zip(types).map { case ((c, _), t) => StructField(c, t, nullable = false) }
    )
    val rows = values.indices.map(i =>
      Row.fromSeq((if (text) values(i).toString else values(i)) +: columns.map(_._2(i)))
    )
    spark.createDataFrame(rows.asJava, schema)
  }

  def save(spark: SparkSession, fit: LogisticFERandomClusterFit, path: String): Unit = {
    val out = path.stripSuffix("/")
    val mapper = new ObjectMapper()
    val m = mapper.createObjectNode()
    m.put("formatVersion", FormatVersion)
    m.put("kind", Kind)
    val c = m.putObject("columns")
    c.put("outcome", fit.spec.outcome)
    val f = c.putArray("features")
    fit.spec.features.foreach(x => f.add(x))
    c.put("provider", fit.spec.provider)
    c.put("cluster", fit.spec.cluster)
    fit.spec.observed.foreach(x => c.put("observed", x))
    fit.spec.rowId.foreach(x => c.put("rowId", x))
    val o = m.putObject("options")
    val opt = fit.options
    o.put("nNodes", opt.nNodes)
    o.put("maxIter", opt.maxIter)
    o.put("tolBits", bits(opt.tol))
    o.put("boundBits", bits(opt.bound))
    o.put("boundMode", opt.boundMode)
    o.put("estimator", opt.estimator)
    o.put("convergenceCriterion", opt.convergenceCriterion)
    o.put("path", opt.path)
    o.put("targetBlockBytes", opt.blocks.targetBlockBytes)
    o.put("maxGroupsOnDriver", opt.blocks.maxGroupsOnDriver)
    o.put("driverBudgetBytes", opt.blocks.driverBudgetBytes)
    o.put("storageLevel", opt.blocks.storageLevel)
    val b = m.putArray("betaBits")
    fit.beta.foreach(v => b.add(bits(v)))
    m.put("sigmaBits", bits(fit.sigma))
    val s3 = fit.stage3
    m.put("converged", s3.converged)
    m.put("stalled", s3.stalled)
    m.put("iterations", s3.iterations)
    m.put("criterionBits", bits(s3.criterion))
    m.put("loglikBits", bits(s3.loglik))
    m.put("cells", s3.cells)
    m.put("bins", s3.bins)
    m.put("providers", s3.providers.size)
    m.put("clusters", s3.clusters.size)
    val w = m.putArray("warnings")
    fit.warnings.foreach(x => w.add(x))
    m.put("withStage1", fit.stage1.nonEmpty)
    m.put("rows", fit.rows)
    m.put("fingerprint", fit.fingerprint)
    val sw = m.putObject("software")
    fit.software.foreach(_.toFields.foreach { case (k, v) => sw.put(k, v) })
    m.put("status", "complete")
    spark
      .createDataset(Seq(mapper.writeValueAsString(m)))(Encoders.STRING)
      .coalesce(1)
      .write
      .mode("errorifexists") // the first write finds an existing fit; nothing is overwritten
      .text(s"$out/metadata")
    keyed(
      spark,
      s3.providers,
      fit.spec.provider,
      Seq("gamma" -> (i => s3.gamma(i)), "held_at_bound" -> (i => s3.held(i))),
      Seq(DoubleType, BooleanType)
    ).write.mode("errorifexists").parquet(s"$out/providers")
    keyed(
      spark,
      s3.clusters,
      fit.spec.cluster,
      Seq("alpha_mean" -> (i => s3.alphaMean(i)), "alpha_var" -> (i => s3.alphaVar(i))),
      Seq(DoubleType, DoubleType)
    ).write.mode("errorifexists").parquet(s"$out/clusters")
  }

  def load(spark: SparkSession, path: String): LogisticFERandomClusterSaved = {
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
    val c = m.get("columns")
    def opt(n: JsonNode, k: String): Option[String] =
      Option(n.get(k)).filterNot(_.isNull).map(_.asText())
    val spec = LogisticFERandomClusterSpec(
      c.get("outcome").asText(),
      c.get("features").elements().asScala.map(_.asText()).toSeq,
      c.get("provider").asText(),
      c.get("cluster").asText(),
      opt(c, "observed"),
      opt(c, "rowId")
    )
    val o = m.get("options")
    val options = LogisticFERandomClusterOptions(
      o.get("nNodes").asInt(),
      o.get("maxIter").asInt(),
      double(o.get("tolBits")),
      double(o.get("boundBits")),
      o.get("boundMode").asText(),
      o.get("estimator").asText(),
      BlockOptions(
        o.get("targetBlockBytes").asLong(),
        o.get("maxGroupsOnDriver").asInt(),
        o.get("driverBudgetBytes").asLong(),
        o.get("storageLevel").asText()
      ),
      o.get("path").asText(),
      o.get("convergenceCriterion").asText()
    )
    def table(
        name: String,
        key: String,
        columns: Seq[String]
    ): (Vector[GroupKey], Seq[Array[Any]]) = {
      val rows = spark.read.parquet(s"$out/$name").orderBy(key).collect()
      (
        rows.map(r => GroupKey.of(r.get(0))).toVector,
        columns.map(col => rows.map(r => r.getAs[Any](col)))
      )
    }
    val (providers, pcols) = table("providers", spec.provider, Seq("gamma", "held_at_bound"))
    val (clusters, ccols) = table("clusters", spec.cluster, Seq("alpha_mean", "alpha_var"))
    require(
      providers.size == m.get("providers").asInt() && clusters.size == m.get("clusters").asInt(),
      s"$out holds ${providers.size} providers and ${clusters.size} clusters; its metadata says ${m.get("providers").asInt()} and ${m.get("clusters").asInt()}"
    )
    val sigma = double(m.get("sigmaBits"))
    val stage3 = ThreeStageStage3(
      providers,
      clusters,
      pcols(0).map(_.asInstanceOf[Double]),
      sigma,
      m.get("converged").asBoolean(),
      m.get("stalled").asBoolean(),
      m.get("iterations").asInt(),
      double(m.get("criterionBits")),
      double(m.get("loglikBits")),
      ccols(0).map(_.asInstanceOf[Double]),
      ccols(1).map(_.asInstanceOf[Double]),
      pcols(1).map(_.asInstanceOf[Boolean]),
      m.get("cells").asInt(),
      m.get("bins").asLong(),
      options.stage3
    )
    val sw = m.get("software")
    LogisticFERandomClusterSaved(
      stage3,
      m.get("betaBits").elements().asScala.map(double).toVector,
      sigma,
      spec,
      options,
      m.get("warnings").elements().asScala.map(_.asText()).toVector,
      m.get("withStage1").asBoolean(),
      m.get("rows").asLong(),
      m.get("fingerprint").asLong(),
      if (sw.size() == 0) None
      else
        Some(
          SoftwareInfo.fromFields(
            sw.properties().asScala.map(e => e.getKey -> e.getValue.asText()).toSeq
          )
        )
    )
  }

  /** The saved fit with `df`'s records re-attached; `df` must be the training data (its rows and fingerprint, API-3). */
  def attach(df: DataFrame, saved: LogisticFERandomClusterSaved): LogisticFERandomClusterFit = {
    val (records, cells, rows, fingerprint) =
      LogisticFERandomCluster.prepare(df, saved.spec, saved.beta, saved.options)
    if (rows != saved.rows || fingerprint != saved.fingerprint) {
      records.unpersist()
      throw new IllegalArgumentException(
        s"these are not the fit's training data: $rows rows with fingerprint $fingerprint, where the fit read ${saved.rows} with ${saved.fingerprint}"
      )
    }
    require(
      cells.providers == saved.stage3.providers && cells.clusters == saved.stage3.clusters,
      "the training data's providers and clusters differ from the saved fit's"
    )
    LogisticFERandomClusterFit(
      saved.stage3,
      saved.beta,
      saved.sigma,
      records,
      cells,
      None,
      saved.spec,
      saved.options,
      saved.warnings,
      rows,
      fingerprint,
      saved.software
    )
  }
}
