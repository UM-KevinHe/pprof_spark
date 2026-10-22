package pprof.spark.app

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import pprof.spark.engine.linear._

/** The linear random-intercept model as a Spark job (docs/spec/linear/random-effect-job.md): reads a run specification
  * (version 1, `model` `linear-re`), fits the model, and writes each requested output to its own directory under
  * `outputs.path` (never overwriting), the run record last.
  *
  * Usage: `LinearREJob --spec <path>`, read through Spark so any file system works, or `LinearREJob --spec-json <json>`.
  */
object LinearREJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("pprof-spark LinearREJob").getOrCreate()
    val json = specification(
      args,
      path => spark.read.option("wholetext", "true").text(path).collect().head.getString(0)
    )
    println(run(spark, LinearRERunSpec.parse(json), json).json)
  }

  def specification(args: Array[String], read: String => String): String = args.toSeq match {
    case Seq("--spec", path)      => read(path)
    case Seq("--spec-json", json) => json
    case _                        =>
      throw new IllegalArgumentException("usage: LinearREJob --spec <path> | --spec-json <json>")
  }

  /** `fixedEffectsTable` as a table: feature, estimate, std_error, t_value, df, p_value (spec §4). */
  def fixedEffectsFrame(spark: SparkSession, effects: Vector[LinearREFixedEffect]): DataFrame =
    spark.createDataFrame(
      effects
        .map(e =>
          Row(e.feature, e.estimate, e.standardError, e.statistic, e.degreesOfFreedom, e.pValue)
        )
        .asJava,
      StructType(
        StructField("feature", StringType, nullable = false) +:
          Seq("estimate", "std_error", "t_value", "df", "p_value")
            .map(name => StructField(name, DoubleType, nullable = false))
      )
    )

  /** `confInt` as a table: feature, lower, upper (spec §4). */
  def confIntFrame(spark: SparkSession, bounds: Vector[(String, Double, Double)]): DataFrame =
    spark.createDataFrame(
      bounds.map { case (feature, lower, upper) => Row(feature, lower, upper) }.asJava,
      StructType(
        Seq(
          StructField("feature", StringType, nullable = false),
          StructField("lower", DoubleType, nullable = false),
          StructField("upper", DoubleType, nullable = false)
        )
      )
    )

  def run(spark: SparkSession, spec: LinearRERunSpec, specJson: String): RunRecord = {
    val timings = scala.collection.mutable.LinkedHashMap.empty[String, Long]
    def timed[A](stage: String)(body: => A): A = {
      val start = System.nanoTime()
      try body
      finally timings(stage) = (System.nanoTime() - start) / 1000000L
    }
    val out = spec.outputs.path.stripSuffix("/")
    val written = Vector.newBuilder[String]
    def write(table: DataFrame, name: String): Unit = {
      table.write.mode("errorifexists").parquet(s"$out/$name")
      written += s"$out/$name"
    }
    val df = timed("read") {
      val reader = spark.read.options(spec.input.options)
      spec.input.table.fold(reader.format(spec.input.format).load(spec.input.path.get))(t =>
        reader.table(t)
      )
    }
    val fit = timed("fit")(LinearRE.fit(df, spec.spec, spec.options))
    val o = spec.outputs
    if (o.fit) timed("write fit") {
      LinearREFitIO.save(spark, fit, s"$out/fit")
      written += s"$out/fit"
    }
    if (o.providers) timed("providers")(write(fit.providers, "providers"))
    o.summary.foreach(s =>
      timed("summary")(
        write(
          LinearJob.summaryTable(
            spark,
            LinearREInference.summary(fit, s.level, s.nullValue, s.alternative)
          ),
          "summary"
        )
      )
    )
    if (o.fixedEffects)
      timed("fixed effects")(
        write(fixedEffectsFrame(spark, LinearREInference.fixedEffectsTable(fit)), "fixed_effects")
      )
    o.confInt.foreach(level =>
      timed("conf_int")(
        write(confIntFrame(spark, LinearREInference.confInt(fit, level)), "conf_int")
      )
    )
    o.providerTests.foreach(t =>
      timed("provider tests")(
        write(
          LinearREInference.test(fit, t.reference, t.alternative, t.level, t.critical),
          "provider_tests"
        )
      )
    )
    o.alphaIntervals.foreach(level =>
      timed("alpha intervals")(
        write(LinearREInference.alphaIntervals(fit, level), "alpha_intervals")
      )
    )
    o.measures.foreach(m =>
      timed("measures") {
        LinearREInference.measures(df, fit, m.kinds, m.reference).foreach { case (kind, table) =>
          write(table, s"measures/$kind")
        }
      }
    )
    o.measureIntervals.foreach(m =>
      timed("measure intervals") {
        LinearREInference
          .smIntervals(df, fit, m.kinds, m.reference, m.alternative, m.level)
          .foreach { case (kind, table) =>
            write(table, s"measure_intervals/$kind")
          }
      }
    )
    o.predictions.foreach(useRe =>
      timed("predictions")(
        write(LinearRE.predict(df, fit, useRe, withOffset = true), "predictions")
      )
    )
    if (o.fitted) timed("fitted")(write(LinearRE.fitted(df, fit), "fitted"))
    val outputs = written.result()
    val mapper = new ObjectMapper()
    val record = mapper.createObjectNode()
    def number(
        key: String,
        value: Double,
        into: com.fasterxml.jackson.databind.node.ObjectNode = record
    ): Unit =
      if (value.isInfinite || value.isNaN) into.putNull(key) else into.put(key, value)
    record.put("model", "linear-re")
    record.set("specification", mapper.readTree(specJson))
    val options = record.putObject("fitOptions")
    options.put("reml", fit.options.reml)
    options.put("includeIntercept", fit.options.includeIntercept)
    number("thetaUpper", fit.options.thetaUpper, options)
    options.put("maxEvaluations", fit.options.maxEvaluations)
    options.put("targetBlockBytes", fit.options.blocks.targetBlockBytes)
    options.put("maxGroupsOnDriver", fit.options.blocks.maxGroupsOnDriver)
    options.put("driverBudgetBytes", fit.options.blocks.driverBudgetBytes)
    options.put("storageLevel", fit.options.blocks.storageLevel)
    val software = record.putObject("software")
    fit.software.toFields.foreach { case (k, v) => software.put(k, v) }
    record.put("fingerprint", fit.fingerprint)
    record.put("rows", fit.rows)
    record.put("providers", fit.providerCount)
    record.put("fixedEffects", fit.coefficients.size)
    record.put("criterion", fit.criterion)
    Seq(
      "sigma" -> fit.sigma,
      "theta" -> fit.theta,
      "sigmaAlpha" -> fit.sigmaAlpha,
      "deviance" -> fit.deviance,
      "loglik" -> fit.loglik,
      "aic" -> fit.aic,
      "bic" -> fit.bic
    ).foreach { case (key, value) => number(key, value) }
    val search = record.putObject("search")
    search.put("evaluations", fit.search.evaluations)
    number("lower", fit.search.lower, search)
    number("upper", fit.search.upper, search)
    number("slope", fit.search.slope, search)
    fit.search.boundary.fold(search.putNull("boundary"))(b => search.put("boundary", b))
    search.put("path", fit.search.path)
    val warnings = record.putArray("warnings")
    fit.warnings.foreach(w => warnings.add(w))
    record.put("converged", true)
    val outputList = record.putArray("outputs")
    outputs.foreach(p => outputList.add(p))
    val times = record.putObject("timingsMillis")
    timings.foreach { case (stage, ms) => times.put(stage, ms) }
    val json = mapper.writeValueAsString(record)
    spark
      .createDataFrame(
        java.util.List.of(Row(json)),
        StructType(Seq(StructField("value", StringType)))
      )
      .coalesce(1)
      .write
      .mode("errorifexists")
      .text(s"$out/run")
    RunRecord(json, converged = true, fit.fingerprint, outputs)
  }
}
