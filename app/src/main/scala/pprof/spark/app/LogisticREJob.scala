package pprof.spark.app

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import pprof.spark.engine.logistic._

/** The logistic random-intercept model as a Spark job (docs/spec/logistic/random-effect-job.md): reads a run
  * specification (version 1, `model` `logistic-re`), fits the model, and writes each requested output to its own
  * directory under `outputs.path` (never overwriting), the run record last.
  *
  * Usage: `LogisticREJob --spec <path>`, read through Spark, or `LogisticREJob --spec-json <json>`.
  */
object LogisticREJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("pprof-spark LogisticREJob").getOrCreate()
    val json = specification(
      args,
      path => spark.read.option("wholetext", "true").text(path).collect().head.getString(0)
    )
    println(run(spark, LogisticRERunSpec.parse(json), json).json)
  }

  def specification(args: Array[String], read: String => String): String = args.toSeq match {
    case Seq("--spec", path)      => read(path)
    case Seq("--spec-json", json) => json
    case _                        =>
      throw new IllegalArgumentException("usage: LogisticREJob --spec <path> | --spec-json <json>")
  }

  /** `LogisticREInference.summary` as a table: feature, estimate, std_error, z_value, p_value, ci_lower, ci_upper. */
  def summaryFrame(spark: SparkSession, tests: Vector[LogisticRETest]): DataFrame =
    spark.createDataFrame(
      tests
        .map(t =>
          Row(t.feature, t.estimate, t.standardError, t.statistic, t.pValue, t.lower, t.upper)
        )
        .asJava,
      StructType(
        StructField("feature", StringType, nullable = false) +:
          Seq("estimate", "std_error", "z_value", "p_value", "ci_lower", "ci_upper")
            .map(name => StructField(name, DoubleType, nullable = false))
      )
    )

  /** σ's profile interval as a table of one row: sigma, lower, upper. */
  def profileFrame(spark: SparkSession, profile: LogisticREProfile): DataFrame =
    spark.createDataFrame(
      java.util.List.of(Row(profile.sigma, profile.lower, profile.upper)),
      StructType(
        Seq("sigma", "lower", "upper").map(name => StructField(name, DoubleType, nullable = false))
      )
    )

  def run(spark: SparkSession, spec: LogisticRERunSpec, specJson: String): RunRecord = {
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
    val fit = timed("fit")(LogisticRE.fit(df, spec.spec, spec.options))
    val o = spec.outputs
    if (o.fit) timed("write fit") {
      LogisticREFitIO.save(spark, fit, s"$out/fit")
      written += s"$out/fit"
    }
    if (o.providers) timed("providers")(write(fit.providers, "providers"))
    o.summary.foreach(level =>
      timed("summary")(
        write(summaryFrame(spark, LogisticREInference.summary(fit, level)), "summary")
      )
    )
    o.providerTests.foreach { t =>
      timed(s"provider tests (${t.method})")(
        write(
          LogisticREInference.test(
            df,
            fit,
            t.method,
            t.reference,
            t.alternative,
            t.level,
            t.critical,
            None,
            t.nResample,
            t.seed
          ),
          s"provider_tests/${t.method}"
        )
      )
    }
    o.alphaIntervals.foreach(level =>
      timed("alpha intervals")(
        write(LogisticREInference.alphaIntervals(fit, level), "alpha_intervals")
      )
    )
    o.measures.foreach(m =>
      timed("measures") {
        LogisticREInference.measures(df, fit, m.kinds, m.reference).foreach { case (kind, table) =>
          write(table, s"measures/$kind")
        }
      }
    )
    o.measureIntervals.foreach(m =>
      timed("measure intervals") {
        LogisticREInference
          .smIntervals(df, fit, m.kinds, m.measures, m.reference, m.alternative, m.level)
          .foreach { case (key, table) =>
            write(table, s"measure_intervals/$key")
          }
      }
    )
    o.profile.foreach(level =>
      timed("profile")(
        write(profileFrame(spark, LogisticREInference.profileSigma(df, fit, level)), "profile")
      )
    )
    o.predictions.foreach { case (useRe, kind) =>
      timed("predictions")(
        write(LogisticRE.predict(df, fit, useRe, withOffset = true, kind), "predictions")
      )
    }
    if (o.fitted) timed("fitted")(write(LogisticRE.fitted(df, fit), "fitted"))
    val outputs = written.result()
    val mapper = new ObjectMapper()
    val record = mapper.createObjectNode()
    def number(key: String, value: Double, into: ObjectNode = record): Unit =
      if (value.isInfinite || value.isNaN) into.putNull(key) else into.put(key, value)
    record.put("model", "logistic-re")
    record.set("specification", mapper.readTree(specJson))
    val options = record.putObject("fitOptions")
    options.put("stage2", fit.options.nagq1)
    options.put("includeIntercept", fit.options.includeIntercept)
    number("sigmaUpper", fit.options.sigmaUpper, options)
    options.put("maxIterations", fit.options.maxIterations)
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
      "deviance" -> fit.deviance,
      "loglik" -> fit.loglik,
      "aic" -> fit.aic,
      "bic" -> fit.bic
    )
      .foreach { case (key, value) => number(key, value) }
    val search = record.putObject("search")
    search.put("stage1Evaluations", fit.search.stage1Evaluations)
    search.put("stage2Evaluations", fit.search.stage2Evaluations)
    number("projectedGradient", fit.search.projectedGradient, search)
    search.put("converged", fit.search.converged)
    fit.search.boundary.fold(search.putNull("boundary"))(b => search.put("boundary", b))
    val warnings = record.putArray("warnings")
    fit.warnings.foreach(w => warnings.add(w))
    record.put("converged", fit.search.converged)
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
    RunRecord(json, fit.search.converged, fit.fingerprint, outputs)
  }
}
