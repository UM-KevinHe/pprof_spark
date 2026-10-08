package pprof.spark.app

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import pprof.spark.engine.logistic.{
  LogisticFE,
  LogisticFitIO,
  LogisticProviderTests,
  LogisticStandardization,
  LogisticTest
}

/** The `spark-submit` entry point for logistic fixed-effect runs (Phase 2e specification §3).
  *
  * Usage: `LogisticJob --spec <path>`, read through Spark so any file system works, or
  * `LogisticJob --spec-json <json>`. Each requested result goes to its own subdirectory of
  * `outputs.path` (`fit` through [[LogisticFitIO]], tables as Parquet), and `run`, written last,
  * holds the run record. Existing outputs are never overwritten.
  */
object LogisticJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("pprof-spark LogisticJob").getOrCreate()
    val json = specification(
      args,
      path => spark.read.option("wholetext", "true").text(path).collect().head.getString(0)
    )
    val record = run(spark, LogisticRunSpec.parse(json), json)
    println(record.json)
  }

  /** The run specification's JSON from the arguments; `read` reads a path. */
  def specification(args: Array[String], read: String => String): String = args.toSeq match {
    case Seq("--spec", path)      => read(path)
    case Seq("--spec-json", json) => json
    case _                        =>
      throw new IllegalArgumentException("usage: LogisticJob --spec <path> | --spec-json <json>")
  }

  /** Covariate tests as a table, one row per covariate and method. */
  def testsTable(spark: SparkSession, tests: Seq[LogisticTest]): DataFrame = {
    val doubles = Seq("estimate", "standard_error", "statistic", "p_value", "ci_lower", "ci_upper")
    spark.createDataFrame(
      tests
        .map(t =>
          Row(
            t.feature,
            t.estimate,
            t.standardError,
            t.statistic,
            t.pValue,
            t.lower,
            t.upper,
            t.method
          )
        )
        .asJava,
      StructType(
        StructField("feature", StringType, nullable = false) +:
          doubles.map(StructField(_, DoubleType, nullable = false)) :+
          StructField("method", StringType, nullable = false)
      )
    )
  }

  def run(spark: SparkSession, spec: LogisticRunSpec, specJson: String): RunRecord = {
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
    val fit = timed("fit")(LogisticFE.fit(df, spec.columns, spec.options))
    val o = spec.outputs
    if (o.fit) timed("write fit") {
      LogisticFitIO.save(spark, fit, s"$out/fit")
      written += s"$out/fit"
    }
    if (o.providers) timed("providers")(write(LogisticFE.providerTable(spark, fit), "providers"))
    o.covariateTests.foreach { c =>
      timed("covariate tests") {
        val tests = c.methods.flatMap {
          case "wald" => LogisticFE.waldTests(fit, robust = c.robust)
          case method => LogisticFE.covariateTests(df, fit, method)
        }
        write(testsTable(spark, tests), "covariate_tests")
      }
    }
    o.providerTests.foreach { t =>
      timed("provider tests") {
        write(
          LogisticProviderTests.test(
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
          "provider_tests"
        )
      }
    }
    o.measures.foreach { m =>
      timed("measures") {
        LogisticStandardization
          .measures(df, fit, m.kinds, m.reference, None, m.extremeTrials, m.method)
          .toSeq
          .sortBy(_._1)
          .foreach { case (kind, table) => write(table, s"measures/$kind") }
      }
    }
    o.measureTests.zipWithIndex.foreach { case (t, i) =>
      timed(s"measure test $i") {
        write(
          LogisticStandardization.test(
            df,
            fit,
            t.measure,
            t.nullValue,
            t.transform,
            t.reference,
            t.variance,
            t.indirectVariance,
            t.alternative,
            t.level,
            t.critical,
            None,
            t.method
          ),
          s"measure_tests/$i-${t.measure}"
        )
      }
    }
    if (o.predictions) timed("predictions")(write(LogisticFE.predict(df, fit), "predictions"))
    val outputs = written.result()
    val mapper = new ObjectMapper()
    val record = mapper.createObjectNode()
    record.put("recordVersion", 1)
    record.put("model", "logistic")
    record.set("specification", mapper.readTree(specJson))
    val options = record.putObject("options")
    val opts = spec.options
    options.put("tol", opts.tol)
    options.put("maxIter", opts.maxIter)
    options.put("bound", opts.bound)
    options.put("backtrack", opts.backtrack)
    options.put("screen", opts.screen)
    options.put("minRecords", opts.minRecords)
    options.put("confidenceLevel", opts.confidenceLevel)
    options.put("correlationThreshold", opts.correlationThreshold)
    options.put("maxProvidersOnDriver", opts.maxProvidersOnDriver)
    val software = record.putObject("software")
    fit.software.toFields.foreach { case (k, v) => software.put(k, v) }
    record.put("fingerprint", fit.fingerprint)
    record.put("converged", fit.converged)
    record.put("iterations", fit.iterations)
    record.put("rows", fit.rows)
    record.put("trials", fit.trials)
    record.put("events", fit.events)
    record.put("providers", fit.providers.size)
    record.put("excludedProviders", fit.excluded.size)
    record.put("degenerateProviders", fit.degenerateProviders)
    record.put("clusters", fit.clusters)
    fit.auc.foreach(a => record.put("auc", a))
    val warnings = record.putArray("warnings")
    fit.warnings.foreach(w => warnings.add(w))
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
    RunRecord(json, fit.converged, fit.fingerprint, outputs)
  }
}
