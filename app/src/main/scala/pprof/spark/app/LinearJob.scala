package pprof.spark.app

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import pprof.spark.engine.linear._

/** The linear fixed-effect model as a Spark job (docs/spec/linear/job-and-python.md): reads a run specification
  * (version 1, `model` `linear`), fits the model, and writes each requested output to its own directory under
  * `outputs.path` (never overwriting), the run record last.
  *
  * Usage: `LinearJob --spec <path>`, read through Spark so any file system works, or `LinearJob --spec-json <json>`.
  */
object LinearJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("pprof-spark LinearJob").getOrCreate()
    val json = specification(
      args,
      path => spark.read.option("wholetext", "true").text(path).collect().head.getString(0)
    )
    println(run(spark, LinearRunSpec.parse(json), json).json)
  }

  def specification(args: Array[String], read: String => String): String = args.toSeq match {
    case Seq("--spec", path)      => read(path)
    case Seq("--spec-json", json) => json
    case _                        =>
      throw new IllegalArgumentException("usage: LinearJob --spec <path> | --spec-json <json>")
  }

  /** `LinearFE.summary`'s tests as a table (slice 3a §4's columns). */
  def summaryTable(spark: SparkSession, tests: Vector[LinearTest]): DataFrame =
    spark.createDataFrame(
      tests
        .map(t =>
          Row(t.feature, t.estimate, t.standardError, t.statistic, t.pValue, t.lower, t.upper)
        )
        .asJava,
      StructType(
        StructField("feature", StringType, nullable = false) +:
          Seq("estimate", "std_error", "stat", "p_value", "ci_lower", "ci_upper")
            .map(name => StructField(name, DoubleType, nullable = false))
      )
    )

  def run(spark: SparkSession, spec: LinearRunSpec, specJson: String): RunRecord = {
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
    val fit = timed("fit")(LinearFE.fit(df, spec.spec, spec.options))
    val o = spec.outputs
    if (o.fit) timed("write fit") {
      LinearFitIO.save(spark, fit, s"$out/fit")
      written += s"$out/fit"
    }
    if (o.providers) timed("providers")(write(fit.providers, "providers"))
    o.summary.foreach(s =>
      timed("summary")(
        write(
          summaryTable(spark, LinearFE.summary(fit, s.level, s.nullValue, s.alternative)),
          "summary"
        )
      )
    )
    o.providerTests.foreach(t =>
      timed("provider tests")(
        write(
          LinearProviderTests.test(fit, t.reference, t.alternative, t.level, t.critical),
          "provider_tests"
        )
      )
    )
    o.effectIntervals.foreach(level =>
      timed("effect intervals")(
        write(LinearProviderTests.gammaIntervals(fit, level), "effect_intervals")
      )
    )
    o.measures.foreach(m =>
      timed("measures") {
        LinearStandardization.measures(df, fit, m.kinds, m.reference).foreach {
          case (kind, table) =>
            write(table, s"measures/$kind")
        }
      }
    )
    o.measureIntervals.foreach(m =>
      timed("measure intervals") {
        LinearStandardization
          .intervals(df, fit, m.kinds, m.reference, m.alternative, m.level)
          .foreach { case (kind, table) =>
            write(table, s"measure_intervals/$kind")
          }
      }
    )
    if (o.predictions) timed("predictions")(write(LinearFE.predict(df, fit), "predictions"))
    val outputs = written.result()
    val mapper = new ObjectMapper()
    val record = mapper.createObjectNode()
    record.put("model", "linear")
    record.set("specification", mapper.readTree(specJson))
    val options = record.putObject("fitOptions")
    options.put("varianceOption", fit.options.varianceOption)
    options.put("targetBlockBytes", fit.options.blocks.targetBlockBytes)
    options.put("maxGroupsOnDriver", fit.options.blocks.maxGroupsOnDriver)
    options.put("driverBudgetBytes", fit.options.blocks.driverBudgetBytes)
    options.put("storageLevel", fit.options.blocks.storageLevel)
    val software = record.putObject("software")
    fit.software.toFields.foreach { case (k, v) => software.put(k, v) }
    record.put("fingerprint", fit.fingerprint)
    record.put("rows", fit.rows)
    record.put("providers", fit.providerCount)
    record.put("features", fit.spec.features.size)
    record.put("degreesOfFreedom", fit.degreesOfFreedom)
    record.put("sigma", fit.sigma)
    record.put("rss", fit.rss)
    record.put("r2", fit.r2)
    record.put("loglik", fit.loglik)
    record.put("aic", fit.aic)
    record.put("bic", fit.bic)
    record.put("condition", fit.condition)
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
