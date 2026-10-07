package pprof.spark.app

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import pprof.spark.engine.cox.{CoxFitIO, CoxMeasures, CoxPH, CoxProviderTests}

/** What one run produced: the run record as JSON, and where each output went. */
final case class RunRecord(
    json: String,
    converged: Boolean,
    fingerprint: Long,
    outputs: Seq[String]
)

/** The `spark-submit` entry point for Cox provider-profiling runs (Phase 1d specification §5).
  *
  * Usage: `CoxJob --spec <path>`, the path read through Spark so any file system works, or
  * `CoxJob --spec-json <json>`. Each requested result goes to its own subdirectory of
  * `outputs.path` as Parquet (`fit` through [[CoxFitIO]], with the baseline when requested), and
  * `run` holds the run record: the specification, software versions, data fingerprint, fit
  * diagnostics and per-stage timings. Existing outputs are never overwritten.
  */
object CoxJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("pprof-spark CoxJob").getOrCreate()
    val json = specification(
      args,
      path => spark.read.option("wholetext", "true").text(path).collect().head.getString(0)
    )
    val record = run(spark, RunSpec.parse(json), json)
    println(record.json)
  }

  /** The run specification's JSON from the arguments; `read` reads a path. */
  def specification(args: Array[String], read: String => String): String = args.toSeq match {
    case Seq("--spec", path)      => read(path)
    case Seq("--spec-json", json) => json
    case _                        =>
      throw new IllegalArgumentException("usage: CoxJob --spec <path> | --spec-json <json>")
  }

  def run(spark: SparkSession, spec: RunSpec, specJson: String): RunRecord = {
    val timings = scala.collection.mutable.LinkedHashMap.empty[String, Long]
    def timed[A](stage: String)(body: => A): A = {
      val start = System.nanoTime()
      try body
      finally timings(stage) = (System.nanoTime() - start) / 1000000L
    }
    val out = spec.outputs.path.stripSuffix("/")
    val written = Vector.newBuilder[String]
    val df = timed("read") {
      val reader = spark.read.options(spec.input.options)
      spec.input.table.fold(reader.format(spec.input.format).load(spec.input.path.get))(t =>
        reader.table(t)
      )
    }
    val fit = timed("fit")(CoxPH.fit(df, spec.columns, spec.options))
    val baseline =
      if (spec.outputs.baseline) Some(timed("baseline")(CoxPH.baseline(df, fit))) else None
    try {
      if (spec.outputs.fit || baseline.isDefined) timed("write fit") {
        CoxFitIO.save(spark, fit, s"$out/fit", baseline)
        written += s"$out/fit"
      }
      if (spec.outputs.residuals) timed("residuals") {
        write(CoxPH.residuals(df, fit), s"$out/residuals")
        written += s"$out/residuals"
      }
      spec.outputs.measures.foreach { m =>
        timed("measures") {
          CoxMeasures.standardized(df, fit, m.provider, m.kinds).foreach { case (kind, table) =>
            write(table, s"$out/measures/${kind.name}")
            written += s"$out/measures/${kind.name}"
          }
        }
      }
      spec.outputs.tests.foreach { t =>
        timed("tests") {
          write(CoxProviderTests.test(df, fit, t.provider, t.method, t.level), s"$out/tests")
          written += s"$out/tests"
        }
      }
    } finally baseline.foreach(_.unpersist())
    val outputs = written.result()
    val mapper = new ObjectMapper()
    val record = mapper.createObjectNode()
    record.put("recordVersion", 1)
    record.set("specification", mapper.readTree(specJson))
    val software = record.putObject("software")
    fit.software.toFields.foreach { case (k, v) => software.put(k, v) }
    record.put("fingerprint", fit.fingerprint)
    record.put("converged", fit.converged)
    record.put("iterations", fit.iterations)
    record.put("observations", fit.observations)
    record.put("events", fit.events)
    val warnings = record.putArray("warnings")
    fit.warnings.foreach(w => warnings.add(w))
    val outputList = record.putArray("outputs")
    outputs.foreach(o => outputList.add(o))
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

  private def write(table: DataFrame, path: String): Unit = {
    table.write.mode("errorifexists").parquet(path)
    table.unpersist()
    ()
  }
}
