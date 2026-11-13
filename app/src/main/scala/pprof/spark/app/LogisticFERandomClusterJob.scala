package pprof.spark.app

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import pprof.spark.engine.logistic._

/** The random-cluster model as a Spark job (docs/spec/logistic/fe-random-cluster-job.md): reads a run specification
  * (version 1, `model` `fe-random-cluster`), fits, and writes each requested output under `outputs.path`, never
  * overwriting, the run record last.
  *
  * Usage: `LogisticFERandomClusterJob --spec <path>`, read through Spark, or `--spec-json <json>`.
  */
object LogisticFERandomClusterJob {

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().appName("pprof-spark LogisticFERandomClusterJob").getOrCreate()
    val json = specification(
      args,
      path => spark.read.option("wholetext", "true").text(path).collect().head.getString(0)
    )
    println(run(spark, LogisticFERandomClusterRunSpec.parse(json), json).json)
  }

  def specification(args: Array[String], read: String => String): String = args.toSeq match {
    case Seq("--spec", path)      => read(path)
    case Seq("--spec-json", json) => json
    case _                        =>
      throw new IllegalArgumentException(
        "usage: LogisticFERandomClusterJob --spec <path> | --spec-json <json>"
      )
  }

  def run(
      spark: SparkSession,
      spec: LogisticFERandomClusterRunSpec,
      specJson: String
  ): RunRecord = {
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
    val (source, stage1) = timed("source") {
      spec.source match {
        case FERandomClusterGivenSpec(beta, sigma, start) =>
          (LogisticFERandomClusterSource.Given(beta, sigma, start), None)
        case FERandomClusterStagesSpec(stage1Path, stage2Path) =>
          val s1 = LogisticFitIO.load(spark, stage1Path)
          val s2 = ThreeStageFitIO.load(spark, stage2Path).stage2
          (LogisticFERandomClusterSource.Stages(s1, s2), Some(s1))
      }
    }
    val fit = timed("fit")(LogisticFERandomCluster.fit(df, spec.spec, source, spec.options))
    val o = spec.outputs
    if (o.fit) timed("write fit") {
      LogisticFERandomClusterFitIO.save(spark, fit, s"$out/fit")
      written += s"$out/fit"
    }
    val api = pprof.spark.app.python.PythonApi
    if (o.providers)
      timed("providers")(write(api.feRandomClusterProviders(spark, fit), "providers"))
    if (o.clusters) timed("clusters")(write(api.feRandomClusterClusters(spark, fit), "clusters"))
    o.summary.foreach { case (level, nullValue, alternative) =>
      timed("summary")(
        write(
          LogisticREJob.summaryFrame(
            spark,
            LogisticFERandomCluster
              .summary(fit, stage1, level, nullValue, alternative)
              .map(t =>
                LogisticRETest(
                  t.feature,
                  t.estimate,
                  t.standardError,
                  t.statistic,
                  t.pValue,
                  t.lower,
                  t.upper
                )
              )
          ),
          "summary"
        )
      )
    }
    o.providerTests.foreach { t =>
      timed(s"provider tests (${t.method})")(
        write(
          LogisticFERandomCluster.test(
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
    o.gammaIntervals.foreach { g =>
      timed("gamma intervals")(
        write(
          LogisticFERandomCluster.intervals(
            fit,
            "gamma",
            Seq("indirect"),
            Seq("ratio"),
            g.alternative,
            g.level,
            g.method,
            g.reference
          )("gamma_ci"),
          "gamma_intervals"
        )
      )
    }
    o.measures.foreach { case (kinds, reference) =>
      timed("measures")(LogisticFERandomCluster.measures(fit, kinds, reference).foreach {
        case (k, t) => write(t, s"measures/$k")
      })
    }
    o.measureIntervals.foreach { m =>
      timed("measure intervals")(
        LogisticFERandomCluster
          .intervals(fit, "SM", m.kinds, m.measures, m.alternative, m.level, m.method, m.reference)
          .foreach { case (k, t) => write(t, s"measure_intervals/$k") }
      )
    }
    if (o.fitted) timed("fitted")(write(LogisticFERandomCluster.fitted(fit), "fitted"))
    val outputs = written.result()
    val mapper = new ObjectMapper()
    val record = mapper.createObjectNode()
    def number(key: String, value: Double, into: ObjectNode = record): Unit =
      if (value.isInfinite || value.isNaN) into.putNull(key) else into.put(key, value)
    record.put("model", "fe-random-cluster")
    record.set("specification", mapper.readTree(specJson))
    val options = record.putObject("fitOptions")
    options.put("estimator", fit.options.estimator)
    options.put("convergenceCriterion", fit.options.convergenceCriterion)
    options.put("nNodes", fit.options.nNodes)
    options.put("maxIter", fit.options.maxIter)
    number("tol", fit.options.tol, options)
    number("bound", fit.options.bound, options)
    options.put("boundMode", fit.options.boundMode)
    options.put("path", fit.options.path)
    record.put("source", if (stage1.isDefined) "stages" else "given")
    val software = record.putObject("software")
    fit.software.foreach(_.toFields.foreach { case (k, v) => software.put(k, v) })
    record.put("rows", fit.rows)
    record.put("fingerprint", fit.fingerprint)
    record.put("providers", fit.providers.size)
    record.put("clusters", fit.stage3.clusters.size)
    number("sigma", fit.sigma)
    number("logLikelihood", fit.stage3.loglik)
    record.put("iterations", fit.stage3.iterations)
    number("criterion", fit.stage3.criterion)
    record.put("converged", fit.stage3.converged)
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
    RunRecord(json, fit.stage3.converged, fit.fingerprint, outputs)
  }
}
