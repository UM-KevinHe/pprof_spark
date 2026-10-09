package pprof.spark.app

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import pprof.spark.engine.layout.GroupKey
import pprof.spark.engine.logistic._

/** The three-stage model as a Spark job (docs/spec/logistic/three-stage-pipeline.md §5): reads a run specification
  * (version 1, `model` `three-stage`), fits the pipeline, and writes the requested outputs under `outputs.path` as
  * Parquet (`fit` through [[ThreeStageFitIO]]); `run`, written last, holds the run record. Existing outputs are
  * never overwritten.
  */
object ThreeStageJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("pprof-spark ThreeStageJob").getOrCreate()
    val json = specification(
      args,
      path => spark.read.option("wholetext", "true").text(path).collect().head.getString(0)
    )
    println(run(spark, ThreeStageRunSpec.parse(json), json).json)
  }

  def specification(args: Array[String], read: String => String): String = args.toSeq match {
    case Seq("--spec", path)      => read(path)
    case Seq("--spec-json", json) => json
    case _                        =>
      throw new IllegalArgumentException("usage: ThreeStageJob --spec <path> | --spec-json <json>")
  }

  private def keyType(keys: Vector[GroupKey]): DataType =
    if (keys.headOption.exists(_.isInstanceOf[GroupKey.Text])) StringType else LongType

  /** Stage 3's effects with stage 2's BLUPs and starts, in provider key order. */
  def providersTable(spark: SparkSession, fit: ThreeStageFit): DataFrame = {
    val s2 = fit.stage2
    val s3 = fit.stage3
    spark.createDataFrame(
      s3.providers.indices.map { j =>
        Row(
          GroupKey.value(s3.providers(j)),
          s3.gamma(j),
          s2.blupProviders(j),
          s2.start(j),
          s3.held.nonEmpty && s3.held(j)
        )
      }.asJava,
      StructType(
        Seq(
          StructField(fit.spec.provider, keyType(s3.providers), nullable = false),
          StructField("gamma", DoubleType, nullable = false),
          StructField("stage2_blup", DoubleType, nullable = false),
          StructField("stage3_start", DoubleType, nullable = false),
          StructField("held_at_bound", BooleanType, nullable = false)
        )
      )
    )
  }

  /** The clusters' posterior moments with stage 2's BLUPs, in cluster key order. */
  def clustersTable(spark: SparkSession, fit: ThreeStageFit): DataFrame = {
    val s3 = fit.stage3
    spark.createDataFrame(
      s3.clusters.indices
        .map(h =>
          Row(
            GroupKey.value(s3.clusters(h)),
            s3.alphaMean(h),
            s3.alphaVar(h),
            fit.stage2.blupClusters(h)
          )
        )
        .asJava,
      StructType(
        Seq(
          StructField(fit.spec.cluster, keyType(s3.clusters), nullable = false),
          StructField("alpha_mean", DoubleType, nullable = false),
          StructField("alpha_var", DoubleType, nullable = false),
          StructField("stage2_blup", DoubleType, nullable = false)
        )
      )
    )
  }

  def run(spark: SparkSession, spec: ThreeStageRunSpec, specJson: String): RunRecord = {
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
    val fit = timed("fit")(ThreeStagePipeline.fit(df, spec.spec, spec.options))
    val records = fit.preparation.data
    val o = spec.outputs
    if (o.fit) timed("write fit") {
      ThreeStageFitIO.save(spark, fit, s"$out/fit")
      written += s"$out/fit"
    }
    if (o.providers) timed("providers")(write(providersTable(spark, fit), "providers"))
    if (o.clusters) timed("clusters")(write(clustersTable(spark, fit), "clusters"))
    o.tests.zipWithIndex.foreach { case (t, i) =>
      timed(s"tests $i") {
        write(
          ThreeStagePipeline.test(
            records,
            spec.spec,
            fit.stage3,
            t.method,
            t.reference,
            t.alternative,
            t.level,
            t.critical,
            nResample = t.nResample,
            seed = t.seed
          ),
          s"tests/$i-${t.method}"
        )
      }
    }
    o.measures.foreach { m =>
      timed("measures") {
        ThreeStagePipeline
          .measures(records, fit.cells, spec.spec, fit.stage3, m.kinds, m.reference)
          .toSeq
          .sortBy(_._1)
          .foreach { case (kind, table) =>
            write(table, s"measures/$kind")
          }
      }
    }
    o.intervals.zipWithIndex.foreach { case (r, i) =>
      timed(s"intervals $i") {
        ThreeStagePipeline
          .intervals(
            records,
            fit.cells,
            spec.spec,
            fit.stage3,
            r.option,
            r.kinds,
            r.measure,
            r.alternative,
            r.level,
            r.method,
            r.reference
          )
          .toSeq
          .sortBy(_._1)
          .foreach { case (key, table) => write(table, s"intervals/$i-$key") }
      }
    }
    o.sensitivity.foreach { s =>
      timed("sensitivity") {
        val result = ThreeStagePipeline.sigmaSensitivity(
          records,
          fit.cells,
          spec.spec,
          fit.stage2,
          s.level,
          s.method,
          s.alternative,
          s.testLevel,
          spec.options.stage2,
          spec.options.stage3
        )
        write(result.flags, "sensitivity/flags")
        write(
          spark.createDataFrame(
            java.util.List.of(Row(result.lower, result.estimate, result.upper)),
            StructType(
              Seq("lower", "estimate", "upper").map(StructField(_, DoubleType, nullable = false))
            )
          ),
          "sensitivity/sigma"
        )
        Seq("lower", "estimate", "upper").foreach(at =>
          write(result.tests(at), s"sensitivity/tests/$at")
        )
      }
    }
    if (o.fitted)
      timed("fitted")(write(ThreeStage.fitted(records, spec.spec, fit.stage3), "fitted"))
    val outputs = written.result()
    val mapper = new ObjectMapper()
    val record = mapper.createObjectNode()
    record.put("model", "three-stage")
    record.set("specification", mapper.readTree(specJson))
    val software = record.putObject("software")
    fit.preparation.stage1.software.toFields.foreach { case (k, v) => software.put(k, v) }
    record.put("fingerprint", fit.preparation.stage1.fingerprint)
    record.put("providers", fit.stage3.providers.size)
    record.put("clusters", fit.stage3.clusters.size)
    record.put("excludedProviders", fit.preparation.excluded.size)
    record.put("cells", fit.cells.cells.length)
    val s2 = record.putObject("stage2")
    s2.put("sigmaProvider", fit.stage2.sigmaProvider)
    s2.put("sigmaCluster", fit.stage2.sigmaCluster)
    s2.put("intercept", fit.stage2.intercept)
    s2.put("deviance", fit.stage2.deviance)
    s2.put("converged", fit.stage2.converged)
    s2.put("iterations", fit.stage2.iterations)
    val s3 = record.putObject("stage3")
    s3.put("converged", fit.stage3.converged)
    s3.put("iterations", fit.stage3.iterations)
    s3.put("criterion", fit.stage3.criterion)
    val converged = fit.preparation.stage1.converged && fit.stage2.converged && fit.stage3.converged
    record.put("converged", converged)
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
    RunRecord(json, converged, fit.preparation.stage1.fingerprint, outputs)
  }
}
