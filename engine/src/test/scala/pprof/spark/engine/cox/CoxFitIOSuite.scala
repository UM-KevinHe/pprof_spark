package pprof.spark.engine.cox

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import pprof.spark.testkit.{Fixtures, SparkSuite}

/** Persistence of Cox fits (§6.10, PERS-1 to PERS-3) under Classic Spark and Spark Connect. */
class CoxFitIOSuite extends SparkSuite {

  private def input(name: String): (DataFrame, CoxSpec) = {
    val info = Fixtures.json(s"cox/$name/case.json")
    val node = info.get("features")
    val features = (0 until node.size).map(i => node.get(i).asText())
    val table = Fixtures.csv(s"cox/$name/input.csv")
    val names = Seq("id", "stratum", "time", "event", "weight", "offset") ++ features
    val index = names.map(table.columns.indexOf(_))
    val rows = table.rows.map { r =>
      Row.fromSeq(
        Seq[Any](r(index(0)).toLong, r(index(1)).toLong, r(index(2)), r(index(3)).toInt) ++
          index.drop(4).map(r(_))
      )
    }
    val schema = StructType(
      Seq(
        StructField("id", LongType),
        StructField("stratum", LongType),
        StructField("time", DoubleType),
        StructField("event", IntegerType),
        StructField("weight", DoubleType),
        StructField("offset", DoubleType)
      ) ++ features.map(StructField(_, DoubleType))
    )
    def role(key: String, column: String) = if (info.get(key).asBoolean) Some(column) else None
    val spec = CoxSpec(
      "time",
      "event",
      features,
      role("stratified", "stratum"),
      Some("id"),
      role("weighted", "weight"),
      role("offset", "offset")
    )
    (spark.createDataFrame(rows.asJava, schema), spec)
  }

  private def freshPath(): String = Files.createTempDirectory("pprof-cox").resolve("model").toString

  private def allBits(fit: CoxFit): Seq[Long] =
    (fit.estimates ++ fit.standardErrors ++
      fit.coefficients.flatMap(c => Seq(c.z, c.pValue, c.lower, c.upper)) ++
      fit.covariance ++ fit.information ++ Seq(fit.logLikelihood, fit.logLikelihoodNull) ++
      fit.iterationLog.flatMap(i => i.beta :+ i.value) ++
      Seq(fit.options.eps, fit.options.aliasTolerance, fit.options.confidenceLevel))
      .map(java.lang.Double.doubleToRawLongBits)

  test("save and load round-trip every bit, both tie methods, weights and offsets (PERS-1)") {
    val (df, spec) = input("rc-stratified-weights-offset")
    for (ties <- Ties.all) {
      val fit = CoxPH.fit(df, spec, CoxOptions(ties = ties))
      val path = freshPath()
      CoxFitIO.save(spark, fit, path)
      val loaded = CoxFitIO.load(spark, path)
      assertEquals(allBits(loaded), allBits(fit))
      assertEquals(loaded, fit)
      assertEquals(loaded.featureStatus, "experimental")
      assertEquals(loaded.ties, ties.name)
    }
  }

  test("a fit that stopped at the iteration cap keeps its warning, message and log") {
    val (df, spec) = input("rc-stratified")
    val fit = CoxPH.fit(df, spec, CoxOptions(maxIterations = 1))
    val path = freshPath()
    CoxFitIO.save(spark, fit, path)
    val loaded = CoxFitIO.load(spark, path)
    assertEquals(loaded, fit)
    assert(!loaded.converged)
    assertEquals(loaded.warnings.size, 1)
  }

  test("an existing path is never overwritten") {
    val (df, spec) = input("tiny-ties")
    val first = CoxPH.fit(df, spec)
    val path = freshPath()
    CoxFitIO.save(spark, first, path)
    intercept[Exception](
      CoxFitIO.save(spark, CoxPH.fit(df, spec, CoxOptions(ties = Ties.Efron)), path)
    )
    assertEquals(CoxFitIO.load(spark, path), first)
  }

  test("another format version fails with a message that names both versions (PERS-2)") {
    val (df, spec) = input("tiny-ties")
    val path = freshPath()
    CoxFitIO.save(spark, CoxPH.fit(df, spec), path)
    val parts = Files
      .list(Path.of(path, "metadata"))
      .iterator
      .asScala
      .filter(_.getFileName.toString.startsWith("part-"))
      .toSeq
    assertEquals(parts.size, 1)
    val text = new String(Files.readAllBytes(parts.head), UTF_8)
    assert(text.contains("\"formatVersion\":1,"), text.take(80))
    Files.write(
      parts.head,
      text.replace("\"formatVersion\":1,", "\"formatVersion\":99,").getBytes(UTF_8)
    )
    // Hadoop's local file system keeps a checksum beside each file; drop it with the edit.
    Files.deleteIfExists(parts.head.resolveSibling(s".${parts.head.getFileName}.crc"))
    val e = intercept[IllegalArgumentException](CoxFitIO.load(spark, path))
    assert(e.getMessage.contains("format version 99"), e.getMessage)
    assert(e.getMessage.contains(s"format version ${CoxFitIO.FormatVersion}"), e.getMessage)
  }
}
