package pprof.spark.engine.logistic

import java.nio.file.Files

import org.apache.spark.sql.functions.{col, lit}
import pprof.spark.engine.cox.CoxFitIO
import pprof.spark.engine.logistic.LogisticFixtures._
import pprof.spark.testkit.SparkSuite

/** Persistence of logistic fits (Phase 2a specification §8; §6.10, PERS-1 to PERS-3). */
class LogisticFitIOSuite extends SparkSuite {

  private def freshPath(): String =
    Files.createTempDirectory("pprof-logistic").resolve("model").toString

  private def bits(values: Seq[Double]): Seq[Long] =
    values.map(java.lang.Double.doubleToRawLongBits)

  private def assertSameFit(loaded: LogisticFit, saved: LogisticFit): Unit = {
    Seq[LogisticCoefficient => Double](_.estimate, _.standardError, _.z, _.pValue, _.lower, _.upper)
      .foreach(f => assertEquals(bits(loaded.coefficients.map(f)), bits(saved.coefficients.map(f))))
    assertEquals(bits(loaded.covariance), bits(saved.covariance))
    val (a, b) = (saved.providers, loaded.providers)
    assertEquals(b.keys.toSeq, a.keys.toSeq)
    Seq[LogisticProviders => Array[Double]](_.gamma, _.varGamma, _.varCaseMix, _.events, _.trials)
      .foreach(f => assertEquals(bits(f(b).toSeq), bits(f(a).toSeq)))
    assertEquals(b.records.toSeq, a.records.toSeq)
    assertEquals(b.atBound.toSeq, a.atBound.toSeq)
    def scalars(fit: LogisticFit) =
      bits(Seq(fit.loglik, fit.aic, fit.bic, fit.criterion, fit.lastStep, fit.trials, fit.events))
    assertEquals(scalars(loaded), scalars(saved))
    def steps(fit: LogisticFit) =
      fit.trace.map(s => (s.iteration, bits(Seq(s.loglik, s.step, s.betaChange)), s.evaluated))
    assertEquals(steps(loaded), steps(saved))
    assertEquals(
      loaded.copy(
        coefficients = saved.coefficients,
        covariance = saved.covariance,
        providers = saved.providers,
        loglik = saved.loglik,
        aic = saved.aic,
        bic = saved.bic,
        criterion = saved.criterion,
        lastStep = saved.lastStep,
        trials = saved.trials,
        events = saved.events,
        trace = saved.trace
      ),
      saved
    )
  }

  test("save and load round-trip every bit, with screened and degenerate providers (PERS-1)") {
    val name = "lfe-degenerate"
    val fit = LogisticFE.fit(frame(spark, name), spec(name))
    assert(fit.excluded.nonEmpty && fit.degenerateProviders == 2)
    val path = freshPath()
    LogisticFitIO.save(spark, fit, path)
    assertSameFit(LogisticFitIO.load(spark, path), fit)
  }

  test("text provider keys and binomial trials round-trip") {
    val name = "lfe-binomial"
    val df = frame(spark, name).withColumn("provider", col("provider").cast("string"))
    val fit = LogisticFE.fit(df, spec(name))
    assert(fit.providers.keys.head.isInstanceOf[pprof.spark.engine.layout.GroupKey.Text])
    val path = freshPath()
    LogisticFitIO.save(spark, fit, path)
    val loaded = LogisticFitIO.load(spark, path)
    assertSameFit(loaded, fit)
    assertEquals(LogisticFE.providerTable(spark, loaded).collect().length, fit.providers.size)
  }

  test(
    "an existing path is never overwritten; another kind or version fails, naming both (PERS-2)"
  ) {
    val name = "lfe-base"
    val fit = LogisticFE.fit(frame(spark, name), spec(name))
    val path = freshPath()
    LogisticFitIO.save(spark, fit, path)
    intercept[Exception](LogisticFitIO.save(spark, fit, path))
    val asCox = intercept[IllegalArgumentException](CoxFitIO.load(spark, path))
    assert(asCox.getMessage.contains(LogisticFitIO.Kind), asCox.getMessage)
    val other = freshPath()
    spark.read
      .schema(LogisticFitIO.MetadataSchema)
      .json(s"$path/metadata")
      .withColumn("formatVersion", lit(99))
      .coalesce(1)
      .write
      .json(s"$other/metadata")
    val failure = intercept[IllegalArgumentException](LogisticFitIO.load(spark, other))
    assert(
      failure.getMessage.contains("format version 99") && failure.getMessage.contains(
        "versions 1 to 3"
      )
    )
    val version1 = freshPath()
    spark.read
      .schema(LogisticFitIO.MetadataSchema)
      .json(s"$path/metadata")
      .withColumn("formatVersion", lit(1))
      .withColumn("correlationThresholdBits", lit(null).cast("long"))
      .coalesce(1)
      .write
      .json(s"$version1/metadata")
    spark.read.parquet(s"$path/providers").coalesce(1).write.parquet(s"$version1/providers")
    assertEquals(LogisticFitIO.load(spark, version1).options.correlationThreshold, 0.9)
    assertSameFit(LogisticFitIO.load(spark, path), fit)
  }

  test("clustered fits keep their robust variances, cluster column and AUC (format version 3)") {
    val name = "lfe-clustered"
    val fit = LogisticFE.fit(frame(spark, name), spec(name))
    assert(fit.robustCovariance.isDefined && fit.auc.isDefined && fit.clusters > 0L)
    val path = freshPath()
    LogisticFitIO.save(spark, fit, path)
    val loaded = LogisticFitIO.load(spark, path)
    assertSameFit(loaded, fit)
    assertEquals(bits(loaded.robustCovariance.get), bits(fit.robustCovariance.get))
    assertEquals(
      bits(loaded.providers.robustVarCaseMix.toSeq),
      bits(fit.providers.robustVarCaseMix.toSeq)
    )
    assertEquals(
      bits(loaded.providers.robustVarFixedBeta.toSeq),
      bits(fit.providers.robustVarFixedBeta.toSeq)
    )
    assertEquals(bits(loaded.auc.toSeq), bits(fit.auc.toSeq))
  }
}
