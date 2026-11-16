package pprof.spark.engine

import scala.collection.mutable

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}

import pprof.spark.engine.backend.Operations
import pprof.spark.engine.cox.{CoxFixtures, CoxOptions, CoxPH, Ties}
import pprof.spark.engine.logistic.{LogisticFE, LogisticFixtures}
import pprof.spark.testkit.{SparkSuite, TestSessions}

/** Operation tags on the jobs of a fit (OI-108; docs/spec/performance/plan.md, step 0). */
class OperationTagsSuite extends SparkSuite {

  private def operations(tags: String): Set[String] =
    tags
      .split(",")
      .map(_.trim)
      .flatMap { t =>
        val at = t.indexOf(Operations.Prefix)
        if (at < 0) None else Some(t.substring(at))
      }
      .toSet

  /** The operation tags of each job `body` starts: Classic only, since a listener needs the SparkContext. */
  private def jobTags(body: => Unit): Vector[Set[String]] = {
    val seen = mutable.ArrayBuffer.empty[Set[String]]
    val listener = new SparkListener {
      override def onJobStart(job: SparkListenerJobStart): Unit = seen.synchronized {
        seen += Option(job.properties)
          .flatMap(p => Option(p.getProperty("spark.job.tags")))
          .fold(Set.empty[String])(operations)
      }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      body
      var stable = 0
      var rounds = 0
      while (stable < 4 && rounds < 120) {
        val before = seen.synchronized(seen.size)
        Thread.sleep(250L)
        if (seen.synchronized(seen.size) == before) stable += 1 else stable = 0
        rounds += 1
      }
    } finally spark.sparkContext.removeSparkListener(listener)
    seen.synchronized(seen.toVector)
  }

  private def leftover: Set[String] = spark.getTags().filter(_.contains(Operations.Prefix))

  private def check(jobs: Vector[Set[String]], expected: Set[String]): Unit = {
    assert(jobs.nonEmpty)
    assert(
      jobs.forall(_.size == 1),
      s"jobs without exactly one operation tag: ${jobs.filter(_.size != 1)}"
    )
    val seen = jobs.flatten.toSet
    expected.foreach(op =>
      assert(seen(op), s"$op missing from ${seen.toSeq.sorted.mkString(", ")}")
    )
    assert(leftover.isEmpty, leftover.mkString(", "))
  }

  test("every job of a Cox fit carries one operation tag, and none outlives the fit") {
    assume(!TestSessions.isConnect, "a listener needs Classic Spark")
    val jobs = jobTags {
      CoxPH.fit(
        CoxFixtures.frame(spark, "rc-stratified"),
        CoxFixtures.spec("rc-stratified"),
        CoxOptions(ties = Ties.Efron)
      )
      ()
    }
    check(
      jobs,
      Set(
        "pprof.cox.validate",
        "pprof.layout.group-sizes",
        "pprof.cox.build",
        "pprof.cox.summarize",
        "pprof.cox.evaluate"
      )
    )
  }

  test("every job of a logistic fit carries one operation tag, and none outlives the fit") {
    assume(!TestSessions.isConnect, "a listener needs Classic Spark")
    val jobs = jobTags {
      LogisticFE.fit(LogisticFixtures.frame(spark, "lfe-base"), LogisticFixtures.spec("lfe-base"))
      ()
    }
    check(
      jobs,
      Set(
        "pprof.logistic.validate",
        "pprof.layout.group-sizes",
        "pprof.logistic.build",
        "pprof.logistic.summarize",
        "pprof.logistic.pass-a",
        "pprof.logistic.pass-b",
        "pprof.logistic.variance",
        "pprof.logistic.provider-variance"
      )
    )
  }

  test("no operation tag outlives a fit, under Classic and Spark Connect") {
    CoxPH.fit(CoxFixtures.frame(spark, "tiny-ties"), CoxFixtures.spec("tiny-ties"))
    LogisticFE.fit(LogisticFixtures.frame(spark, "lfe-base"), LogisticFixtures.spec("lfe-base"))
    assert(leftover.isEmpty, leftover.mkString(", "))
  }

  test("an action that fails removes its tag") {
    intercept[IllegalStateException](
      Operations.tagged(spark, "test.failure")(throw new IllegalStateException("x"))
    )
    assert(leftover.isEmpty, leftover.mkString(", "))
  }
}
