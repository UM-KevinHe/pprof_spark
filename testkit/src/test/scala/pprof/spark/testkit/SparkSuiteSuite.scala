package pprof.spark.testkit

/** The base class of the Spark suites. */
class SparkSuiteSuite extends SparkSuite {

  test("each suite's end clears the shared session's cache (OI-83)") {
    spark.range(3).createOrReplaceTempView("oi83")
    try {
      spark.catalog.cacheTable("oi83")
      assert(spark.catalog.isCached("oi83"))
      new SparkSuite {}.afterAll()
      assert(!spark.catalog.isCached("oi83"))
    } finally spark.catalog.dropTempView("oi83")
  }
}
