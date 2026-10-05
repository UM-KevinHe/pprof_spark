package pprof.spark.testkit

class LocalSparkSuite extends munit.FunSuite {

  test("SparkConf is loaded from spark-core, not from spark-connect-shims") {
    LocalSpark.requireSparkCoreClasses()
  }
}
