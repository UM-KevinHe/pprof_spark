package pprof.spark.bench

import java.nio.file.Files

import com.fasterxml.jackson.databind.ObjectMapper
import pprof.spark.testkit.SparkSuite

/** The S1 smoke's runner (docs/parity/scale/s1-linear-re.md) on a small point: it generates the data, fits three
  * times, and reports every check passed; a second run reuses the data.
  */
class S1LinearRESuite extends SparkSuite {

  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(4, "min")

  test("a small point passes every check, and a second run reuses its data") {
    val path = Files.createTempDirectory("pprof-s1-linear-re").resolve("point").toString
    val point = S1LinearRE.Point(n = 20000L, m = 100, p = 4)
    val text = S1LinearRE.run(spark, path, point)
    println(s"S1 report: $text")
    val report = new ObjectMapper().readTree(text)
    assert(report.get("generated").asBoolean())
    report.get("checks").properties().forEach { check =>
      assert(check.getValue.get("passed").asBoolean(), s"${check.getKey}: ${check.getValue}")
    }
    assert(report.get("passed").asBoolean())
    assertEquals(report.get("fit").get("providers").asInt(), 100)
    assertEquals(report.get("fit").get("search (distributed)").get("path").asText(), "distributed")
    assert(report.get("timesSeconds").has("fit, R0 rerun"))
    val again = new ObjectMapper().readTree(S1LinearRE.run(spark, path, point))
    assert(!again.get("generated").asBoolean() && again.get("passed").asBoolean())
  }

  test("arguments: --path is required, and unknown ones are refused") {
    assertEquals(S1LinearRE.arguments(Array("--path", "p", "--rows", "5"))("rows"), "5")
    intercept[IllegalArgumentException](S1LinearRE.arguments(Array("--rows", "5")))
    intercept[IllegalArgumentException](S1LinearRE.arguments(Array("--path", "p", "--bogus", "1")))
    intercept[IllegalArgumentException](S1LinearRE.arguments(Array("--path")))
  }
}
