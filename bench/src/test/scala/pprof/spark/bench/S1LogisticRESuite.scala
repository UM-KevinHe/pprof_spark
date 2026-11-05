package pprof.spark.bench

import java.nio.file.Files

import com.fasterxml.jackson.databind.ObjectMapper
import pprof.spark.testkit.SparkSuite

/** The S1 smoke's runner for part 4b (docs/parity/scale/s1-logistic-re.md) on a small point: it generates the data,
  * fits and refits, and reports every check passed; a second run reuses the data.
  */
class S1LogisticRESuite extends SparkSuite {

  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(8, "min")

  test(
    "a small point passes every check, the layout check included, and a second run reuses its data"
  ) {
    val path = Files.createTempDirectory("pprof-s1-logistic-re").resolve("point").toString
    val point = S1LogisticRE.Point(n = 20000L, m = 100, p = 4)
    val text = S1LogisticRE.run(spark, path, point, layout = true)
    println(s"S1 report: $text")
    val report = new ObjectMapper().readTree(text)
    assert(report.get("generated").asBoolean())
    report.get("checks").properties().forEach { check =>
      assert(check.getValue.get("passed").asBoolean(), s"${check.getKey}: ${check.getValue}")
    }
    assert(report.get("passed").asBoolean())
    assertEquals(report.get("fit").get("providers").asInt(), 100)
    assert(report.get("timesSeconds").has("fit, half-size blocks"))
    val again = new ObjectMapper().readTree(S1LogisticRE.run(spark, path, point))
    assert(!again.get("generated").asBoolean() && again.get("passed").asBoolean())
  }

  test("arguments: --path is required, and unknown ones are refused") {
    assertEquals(S1LogisticRE.arguments(Array("--path", "p", "--layout", "true"))("layout"), "true")
    intercept[IllegalArgumentException](S1LogisticRE.arguments(Array("--rows", "5")))
    intercept[IllegalArgumentException](
      S1LogisticRE.arguments(Array("--path", "p", "--budget", "1"))
    )
  }
}
