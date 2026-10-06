package pprof.spark.engine.cox

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.data.{InputProblem, InvalidInputException}
import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** Parity and behavior of the first Cox slice (docs/spec/cox/first-slice.md §12) against the
  * reference fixtures of pprof_py v0.7.0 and R survival 3.5-8 (ADR-0005).
  */
class CoxPHSuite extends SparkSuite {

  private val cases = Seq("tiny-ties", "rc-unstratified", "rc-stratified")
  private val tight = CoxOptions(eps = 1e-11, maxIterations = 100)

  private def info(name: String): JsonNode = Fixtures.json(s"cox/$name/case.json")

  private def features(name: String): Seq[String] = {
    val node = info(name).get("features")
    (0 until node.size).map(i => node.get(i).asText())
  }

  private def spec(name: String): CoxSpec =
    CoxSpec(
      "time",
      "event",
      features(name),
      if (info(name).get("stratified").asBoolean) Some("stratum") else None,
      Some("id")
    )

  private def frame(name: String, reverse: Boolean = false): DataFrame = {
    val table = Fixtures.csv(s"cox/$name/input.csv")
    val names = Seq("id", "stratum", "time", "event") ++ features(name)
    val index = names.map(table.columns.indexOf(_))
    val rows = table.rows.map { r =>
      Row.fromSeq(
        Seq[Any](r(index(0)).toLong, r(index(1)).toLong, r(index(2)), r(index(3)).toInt) ++
          index.drop(4).map(r(_))
      )
    }
    val schema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("stratum", LongType, nullable = false),
        StructField("time", DoubleType, nullable = false),
        StructField("event", IntegerType, nullable = false)
      ) ++ features(name).map(StructField(_, DoubleType, nullable = false))
    )
    spark.createDataFrame((if (reverse) rows.reverse else rows).asJava, schema)
  }

  private def reference(name: String, source: String): JsonNode =
    Fixtures.json(s"cox/$name/$source.json").get("breslow")

  private def doubles(node: JsonNode, key: String): Array[Double] = Fixtures.doubles(node.get(key))

  private def check(
      cls: String,
      what: String,
      actual: Seq[Double],
      expected: Array[Double]
  ): Unit = {
    val ratio = Tolerances(cls).worstRatio(actual.toArray, expected)
    assert(
      ratio <= 1.0,
      s"$what: worst ratio $ratio under $cls; actual ${actual.mkString(", ")}; " +
        s"expected ${expected.mkString(", ")}"
    )
  }

  private def bits(values: Seq[Double]): Seq[Long] = values.map(java.lang.Double.doubleToLongBits)

  test("function-level parity at beta = 0 and the fixed beta (T-fn)") {
    for (name <- cases; point <- Seq("beta_zero", "beta_fixed")) {
      val beta = doubles(reference(name, "pprof_py").get(point), "beta")
      val e = CoxPH.evaluateAt(frame(name), spec(name), beta)
      for (source <- Seq("pprof_py", "r_survival")) {
        val node = reference(name, source).get(point)
        check("T-fn", s"$name $point loglik vs $source", Seq(e.value), doubles(node, "loglik"))
        check("T-fn", s"$name $point score vs $source", e.gradient.toSeq, doubles(node, "score"))
        check(
          "T-fn",
          s"$name $point information vs $source",
          e.information.toSeq,
          doubles(node, "information")
        )
      }
    }
  }

  test("tight fits match pprof_py and R (T-coef, T-var, T-fn)") {
    for (name <- cases) {
      val fit = CoxPH.fit(frame(name), spec(name), tight)
      assert(fit.converged, s"$name: ${fit.message}")
      for (source <- Seq("pprof_py", "r_survival")) {
        val node = reference(name, source).get("tight")
        check("T-coef", s"$name coefficients vs $source", fit.estimates, doubles(node, "coef"))
        check("T-var", s"$name standard errors vs $source", fit.standardErrors, doubles(node, "se"))
        check("T-var", s"$name covariance vs $source", fit.covariance, doubles(node, "covariance"))
        check("T-fn", s"$name loglik vs $source", Seq(fit.logLikelihood), doubles(node, "loglik"))
        check(
          "T-fn",
          s"$name null loglik vs $source",
          Seq(fit.logLikelihoodNull),
          doubles(node, "loglik_null")
        )
      }
    }
  }

  test("default fits stop where pprof_py stops, within T-coef of its estimates") {
    for (name <- cases) {
      val fit = CoxPH.fit(frame(name), spec(name))
      val node = reference(name, "pprof_py").get("default")
      assert(fit.converged, s"$name: ${fit.message}")
      assertEquals(fit.iterations, node.get("iterations").asInt, s"$name iterations")
      check(
        "T-coef",
        s"$name default coefficients vs pprof_py",
        fit.estimates,
        doubles(node, "coef")
      )
    }
  }

  test("lockstep: iterates before the final one match pprof_py's (T-iter)") {
    for (name <- cases) {
      val fit = CoxPH.fit(frame(name), spec(name))
      val iterates = reference(name, "pprof_py").get("iterates")
      val compared =
        (1 until fit.iterations).filter(k => iterates.get(k - 1).get("iterations").asInt == k)
      assert(compared.nonEmpty, s"$name: no iterate to compare")
      for (k <- compared)
        check(
          "T-iter",
          s"$name iterate $k",
          fit.iterationLog(k - 1).beta,
          doubles(iterates.get(k - 1), "beta")
        )
    }
  }

  test("Wald statistics, p-values and intervals (T-test, T-p, T-fn)") {
    val threshold = Tolerances.number("T-p.log10.threshold")
    val atol = Tolerances.number("T-p.log10.atol")
    for (name <- cases) {
      val py = reference(name, "pprof_py").get("tight")
      val (coef, se, z, p) =
        (doubles(py, "coef"), doubles(py, "se"), doubles(py, "z"), doubles(py, "p"))
      val q = pprof.spark.numerics.Normal.upperQuantile(0.025)
      for (j <- coef.indices) {
        val ours = pprof.spark.numerics.Normal.twoSidedPValue(z(j))
        if (p(j) < threshold)
          assert(math.abs(StrictMath.log10(ours) - StrictMath.log10(p(j))) <= atol, s"$name p $j")
        else assert(Tolerances("T-test").accepts(ours, p(j)), s"$name p $j: $ours vs ${p(j)}")
      }
      check(
        "T-fn",
        s"$name lower bounds",
        coef.indices.map(j => coef(j) - q * se(j)),
        doubles(py, "ci_lower")
      )
      check(
        "T-fn",
        s"$name upper bounds",
        coef.indices.map(j => coef(j) + q * se(j)),
        doubles(py, "ci_upper")
      )
      val r = reference(name, "r_survival").get("tight")
      val fit = CoxPH.fit(frame(name), spec(name), tight)
      val rz = doubles(r, "coef").zip(doubles(r, "se")).map { case (b, s) => b / s }
      check("T-test", s"$name z vs R", fit.coefficients.map(_.z), rz)
    }
  }

  test("negative controls: Efron and shifted-tie estimates lie outside T-coef") {
    for (name <- cases) {
      val fit = CoxPH.fit(frame(name), spec(name), tight)
      val controls = reference(name, "pprof_py").get("negative_controls")
      for (control <- Seq("other_ties", "shifted_tie")) {
        val ratio = Tolerances("T-coef").worstRatio(
          fit.estimates.toArray,
          doubles(controls.get(control), "coef")
        )
        assert(ratio > 10.0, s"$name $control: ratio $ratio")
      }
    }
  }

  test("R0: row order and partitioning leave every bit unchanged") {
    val a = CoxPH.fit(frame("rc-stratified"), spec("rc-stratified"))
    val b = CoxPH.fit(frame("rc-stratified", reverse = true).repartition(5), spec("rc-stratified"))
    assertEquals(bits(b.estimates), bits(a.estimates))
    assertEquals(bits(b.covariance), bits(a.covariance))
    assertEquals(bits(Seq(b.logLikelihood)), bits(Seq(a.logLikelihood)))
    assertEquals(b.fingerprint, a.fingerprint)
  }

  test("R1: another block layout stays within T-part") {
    val small = CoxOptions(blocks = BlockOptions(targetBlockBytes = 2048L))
    val a = CoxPH.fit(frame("rc-stratified"), spec("rc-stratified"))
    val b = CoxPH.fit(frame("rc-stratified"), spec("rc-stratified"), small)
    assert(b.layout.blockCount > a.layout.blockCount, s"${b.layout} vs ${a.layout}")
    check("T-part.parameters", "estimates across layouts", b.estimates, a.estimates.toArray)
    check("T-part.objective", "loglik across layouts", Seq(b.logLikelihood), Array(a.logLikelihood))
    assertEquals(b.fingerprint, a.fingerprint)
  }

  test("metamorphic: monotone time maps, scaling and translation") {
    val name = "rc-stratified"
    val base = CoxPH.fit(frame(name), spec(name))
    val squared =
      CoxPH.fit(frame(name).withColumn("time", col("time") * col("time") + 5.0), spec(name))
    assertEquals(bits(squared.estimates), bits(base.estimates))
    assertEquals(bits(Seq(squared.logLikelihood)), bits(Seq(base.logLikelihood)))
    val scaled = CoxPH.fit(frame(name).withColumn("x1", col("x1") * 4.0), spec(name))
    check(
      "T-coef",
      "x1 scaled by 4",
      scaled.estimates,
      base.estimates.updated(0, base.estimates(0) / 4).toArray
    )
    val shifted = CoxPH.fit(frame(name).withColumn("x2", col("x2") + 3.0), spec(name))
    check("T-coef", "x2 shifted by 3", shifted.estimates, base.estimates.toArray)
  }

  private def small(rows: (Double, Int, Double)*): DataFrame = {
    val schema = StructType(
      Seq(
        StructField("time", DoubleType),
        StructField("event", IntegerType),
        StructField("x1", DoubleType)
      )
    )
    spark.createDataFrame(rows.map { case (t, e, x) => Row(t, e, x) }.asJava, schema)
  }

  test("invalid times, events and covariates fail with counts, never values") {
    val e = intercept[InvalidInputException](
      CoxPH.fit(
        small((0.0, 1, 1.0), (-1.0, 0, 2.0), (2.0, 2, Double.NaN), (3.0, 1, 0.5)),
        CoxSpec("time", "event", Seq("x1"))
      )
    )
    assertEquals(
      e.problems.toSet,
      Set[InputProblem](
        InputProblem.NonPositiveValues("time", 2L),
        InputProblem.NonBinaryValues("event", 1L),
        InputProblem.InvalidValues("x1", 0L, 1L)
      )
    )
  }

  test("no events at all fails (X-012)") {
    val e = intercept[InvalidInputException](
      CoxPH.fit(small((1.0, 0, 1.0), (2.0, 0, 2.0)), CoxSpec("time", "event", Seq("x1")))
    )
    assertEquals(e.problems, Seq[InputProblem](InputProblem.NoEvents))
  }

  test("collinear and constant covariates fail and are named (X-011)") {
    val name = "rc-unstratified"
    val collinear = intercept[CoxAliasingException](
      CoxPH.fit(
        frame(name).withColumn("x4", col("x1") * 2.0),
        spec(name).copy(features = features(name) :+ "x4")
      )
    )
    assertEquals(collinear.features, Seq("x4"))
    val constant = intercept[CoxAliasingException](
      CoxPH.fit(frame(name).withColumn("c", lit(1.0)), spec(name).copy(features = Seq("x1", "c")))
    )
    assertEquals(constant.features, Seq("c"))
  }

  test("strata without events are counted and contribute nothing") {
    val name = "rc-stratified"
    val modified =
      frame(name).withColumn("event", when(col("stratum") <= 2L, 0).otherwise(col("event")))
    val expected = modified.groupBy("stratum").sum("event").collect().count(_.getLong(1) == 0L)
    val fit = CoxPH.fit(modified, spec(name))
    assert(expected >= 1)
    assertEquals(fit.strataWithoutEvents, expected)
    assert(fit.converged)
  }

  test("stopping at the iteration cap is reported in the result") {
    val fit =
      CoxPH.fit(frame("rc-stratified"), spec("rc-stratified"), CoxOptions(maxIterations = 1))
    assert(!fit.converged)
    assertEquals(fit.iterations, 1)
    assertEquals(fit.warnings.size, 1)
  }

  test("a stratum larger than maxStratumRows fails and points to TimeRange") {
    val e = intercept[IllegalArgumentException](
      CoxPH.fit(
        frame("rc-unstratified"),
        spec("rc-unstratified"),
        CoxOptions(maxStratumRows = 100L)
      )
    )
    assert(e.getMessage.contains("TimeRange"), e.getMessage)
  }

  test("blocks hold rows in canonical order: time descending, events first, then row id") {
    val rows = Seq(
      CoxRow(0, 1, 7L, 2.0, event = false, Array(1.0)),
      CoxRow(0, 0, 3L, 1.0, event = true, Array(-0.0)),
      CoxRow(0, 1, 5L, 2.0, event = true, Array(2.0)),
      CoxRow(0, 1, 4L, 3.0, event = false, Array(0.5))
    )
    val block = CoxBlockBuilder.build(0, rows.iterator, 1)
    assertEquals(block.groupIndex.toSeq, Seq(0, 1))
    assertEquals(block.groupStart.toSeq, Seq(0, 1))
    assertEquals(block.rowId.toSeq, Seq(3L, 4L, 5L, 7L))
    assertEquals(java.lang.Double.doubleToLongBits(block.x(0)), 0L)
  }
}
