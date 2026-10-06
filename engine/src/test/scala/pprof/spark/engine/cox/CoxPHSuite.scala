package pprof.spark.engine.cox

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.data.{InputProblem, InvalidInputException}
import pprof.spark.numerics.{Cholesky, Normal}
import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** Parity and behavior of Cox regression (docs/spec/cox/first-slice.md §12 and
  * efron-weights-offsets.md §7) against the fixtures of pprof_py v0.7.0 and R survival 3.5-8.
  */
class CoxPHSuite extends SparkSuite {

  private val cases =
    Seq(
      "tiny-ties",
      "rc-unstratified",
      "rc-stratified",
      "rc-stratified-weights-offset",
      "lt-stratified",
      "lt-weights-offset"
    )
  private val methods = Seq[Ties](Ties.Breslow, Ties.Efron)

  private def tight(ties: Ties) = CoxOptions(ties = ties, eps = 1e-11, maxIterations = 100)

  private def info(name: String): JsonNode = Fixtures.json(s"cox/$name/case.json")

  private def flag(name: String, key: String): Boolean =
    Option(info(name).get(key)).exists(_.asBoolean)

  private def features(name: String): Seq[String] = {
    val node = info(name).get("features")
    (0 until node.size).map(i => node.get(i).asText())
  }

  private def spec(name: String): CoxSpec =
    CoxSpec(
      "time",
      "event",
      features(name),
      if (flag(name, "stratified")) Some("stratum") else None,
      Some("id"),
      if (flag(name, "weighted")) Some("weight") else None,
      if (flag(name, "offset")) Some("offset") else None,
      if (flag(name, "truncated")) Some("entry") else None
    )

  private def frame(name: String, reverse: Boolean = false): DataFrame = {
    val table = Fixtures.csv(s"cox/$name/input.csv")
    val extra = if (table.columns.contains("entry")) Seq("entry") else Seq.empty
    val names = Seq("id", "stratum", "time", "event", "weight", "offset") ++ extra ++ features(name)
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
        StructField("event", IntegerType, nullable = false),
        StructField("weight", DoubleType, nullable = false),
        StructField("offset", DoubleType, nullable = false)
      ) ++ (extra ++ features(name)).map(StructField(_, DoubleType, nullable = false))
    )
    spark.createDataFrame((if (reverse) rows.reverse else rows).asJava, schema)
  }

  private def reference(name: String, source: String, ties: Ties): JsonNode =
    Fixtures.json(s"cox/$name/$source.json").get(ties.name)

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

  test("function-level parity at beta = 0 and the fixed beta, both tie methods (T-fn)") {
    for (name <- cases; ties <- methods; point <- Seq("beta_zero", "beta_fixed")) {
      val beta = doubles(reference(name, "pprof_py", ties).get(point), "beta")
      val e = CoxPH.evaluateAt(frame(name), spec(name), beta, CoxOptions(ties = ties))
      for (source <- Seq("pprof_py", "r_survival")) {
        val node = reference(name, source, ties).get(point)
        val what = s"$name ${ties.name} $point vs $source"
        check("T-fn", s"$what loglik", Seq(e.value), doubles(node, "loglik"))
        check("T-fn", s"$what score", e.gradient.toSeq, doubles(node, "score"))
        check("T-fn", s"$what information", e.information.toSeq, doubles(node, "information"))
      }
    }
  }

  test("tight fits match pprof_py and R, both tie methods (T-coef, T-var, T-fn)") {
    for (name <- cases; ties <- methods) {
      val fit = CoxPH.fit(frame(name), spec(name), tight(ties))
      assert(fit.converged, s"$name ${ties.name}: ${fit.message}")
      for (source <- Seq("pprof_py", "r_survival")) {
        val node = reference(name, source, ties).get("tight")
        val what = s"$name ${ties.name} vs $source"
        check("T-coef", s"$what coefficients", fit.estimates, doubles(node, "coef"))
        check("T-var", s"$what standard errors", fit.standardErrors, doubles(node, "se"))
        check("T-var", s"$what covariance", fit.covariance, doubles(node, "covariance"))
        val information =
          Cholesky.factor(doubles(node, "covariance"), features(name).size).inversePacked
        check("T-var", s"$what information", fit.information, information)
        check("T-fn", s"$what loglik", Seq(fit.logLikelihood), doubles(node, "loglik"))
        check(
          "T-fn",
          s"$what null loglik",
          Seq(fit.logLikelihoodNull),
          doubles(node, "loglik_null")
        )
      }
    }
  }

  test("default fits stop where pprof_py stops, within T-coef of its estimates") {
    for (name <- cases; ties <- methods) {
      val fit = CoxPH.fit(frame(name), spec(name), CoxOptions(ties = ties))
      val node = reference(name, "pprof_py", ties).get("default")
      assert(fit.converged, s"$name ${ties.name}: ${fit.message}")
      assertEquals(fit.iterations, node.get("iterations").asInt, s"$name ${ties.name} iterations")
      check(
        "T-coef",
        s"$name ${ties.name} default coefficients",
        fit.estimates,
        doubles(node, "coef")
      )
    }
  }

  test("lockstep: iterates before the final one match pprof_py's (T-iter)") {
    for (name <- cases; ties <- methods) {
      val fit = CoxPH.fit(frame(name), spec(name), CoxOptions(ties = ties))
      val iterates = reference(name, "pprof_py", ties).get("iterates")
      val compared =
        (1 until fit.iterations).filter(k => iterates.get(k - 1).get("iterations").asInt == k)
      assert(compared.nonEmpty, s"$name ${ties.name}: no iterate to compare")
      for (k <- compared)
        check(
          "T-iter",
          s"$name ${ties.name} iterate $k",
          fit.iterationLog(k - 1).beta,
          doubles(iterates.get(k - 1), "beta")
        )
    }
  }

  test("Wald statistics, p-values and intervals (T-test, T-p, T-fn)") {
    val threshold = Tolerances.number("T-p.log10.threshold")
    val atol = Tolerances.number("T-p.log10.atol")
    val q = Normal.upperQuantile(0.025)
    for (name <- cases; ties <- methods) {
      val py = reference(name, "pprof_py", ties).get("tight")
      val (coef, se, z, p) =
        (doubles(py, "coef"), doubles(py, "se"), doubles(py, "z"), doubles(py, "p"))
      for (j <- coef.indices) {
        val ours = Normal.twoSidedPValue(z(j))
        if (p(j) < threshold)
          assert(math.abs(StrictMath.log10(ours) - StrictMath.log10(p(j))) <= atol, s"$name p $j")
        else assert(Tolerances("T-test").accepts(ours, p(j)), s"$name p $j: $ours vs ${p(j)}")
      }
      val lower = coef.indices.map(j => coef(j) - q * se(j))
      val upper = coef.indices.map(j => coef(j) + q * se(j))
      check("T-fn", s"$name ${ties.name} lower bounds", lower, doubles(py, "ci_lower"))
      check("T-fn", s"$name ${ties.name} upper bounds", upper, doubles(py, "ci_upper"))
      val r = reference(name, "r_survival", ties).get("tight")
      val fit = CoxPH.fit(frame(name), spec(name), tight(ties))
      val rz = doubles(r, "coef").zip(doubles(r, "se")).map { case (b, s) => b / s }
      check("T-test", s"$name ${ties.name} z vs R", fit.coefficients.map(_.z), rz)
    }
  }

  test("negative controls: the other tie method and shifted ties lie outside T-coef") {
    for (name <- cases; ties <- methods) {
      val fit = CoxPH.fit(frame(name), spec(name), tight(ties))
      val controls = reference(name, "pprof_py", ties).get("negative_controls")
      for (control <- Seq("other_ties", "shifted_tie")) {
        val ratio =
          Tolerances("T-coef").worstRatio(
            fit.estimates.toArray,
            doubles(controls.get(control), "coef")
          )
        assert(ratio > 10.0, s"$name ${ties.name} $control: ratio $ratio")
      }
    }
  }

  test("R0: row order and partitioning leave every bit unchanged, with weights and offsets") {
    val name = "rc-stratified-weights-offset"
    for (ties <- methods) {
      val a = CoxPH.fit(frame(name), spec(name), CoxOptions(ties = ties))
      val b =
        CoxPH.fit(frame(name, reverse = true).repartition(5), spec(name), CoxOptions(ties = ties))
      assertEquals(bits(b.estimates), bits(a.estimates))
      assertEquals(bits(b.covariance), bits(a.covariance))
      assertEquals(bits(Seq(b.logLikelihood)), bits(Seq(a.logLikelihood)))
      assertEquals(b.fingerprint, a.fingerprint)
    }
  }

  test("R1: another block layout stays within T-part") {
    val name = "rc-stratified-weights-offset"
    for (ties <- methods) {
      val small = CoxOptions(ties = ties, blocks = BlockOptions(targetBlockBytes = 2048L))
      val a = CoxPH.fit(frame(name), spec(name), CoxOptions(ties = ties))
      val b = CoxPH.fit(frame(name), spec(name), small)
      assert(b.layout.blockCount > a.layout.blockCount, s"${b.layout} vs ${a.layout}")
      check(
        "T-part.parameters",
        s"${ties.name} estimates across layouts",
        b.estimates,
        a.estimates.toArray
      )
      check(
        "T-part.objective",
        s"${ties.name} loglik across layouts",
        Seq(b.logLikelihood),
        Array(a.logLikelihood)
      )
      assertEquals(b.fingerprint, a.fingerprint)
    }
  }

  test("metamorphic: monotone time maps, scaling and translation") {
    val name = "rc-stratified"
    for (ties <- methods) {
      val options = CoxOptions(ties = ties)
      val base = CoxPH.fit(frame(name), spec(name), options)
      val squared =
        CoxPH.fit(
          frame(name).withColumn("time", col("time") * col("time") + 5.0),
          spec(name),
          options
        )
      assertEquals(bits(squared.estimates), bits(base.estimates))
      assertEquals(bits(Seq(squared.logLikelihood)), bits(Seq(base.logLikelihood)))
      val scaled = CoxPH.fit(frame(name).withColumn("x1", col("x1") * 4.0), spec(name), options)
      val expected = base.estimates.updated(0, base.estimates(0) / 4).toArray
      check("T-coef", s"${ties.name} x1 scaled by 4", scaled.estimates, expected)
      val shifted = CoxPH.fit(frame(name).withColumn("x2", col("x2") + 3.0), spec(name), options)
      check("T-coef", s"${ties.name} x2 shifted by 3", shifted.estimates, base.estimates.toArray)
    }
  }

  test("metamorphic: weights scaled by 3 and an offset proportional to a covariate") {
    for (ties <- methods) {
      val options = CoxOptions(ties = ties)
      val name = "rc-stratified-weights-offset"
      val base = CoxPH.fit(frame(name), spec(name), options)
      val tripled =
        CoxPH.fit(frame(name).withColumn("weight", col("weight") * 3.0), spec(name), options)
      check("T-coef", s"${ties.name} weights times 3", tripled.estimates, base.estimates.toArray)
      val rootThree = math.sqrt(3.0)
      check(
        "T-var",
        s"${ties.name} weights times 3, SE",
        tripled.standardErrors,
        base.standardErrors.map(_ / rootThree).toArray
      )
      val plain = "rc-stratified"
      val noOffset = CoxPH.fit(frame(plain), spec(plain), options)
      val withOffset = CoxPH.fit(
        frame(plain).withColumn("o", col("x1") * 0.5),
        spec(plain).copy(offset = Some("o")),
        options
      )
      val expected = noOffset.estimates.updated(0, noOffset.estimates(0) - 0.5).toArray
      check("T-coef", s"${ties.name} offset 0.5 x1", withOffset.estimates, expected)
    }
  }

  test("a zero weight is the same as dropping the row, under both tie methods (X-013)") {
    val name = "tiny-ties"
    for (ties <- methods) {
      val options = tight(ties)
      // id 0 is an event tied with id 1 at time 1
      val zero = frame(name).withColumn("weight", when(col("id") === 0L, 0.0).otherwise(1.0))
      val a = CoxPH.fit(zero, spec(name).copy(weight = Some("weight")), options)
      val b = CoxPH.fit(frame(name).filter(col("id") =!= 0L), spec(name), options)
      check("T-coef", s"${ties.name} zero weight vs dropped row", a.estimates, b.estimates.toArray)
      check(
        "T-var",
        s"${ties.name} zero weight vs dropped row, SE",
        a.standardErrors,
        b.standardErrors.toArray
      )
    }
  }

  test("under Breslow, an integer weight of 2 equals a duplicated row") {
    val name = "rc-unstratified"
    val doubled = frame(name).withColumn("weight", when(col("id") % 3L === 0L, 2.0).otherwise(1.0))
    val copies = frame(name).union(
      frame(name).filter(col("id") % 3L === 0L).withColumn("id", col("id") + 100000L)
    )
    val a = CoxPH.fit(doubled, spec(name).copy(weight = Some("weight")))
    val b = CoxPH.fit(copies, spec(name))
    check("T-coef", "weight 2 vs duplicated rows", a.estimates, b.estimates.toArray)
    check("T-var", "weight 2 vs duplicated rows, SE", a.standardErrors, b.standardErrors.toArray)
  }

  private def small(rows: (Double, Int, Double, Double, Double)*): DataFrame = {
    val schema = StructType(
      Seq(
        StructField("time", DoubleType),
        StructField("event", IntegerType),
        StructField("x1", DoubleType),
        StructField("w", DoubleType),
        StructField("o", DoubleType)
      )
    )
    spark.createDataFrame(rows.map { case (t, e, x, w, o) => Row(t, e, x, w, o) }.asJava, schema)
  }

  test("invalid times, events, covariates, weights and offsets fail with counts, never values") {
    val e = intercept[InvalidInputException](
      CoxPH.fit(
        small(
          (0.0, 1, 1.0, 1.0, 0.0),
          (-1.0, 0, 2.0, -0.5, 0.0),
          (2.0, 2, Double.NaN, 1.0, Double.NaN),
          (3.0, 1, 0.5, 1.0, 0.0)
        ),
        CoxSpec("time", "event", Seq("x1"), weight = Some("w"), offset = Some("o"))
      )
    )
    assertEquals(
      e.problems.toSet,
      Set[InputProblem](
        InputProblem.NonPositiveValues("time", 2L),
        InputProblem.NonBinaryValues("event", 1L),
        InputProblem.InvalidValues("x1", 0L, 1L),
        InputProblem.NegativeValues("w", 1L),
        InputProblem.InvalidValues("o", 0L, 1L)
      )
    )
  }

  test("no events, or only zero-weight events, fails (X-012)") {
    val none = intercept[InvalidInputException](
      CoxPH.fit(
        small((1.0, 0, 1.0, 1.0, 0.0), (2.0, 0, 2.0, 1.0, 0.0)),
        CoxSpec("time", "event", Seq("x1"))
      )
    )
    assertEquals(none.problems, Seq[InputProblem](InputProblem.NoEvents))
    val weightless = intercept[InvalidInputException](
      CoxPH.fit(
        small((1.0, 1, 1.0, 0.0, 0.0), (2.0, 0, 2.0, 1.0, 0.0)),
        CoxSpec("time", "event", Seq("x1"), weight = Some("w"))
      )
    )
    assertEquals(weightless.problems, Seq[InputProblem](InputProblem.NoEvents))
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
      CoxRow(0, 1, 7L, 2.0, event = false, 1.0, 0.0, Array(1.0)),
      CoxRow(0, 0, 3L, 1.0, event = true, 1.0, -0.0, Array(-0.0)),
      CoxRow(0, 1, 5L, 2.0, event = true, 0.5, 0.25, Array(2.0)),
      CoxRow(0, 1, 4L, 3.0, event = false, 2.0, 0.0, Array(0.5))
    )
    val block = CoxBlockBuilder.build(0, rows.iterator, 1)
    assertEquals(block.groupIndex.toSeq, Seq(0, 1))
    assertEquals(block.groupStart.toSeq, Seq(0, 1))
    assertEquals(block.rowId.toSeq, Seq(3L, 4L, 5L, 7L))
    assertEquals(block.weight.toSeq, Seq(1.0, 2.0, 0.5, 1.0))
    assertEquals(java.lang.Double.doubleToLongBits(block.x(0)), 0L)
    assertEquals(java.lang.Double.doubleToLongBits(block.offset(0)), 0L)
  }

  test("an entry column of zeros reproduces the right-censored fit bit for bit") {
    val name = "rc-stratified"
    for (ties <- methods) {
      val options = CoxOptions(ties = ties)
      val plain = CoxPH.fit(frame(name), spec(name), options)
      val zero = CoxPH.fit(
        frame(name).withColumn("entry", lit(0.0)),
        spec(name).copy(entry = Some("entry")),
        options
      )
      assertEquals(bits(zero.estimates), bits(plain.estimates))
      assertEquals(bits(zero.covariance), bits(plain.covariance))
      assertEquals(bits(Seq(zero.logLikelihood)), bits(Seq(plain.logLikelihood)))
    }
  }

  test("splitting (a, b] at c into (a, c] without an event and (c, b] leaves the fit unchanged") {
    val name = "lt-stratified"
    for (ties <- methods) {
      val base = frame(name)
      val long = col("time") - col("entry") >= 2.0
      val first =
        base.filter(long).withColumn("time", col("entry") + 1.0).withColumn("event", lit(0))
      val second =
        base
          .filter(long)
          .withColumn("entry", col("entry") + 1.0)
          .withColumn("id", col("id") + 100000L)
      val pieces = base.filter(!long).unionByName(first).unionByName(second)
      val a = CoxPH.fit(base, spec(name), tight(ties))
      val b = CoxPH.fit(pieces, spec(name), tight(ties))
      assert(b.observations > a.observations)
      check("T-coef", s"${ties.name} split intervals", b.estimates, a.estimates.toArray)
      check(
        "T-fn",
        s"${ties.name} split intervals, loglik",
        Seq(b.logLikelihood),
        Array(a.logLikelihood)
      )
    }
  }

  test(
    "shifting every entry and exit time, here below zero, leaves the fit unchanged bit for bit"
  ) {
    val name = "lt-weights-offset"
    for (ties <- methods) {
      val options = CoxOptions(ties = ties)
      val a = CoxPH.fit(frame(name), spec(name), options)
      val shifted = frame(name)
        .withColumn("time", col("time") - 100.0)
        .withColumn("entry", col("entry") - 100.0)
      val b = CoxPH.fit(shifted, spec(name), options)
      assertEquals(bits(b.estimates), bits(a.estimates))
      assertEquals(bits(b.covariance), bits(a.covariance))
      assertEquals(bits(Seq(b.logLikelihood)), bits(Seq(a.logLikelihood)))
    }
  }

  test("entry times must be finite and below the exit time, with counts reported") {
    val schema = StructType(
      Seq(
        StructField("entry", DoubleType),
        StructField("time", DoubleType),
        StructField("event", IntegerType),
        StructField("x1", DoubleType)
      )
    )
    val rows =
      Seq((1.0, 1.0, 1, 0.5), (2.0, 1.5, 0, 1.0), (Double.NaN, 3.0, 1, 0.0), (0.0, 2.0, 1, -1.0))
    val df = spark.createDataFrame(rows.map { case (a, b, e, x) => Row(a, b, e, x) }.asJava, schema)
    val e = intercept[InvalidInputException](
      CoxPH.fit(df, CoxSpec("time", "event", Seq("x1"), entry = Some("entry")))
    )
    assertEquals(
      e.problems.toSet,
      Set[InputProblem](
        InputProblem.InvalidValues("entry", 0L, 1L),
        InputProblem.EntryNotBeforeExit("entry", "time", 2L)
      )
    )
  }
}
