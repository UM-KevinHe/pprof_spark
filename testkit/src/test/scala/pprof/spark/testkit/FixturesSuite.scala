package pprof.spark.testkit

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode

/** The reference fixtures and the tolerance calibration (D-09, NN-9, ADR-0005). This suite repeats
  * in CI what reference/fixtures/calibrate.py checks; keep the two in step.
  */
class FixturesSuite extends munit.FunSuite {

  /** Every negative control must miss its tolerance class by at least this factor. */
  private val Margin = 10.0

  test("every fixture file matches the manifest's checksum, and none is unlisted") {
    val listed = Fixtures.checksums
    assertEquals(Fixtures.filesOnDisk, listed.keys.toVector.sorted)
    listed.foreach { case (file, expected) => assertEquals(Fixtures.sha256(file), expected, file) }
  }

  test("the fixtures come from the reference pinned in REFERENCE.lock") {
    val lock = new String(
      Files.readAllBytes(Fixtures.root.getParent.resolve("reference/REFERENCE.lock")),
      StandardCharsets.UTF_8
    )
    val commit = Fixtures.manifest.get("reference").get("pprof_py").get("commit").asText()
    assert(
      lock.contains("commit = \"" + commit + "\""),
      s"$commit is not pinned; regenerate (PAR-1)"
    )
  }

  test("inputs lie on exact grids, so every implementation reads the same numbers") {
    Fixtures.coxCases.foreach { name =>
      val table = Fixtures.csv(s"cox/$name/input.csv")
      def onGrid(column: String, scale: Double): Boolean =
        table.column(column).forall(v => v * scale == math.rint(v * scale))
      assert(onGrid("time", 1.0) && table.column("time").forall(_ >= 1.0), name)
      assert(table.column("event").forall(v => v == 0.0 || v == 1.0), name)
      assert(onGrid("weight", 2.0) && onGrid("offset", 16.0), name)
      table.columns.filter(_.startsWith("x")).foreach(c => assert(onGrid(c, 64.0), s"$name $c"))
      if (table.columns.contains("entry")) {
        assert(onGrid("entry", 1.0), name)
        assert(
          table.column("entry").zip(table.column("time")).forall { case (a, b) => a < b },
          name
        )
      }
    }
  }

  test("pprof_py and R agree within the tolerance classes; negative controls fall far outside") {
    val fits = Seq("coef" -> "T-coef", "se" -> "T-var", "covariance" -> "T-var", "loglik" -> "T-fn")
    for (name <- Fixtures.coxCases; ties <- Seq("breslow", "efron")) {
      val py = Fixtures.json(s"cox/$name/pprof_py.json").get(ties)
      val r = Fixtures.json(s"cox/$name/r_survival.json").get(ties)
      fits.foreach { case (quantity, cls) =>
        val reference = Fixtures.doubles(py.get("tight").get(quantity))
        val agreement =
          Tolerances(cls).worstRatio(reference, Fixtures.doubles(r.get("tight").get(quantity)))
        assert(agreement <= 1.0, s"$name $ties $quantity: pprof_py vs R is $agreement of $cls")
        py.get("negative_controls").properties().asScala.foreach { control =>
          val ratio =
            Tolerances(cls).worstRatio(Fixtures.doubles(control.getValue.get(quantity)), reference)
          assert(ratio >= Margin, s"$name $ties $quantity: ${control.getKey} is $ratio of $cls")
        }
      }
      for (
        point <- Seq("beta_zero", "beta_fixed"); quantity <- Seq("loglik", "score", "information")
      ) {
        val agreement = Tolerances("T-fn").worstRatio(
          Fixtures.doubles(py.get(point).get(quantity)),
          Fixtures.doubles(r.get(point).get(quantity))
        )
        assert(agreement <= 1.0, s"$name $ties $quantity at $point: $agreement of T-fn")
      }
    }
  }

  test("logistic inputs are exact: integer outcomes and trials, features on a 1/64 grid") {
    Fixtures.cases("logistic").foreach { name =>
      val table = Fixtures.csv(s"logistic/$name/input.csv")
      def integral(values: Array[Double]): Boolean = values.forall(v => v == math.rint(v))
      val y = table.column("y")
      val n =
        if (table.columns.contains("n")) table.column("n") else Array.fill(y.length)(1.0)
      assert(integral(y) && integral(n) && integral(table.column("provider")), name)
      assert(
        y.zip(n).forall { case (events, trials) =>
          trials >= 1.0 && events >= 0.0 && events <= trials
        },
        name
      )
      Seq("x1", "x2", "x3").foreach { c =>
        assert(table.column(c).forall(v => v * 64.0 == math.rint(v * 64.0)), s"$name $c")
      }
    }
  }

  test("logistic: pprof_py and R agree within the tolerance classes; controls fall far outside") {
    val quantities = Seq(
      "beta" -> "T-coef",
      "gamma" -> "T-coef",
      "var_beta" -> "T-var",
      "var_gamma" -> "T-var",
      "var_case_mix" -> "T-var",
      "loglik" -> "T-fn"
    )
    def ratio(cls: String, actual: JsonNode, expected: JsonNode): Double =
      Tolerances(cls).worstRatio(Fixtures.doubles(actual), Fixtures.doubles(expected))
    Fixtures.cases("logistic").foreach { name =>
      val py = Fixtures.json(s"logistic/$name/pprof_py.json")
      val r = Fixtures.json(s"logistic/$name/r_logistic.json")
      if (r.has("glm")) quantities.foreach { case (q, cls) =>
        val agreement = ratio(cls, py.get("tight").get(q), r.get("glm").get(q))
        assert(agreement <= 1.0, s"$name $q: pprof_py vs glm is $agreement of $cls")
      }
      for (fit <- Seq("default", "tight"); q <- Seq("beta", "gamma")) {
        val agreement = ratio("T-coef", py.get(fit).get(q), r.get(s"serbin_$fit").get(q))
        assert(agreement <= 1.0, s"$name $fit $q: pprof_py vs R SerBIN is $agreement of T-coef")
      }
      py.get("negative_controls").properties().asScala.foreach { control =>
        quantities.foreach { case (q, cls) =>
          val miss = ratio(cls, control.getValue.get(q), py.get("tight").get(q))
          assert(miss >= Margin, s"$name $q: ${control.getKey} is $miss of $cls")
        }
      }
    }
  }
}
