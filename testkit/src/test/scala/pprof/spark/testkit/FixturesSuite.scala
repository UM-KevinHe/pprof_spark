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
      py.get("negative_controls")
        .properties()
        .asScala
        .filter(_.getKey != "cluster_shifted")
        .foreach { control =>
          quantities.foreach { case (q, cls) =>
            val miss = ratio(cls, control.getValue.get(q), py.get("tight").get(q))
            assert(miss >= Margin, s"$name $q: ${control.getKey} is $miss of $cls")
          }
        }
    }
  }

  test(
    "logistic slice 2b: tests, AUC, predictions and robust variances agree with R; controls fall far outside"
  ) {
    def ratio(cls: String, actual: JsonNode, expected: JsonNode): Double =
      Tolerances(cls).worstRatio(Fixtures.doubles(actual), Fixtures.doubles(expected))
    val robust = Seq("var_beta", "var_case_mix", "var_fixed_beta")
    Fixtures.cases("logistic").foreach { name =>
      val inference = Fixtures.json(s"logistic/$name/pprof_py.json").get("inference").get("tight")
      val py = Fixtures.json(s"logistic/$name/pprof_py.json")
      val r = Fixtures.json(s"logistic/$name/r_logistic.json")
      if (r.has("glm_tests")) {
        val t = r.get("glm_tests")
        val checks = Seq(
          ("LR", "T-test", inference.get("lr").get("stat"), t.get("lr")),
          ("score", "T-test", inference.get("score").get("stat"), t.get("rao")),
          ("predictions", "T-meas", inference.get("predict"), t.get("predict"))
        ) ++ (if (t.has("auc")) Seq(("AUC", "T-meas", inference.get("auc"), t.get("auc")))
              else Seq.empty) ++
          (if (r.has("glm_robust"))
             robust
               .map(q => (q, "T-var", inference.get("robust").get(q), r.get("glm_robust").get(q)))
           else Seq.empty)
        checks.foreach { case (what, cls, a, e) =>
          val agreement = ratio(cls, a, e)
          assert(agreement <= 1.0, s"$name $what: pprof_py vs R is $agreement of $cls")
        }
      }
      py.get("negative_controls").properties().asScala.foreach { entry =>
        val c = entry.getValue.get("inference")
        val targets =
          if (entry.getKey == "cluster_shifted")
            robust.map(q => (q, "T-var", c.get("robust").get(q), inference.get("robust").get(q)))
          else
            Seq(
              ("LR", "T-test", c.get("lr").get("stat"), inference.get("lr").get("stat")),
              ("score", "T-test", c.get("score").get("stat"), inference.get("score").get("stat"))
            ) ++ (if (entry.getKey == "flipped_outcome" && inference.get("auc").size > 0)
                    Seq(("AUC", "T-meas", c.get("auc"), inference.get("auc")))
                  else Seq.empty)
        targets.foreach { case (what, cls, a, e) =>
          val miss = ratio(cls, a, e)
          assert(miss >= Margin, s"$name ${entry.getKey} $what: $miss of $cls")
        }
      }
    }
  }

  test(
    "logistic slice 2c: provider tests agree with R pprof; controls fall far outside; tails match mpmath"
  ) {
    def ratio(cls: String, actual: Array[Double], expected: Array[Double]): Double =
      Tolerances(cls).worstRatio(actual, expected)
    val pairs = Seq(
      "exact_two_sided_median" -> "r_tests_exact_two_sided",
      "exact_greater_median" -> "r_tests_exact_greater",
      "exact_less_median" -> "r_tests_exact_less",
      "score_two_sided_median" -> "r_tests_score_two_sided"
    )
    Fixtures.cases("logistic").foreach { name =>
      val py = Fixtures.json(s"logistic/$name/pprof_py.json")
      val r = Fixtures.json(s"logistic/$name/r_logistic.json")
      val tests = py.get("provider_tests")
      pairs.filter(pair => r.has(pair._2)).foreach { case (mine, theirs) =>
        val za = Fixtures.doubles(tests.get(mine).get("z"))
        val ze = Fixtures.doubles(r.get(theirs).get("z"))
        val finite = ze.indices.filter(k => ze(k).isFinite)
        assert(
          ratio("T-test", finite.map(za).toArray, finite.map(ze).toArray) <= 1.0,
          s"$name $mine z"
        )
        assert(
          ze.indices
            .filterNot(finite.contains)
            .forall(k => math.signum(za(k)) == math.signum(ze(k)) && math.abs(za(k)) > 37.0)
        )
        val flags = tests.get(mine).get("flag")
        assertEquals(
          (0 until flags.size).map(flags.get(_).asInt).toVector,
          Fixtures.doubles(r.get(theirs).get("flag")).toVector.map(_.toInt)
        )
      }
      val exact = Fixtures.doubles(tests.get("exact_two_sided_median").get("z"))
      val controls = py.get("provider_test_controls")
      Seq(
        Fixtures.doubles(controls.get("reference_shifted").get("exact_two_sided_median").get("z")),
        Fixtures.doubles(controls.get("flipped_outcome").get("exact_two_sided_median").get("z"))
      ).foreach(control => assert(ratio("T-test", control, exact) >= Margin, name))
      val greater = Fixtures.doubles(tests.get("exact_greater_median").get("z"))
      assert(
        ratio(
          "T-test",
          Fixtures.doubles(tests.get("exact_less_median").get("z")),
          greater
        ) >= Margin,
        name
      )
    }
    val tails = Fixtures.json("logistic/poibin-tails.json")
    tails.properties().asScala.foreach { vector =>
      val rows = vector.getValue.get("tails")
      (0 until rows.size).foreach { i =>
        val exact = Fixtures.doubles(rows.get(i).get("mpmath"))
        val theirs = Fixtures.doubles(rows.get(i).get("pprof_py"))
        assert(exact.forall(v => v >= 0.0 && v <= 1.0), vector.getKey)
        exact.indices
          .filter(k => exact(k) > 0.0 && (k == 1 || k == 3 || exact(k) >= 1e-7))
          .foreach { k =>
            assert(
              math.abs(theirs(k) - exact(k)) <= 1e-8 * exact(k),
              s"${vector.getKey} row $i tail $k (X-024)"
            )
          }
      }
    }
  }

  test("logistic slice 2d: standardized measures agree with R pprof; controls fall far outside") {
    def ratio(cls: String, actual: Array[Double], expected: Array[Double]): Double =
      Tolerances(cls).worstRatio(actual, expected)
    Fixtures.cases("logistic").foreach { name =>
      val py = Fixtures.json(s"logistic/$name/pprof_py.json")
      val r = Fixtures.json(s"logistic/$name/r_logistic.json")
      val standard = py.get("standardization")
      if (r.has("r_sm")) {
        val table = standard.get("measures").get("median")
        Seq(
          "indirect_ratio" -> "indirect",
          "indirect_rate" -> "indirect",
          "direct_ratio" -> "direct",
          "direct_rate" -> "direct"
        )
          .foreach { case (measure, kind) =>
            val agreement =
              ratio(
                "T-meas",
                Fixtures.doubles(table.get(kind).get(measure)),
                Fixtures.doubles(r.get("r_sm").get(measure))
              )
            assert(agreement <= 1.0, s"$name $measure vs R: $agreement")
          }
      }
      val controls = py.get("standardization_controls")
      val auto = Fixtures.doubles(standard.get("tests").get("direct_rate_auto").get("z"))
      val expected =
        Fixtures.doubles(standard.get("measures").get("median").get("indirect").get("expected"))
      assert(
        ratio(
          "T-meas",
          Fixtures.doubles(controls.get("reference_shifted").get("indirect_expected")),
          expected
        ) >= Margin
      )
      Seq(
        Fixtures.doubles(controls.get("reference_shifted").get("direct_rate_z")),
        Fixtures.doubles(controls.get("flipped_outcome").get("direct_rate_z")),
        Fixtures.doubles(standard.get("tests").get("direct_rate_log").get("z"))
      ).foreach(control => assert(ratio("T-test", control, auto) >= Margin, name))
    }
  }

  test(
    "three-stage slice 2f-1: preparation matches R's glmm.data.prep; stage 1 agrees with R; controls"
  ) {
    def ratio(cls: String, actual: Array[Double], expected: Array[Double]): Double =
      Tolerances(cls).worstRatio(actual, expected)
    def texts(node: JsonNode): Seq[String] = (0 until node.size).map(node.get(_).asText())
    val cases = Fixtures.cases("three-stage")
    assertEquals(cases, Seq("ts-golden-prep", "ts-golden", "ts-synthetic", "ts-shuffled"))
    cases.foreach { name =>
      val py = Fixtures.json(s"three-stage/$name/pprof_py.json")
      val r = Fixtures.json(s"three-stage/$name/r_threestage.json")
      val prep = py.get("preparation")
      val tight = py.get("stage1").get("tight")
      if (r.has("prep")) {
        val rp = r.get("prep")
        Seq("rid", "cell", "included").foreach(f =>
          assertEquals(texts(prep.get(f)), texts(rp.get(f)), s"$name $f")
        )
        assert(
          ratio(
            "T-meas",
            Fixtures.doubles(prep.get("y_adj")),
            Fixtures.doubles(rp.get("y_adj"))
          ) <= 1.0
        )
        assertEquals(prep.get("cells").asInt, rp.get("cells_present").asInt)
      }
      val beta = Fixtures.doubles(tight.get("beta"))
      assert(
        ratio("T-coef", beta, Fixtures.doubles(r.get("glm_stage1").get("beta"))) <= 1.0,
        s"$name beta vs glm"
      )
      if (r.has("stage23"))
        assert(
          ratio("T-coef", beta, Fixtures.doubles(r.get("stage23").get("beta"))) <= 1.0,
          s"$name beta vs R"
        )
      val controls = py.get("negative_controls")
      val cutoff = controls.get("cutoff_11")
      assert(
        (cutoff.get("kept").asInt, cutoff.get("included_cells").asInt) != (
          prep.get("kept").asInt,
          prep.get("included_cells").asInt
        )
      )
      assert(
        ratio(
          "T-coef",
          Fixtures.doubles(controls.get("flipped_outcome").get("beta")),
          beta
        ) >= Margin,
        name
      )
    }
  }

  test(
    "three-stage slice 2f-2: stage 3 is optimal at its tight fit, tends to the sigma = 0 limit; controls; rules"
  ) {
    def ratio(cls: String, actual: Array[Double], expected: Array[Double]): Double =
      Tolerances(cls).worstRatio(actual, expected)
    Seq("ts-golden", "ts-synthetic").foreach { name =>
      val s3 = Fixtures.json(s"three-stage/$name/pprof_py.json").get("stage3")
      assert(
        Fixtures
          .doubles(s3.get("function").get("tight").get("score"))
          .forall(v => math.abs(v) <= 1e-9),
        name
      )
      val small = Fixtures.doubles(s3.get("small_sigma").get("gamma"))
      val limit = Fixtures.doubles(s3.get("zero_sigma_limit"))
      assert(small.indices.forall(i => math.abs(small(i) - limit(i)) <= 1e-6), name)
      val tight = Fixtures.doubles(s3.get("tight").get("gamma"))
      s3.get("negative_controls").properties().asScala.foreach { control =>
        assert(
          ratio("T-coef", Fixtures.doubles(control.getValue), tight) >= Margin,
          s"$name ${control.getKey}"
        )
      }
    }
    val rules = Fixtures.json("three-stage/gauss-hermite.json")
    rules.properties().asScala.foreach { rule =>
      Seq("nodes", "weights").foreach { part =>
        val agreement = ratio(
          "T-fn",
          Fixtures.doubles(rule.getValue.get(s"numpy_$part")),
          Fixtures.doubles(rule.getValue.get(part))
        )
        assert(agreement <= 1.0, s"${rule.getKey} $part")
      }
    }
  }

  test(
    "three-stage slice 2f-3: stage 2 agrees with glmer under T-opt on integral outcomes; controls fall outside"
  ) {
    def ratio(cls: String, actual: Array[Double], expected: Array[Double]): Double =
      Tolerances(cls).worstRatio(actual, expected)
    Seq("ts-golden", "ts-synthetic", "ts-shuffled").foreach { name =>
      val s2 = Fixtures.json(s"three-stage/$name/pprof_py.json").get("stage2")
      val r = Fixtures.json(s"three-stage/$name/r_threestage.json").get("glmer_stage2")
      val tight = s2.get("tight")
      val fractional = Fixtures.doubles(r.get("saturated"))(0) != 0.0
      Seq("sigma", "intercept", "blup_providers", "blup_clusters").foreach { q =>
        if (!fractional)
          assert(
            ratio("T-opt", Fixtures.doubles(tight.get(q)), Fixtures.doubles(r.get(q))) <= 1.0,
            s"$name $q"
          )
        assert(
          ratio(
            "T-opt",
            Fixtures.doubles(s2.get("default").get(q)),
            Fixtures.doubles(tight.get(q))
          ) <= 1.0,
          s"$name $q default"
        )
      }
      val reference =
        Fixtures.doubles(tight.get("sigma")) ++ Fixtures.doubles(tight.get("intercept"))
      s2.get("negative_controls").properties().asScala.foreach { control =>
        val moved = Fixtures.doubles(control.getValue.get("sigma")) ++ Fixtures.doubles(
          control.getValue.get("intercept")
        )
        assert(ratio("T-opt", moved, reference) >= Margin, s"$name ${control.getKey}")
      }
    }
  }

  test(
    "three-stage slice 2f-4a: profile intervals nest; sensitivity matches the profile; controls fall outside"
  ) {
    Seq("ts-golden", "ts-synthetic", "ts-shuffled").foreach { name =>
      val inf = Fixtures.json(s"three-stage/$name/pprof_py.json").get("inference")
      val p95 = Fixtures.doubles(inf.get("profile").get("0.95"))
      val p90 = Fixtures.doubles(inf.get("profile").get("0.9"))
      assert(p95(1) <= p90(1) && p90(1) <= p90(0) && p90(0) <= p90(2) && p90(2) <= p95(2), name)
      if (inf.get("sensitivity").has("sigma"))
        assertEquals(
          Fixtures.doubles(inf.get("sensitivity").get("sigma")).toSeq,
          Seq(p95(1), p95(0), p95(2))
        )
      else
        assert(
          p95(1) == 0.0,
          s"$name: pprof_py's sensitivity failed although the interval stays above 0"
        )
      val exact = Fixtures.doubles(inf.get("tests").get("exact_two_sided").get("z"))
      inf.get("negative_controls").properties().asScala.foreach { control =>
        val miss =
          Tolerances("T-test").worstRatio(Fixtures.doubles(control.getValue.get("z")), exact)
        assert(miss >= Margin, s"$name ${control.getKey}")
      }
    }
  }

  test(
    "linear inputs are exact: features on a 1/64 grid (x3 on 1/1024), outcomes on a 1/1024 grid"
  ) {
    Fixtures.cases("linear").foreach { name =>
      // Read directly: lin-text's provider keys are strings, which Fixtures.csv does not parse.
      val lines = Files
        .readAllLines(Fixtures.root.resolve(s"linear/$name/input.csv"), StandardCharsets.UTF_8)
        .asScala
        .toVector
        .filter(_.nonEmpty)
      val header = lines.head.split(",").toVector
      val rows = lines.tail.map(_.split(",", -1))
      def column(c: String): Vector[Double] = rows.map(r => r(header.indexOf(c)).toDouble)
      def onGrid(c: String, scale: Double): Boolean =
        column(c).forall(v => v * scale == math.rint(v * scale))
      assert(
        onGrid("y", 1024.0) && onGrid("x1", 64.0) && onGrid("x2", 64.0) && onGrid("x3", 1024.0),
        name
      )
      assert(rows.forall(r => r(header.indexOf("provider")).nonEmpty), name)
    }
  }

  test(
    "linear random-effect inputs are exact: features, weights and offsets on a 1/64 grid, outcomes on 1/1024"
  ) {
    Fixtures.cases("linear-re").foreach { name =>
      val lines = Files
        .readAllLines(Fixtures.root.resolve(s"linear-re/$name/input.csv"), StandardCharsets.UTF_8)
        .asScala
        .toVector
        .filter(_.nonEmpty)
      val header = lines.head.split(",").toVector
      val rows = lines.tail.map(_.split(",", -1))
      def onGrid(c: String, scale: Double): Boolean =
        rows.forall { r =>
          val v = r(header.indexOf(c)).toDouble
          v * scale == math.rint(v * scale)
        }
      val grids = Seq("y" -> 1024.0, "x1" -> 64.0, "x2" -> 64.0, "x3" -> 1024.0) ++
        Seq("z", "w", "o").filter(header.contains).map(_ -> 64.0)
      grids.foreach { case (c, scale) => assert(onGrid(c, scale), s"$name $c") }
      assert(rows.forall(r => r(header.indexOf("provider")).nonEmpty), name)
    }
  }

  test(
    "linear slice 4a-1: pprof_py and lme4 agree at their tight optima within D-53's classes; controls fall outside"
  ) {
    def ratio(cls: String, actual: JsonNode, expected: JsonNode): Double =
      Tolerances(cls).worstRatio(Fixtures.doubles(actual), Fixtures.doubles(expected))
    val classes = Seq("beta", "blups", "predict_fixed", "predict_re", "fitted").map(
      _ -> "T-coef"
    ) ++
      Seq("theta", "sigma", "sigma_alpha", "var_beta", "pwrss", "ldL2", "ussq").map(_ -> "T-var") ++
      Seq("deviance", "loglik", "aic", "bic").map(_ -> "T-fn")
    Fixtures.cases("linear-re").foreach { name =>
      val py = Fixtures.json(s"linear-re/$name/pprof_py.json")
      val r = Fixtures.json(s"linear-re/$name/r_lme4.json")
      // lre-shifted's references lose precision to its conditioning (spec 4a-1 §13, D-53): informational there.
      if (name != "lre-shifted") Seq("reml", "ml").foreach { crit =>
        classes.foreach { case (q, cls) =>
          val agreement =
            ratio(cls, py.get(crit).get("tight").get(q), r.get(crit).get("tight").get(q))
          assert(agreement <= 1.0, s"$name $crit $q: pprof_py vs lme4 is $agreement of $cls")
        }
        val atTheta =
          ratio(
            "T-fn",
            py.get(crit).get("function").get("deviance"),
            r.get(crit).get("function").get("deviance")
          )
        assert(atTheta <= 1.0, s"$name $crit deviance at fixed theta: $atTheta of T-fn")
      }
      val tight = py.get("reml").get("tight")
      val boundary = Fixtures.doubles(tight.get("theta")).head == 0.0
      val moved = Seq("beta" -> "T-coef", "deviance" -> "T-fn") ++
        (if (boundary) Nil else Seq("theta" -> "T-var", "blups" -> "T-coef"))
      py.get("negative_controls").properties().asScala.foreach { control =>
        moved.foreach { case (q, cls) =>
          val miss = ratio(cls, control.getValue.get(q), tight.get(q))
          assert(miss >= Margin, s"$name $q: ${control.getKey} is $miss of $cls")
        }
      }
    }
  }

  test(
    "linear slice 4a-2: pprof_py's inference agrees with base R on lme4's tight fit; measures and controls (D-55)"
  ) {
    def h(node: JsonNode): Array[Double] = Fixtures.doubles(node)
    def ratio(cls: String, actual: Array[Double], expected: Array[Double]): Double =
      Tolerances(cls).worstRatio(actual, expected)
    val threshold = Tolerances.number("T-p.log10.threshold")
    val atol = Tolerances.number("T-p.log10.atol")
    val rtol = Tolerances.number("T-test.rtol")
    val band = Tolerances.number("T-flag.boundary.rtol") * 1.959963984540054
    def pOk(x: Double, y: Double): Boolean =
      if (y >= threshold) math.abs(x - y) <= rtol * math.abs(y)
      else if (y == 0.0) x < 1e-300
      else x > 0.0 && math.abs(math.log10(x) - math.log10(y)) <= atol
    Fixtures.cases("linear-re").foreach { name =>
      val inf = Fixtures.json(s"linear-re/$name/inference_pprof_py.json")
      val r = Fixtures.json(s"linear-re/$name/r_lme4.json")
      val py = Fixtures.json(s"linear-re/$name/pprof_py.json")
      val weighted = Fixtures.json(s"linear-re/$name/case.json").has("weights")
      val conditioned = name != "lre-shifted" // its references lose precision (D-53)
      val boundary = h(py.get("reml").get("tight").get("theta")).head == 0.0
      val sizes = h(py.get("summaries").get("W_j")).indices
        .map(i => py.get("summaries").get("n_j").get(i).asDouble)
      Seq("reml", "ml").filter(inf.has).foreach { crit =>
        val q = inf.get(crit)
        val e = r.get(crit).get("inference")
        if (conditioned) {
          Seq("two_sided", "greater", "less").foreach { alt =>
            val s = q.get(s"summary_$alt")
            assert(
              ratio("T-test", h(s.get("stat")), h(e.get("summary_stat"))) <= 1.0,
              s"$name $crit $alt t"
            )
            h(s.get("p_value")).zip(h(e.get(s"summary_p_$alt"))).foreach { case (x, y) =>
              assert(
                math.abs(x - y) <= 5e-8,
                s"$name $crit $alt p: $x vs $y beyond pprof_py's rounding (X-041)"
              )
            }
          }
          Seq(
            ("two_sided", "ci_lower", "summary_lower_two_sided"),
            ("two_sided", "ci_upper", "summary_upper_two_sided"),
            ("greater", "ci_lower", "summary_lower_greater"),
            ("less", "ci_upper", "summary_upper_less")
          ).foreach { case (alt, k, ek) =>
            assert(
              ratio("T-coef", h(q.get(s"summary_$alt").get(k)), h(e.get(ek))) <= 1.0,
              s"$name $crit $alt $k"
            )
          }
          h(q.get("fixed_effects_table").get("p")).zip(h(e.get("fet_p"))).foreach { case (x, y) =>
            assert(pOk(x, y), s"$name $crit fixed_effects_table p: $x vs $y")
          }
          assert(
            ratio("T-coef", h(q.get("conf_int").get("lower")), h(e.get("conf_lower"))) <= 1.0,
            s"$name conf_int"
          )
          assert(
            ratio("T-coef", h(q.get("conf_int").get("upper")), h(e.get("conf_upper"))) <= 1.0,
            s"$name conf_int"
          )
          if (!weighted && !boundary) Seq("zero", "median").foreach { label =>
            val t = q.get("test").get(label)
            val z = h(e.get(s"test_${label}_z"))
            assert(ratio("T-test", h(t.get("z_raw")), z) <= 1.0, s"$name $crit test $label z")
            h(t.get("p_value")).zip(h(e.get(s"test_${label}_p"))).foreach { case (x, y) =>
              assert(pOk(x, y), s"$name $crit test $label p: $x vs $y")
            }
            h(t.get("flag")).zip(h(e.get(s"test_${label}_flag"))).zip(z).foreach {
              case ((x, y), zz) =>
                assert(
                  x == y || math.abs(math.abs(zz) - 1.959963984540054) <= band,
                  s"$name $crit test $label flag"
                )
            }
            assert(
              ratio("T-coef", h(t.get("ci_lower")), h(e.get(s"test_${label}_lower"))) <= 1.0,
              s"$name $label lower"
            )
            assert(
              ratio("T-coef", h(t.get("ci_upper")), h(e.get(s"test_${label}_upper"))) <= 1.0,
              s"$name $label upper"
            )
          }
        }
        val b = h(py.get(crit).get("tight").get("blups"))
        val mean = b.indices.map(i => sizes(i) * b(i)).sum / sizes.sum
        val sorted = b.sorted
        val median =
          if (sorted.length % 2 == 1) sorted(sorted.length / 2)
          else 0.5 * (sorted(sorted.length / 2 - 1) + sorted(sorted.length / 2))
        Seq("median" -> median, "mean" -> mean, "zero" -> 0.0).foreach { case (label, g0) =>
          val m = q.get(s"measures_$label")
          val indirect =
            ratio("T-meas", h(m.get("indirect").get("indirect_difference")), b.map(_ - g0))
          val direct = ratio("T-meas", h(m.get("direct").get("direct_difference")), b.map(_ - mean))
          assert(
            indirect <= 1.0 && direct <= 1.0,
            s"$name $crit measures at $label: $indirect, $direct of T-meas"
          )
        }
      }
      if (!boundary) inf.get("negative_controls").properties().asScala.foreach { control =>
        val miss = ratio(
          "T-test",
          h(control.getValue.get("test_z")),
          h(inf.get("reml").get("test").get("zero").get("z_raw"))
        )
        assert(miss >= Margin, s"$name test z: ${control.getKey} is $miss of T-test")
      }
    }
  }

  test(
    "linear slice 3a: pprof_py and R's lm agree within the tolerance classes; controls fall far outside"
  ) {
    def ratio(cls: String, actual: JsonNode, expected: JsonNode): Double =
      Tolerances(cls).worstRatio(Fixtures.doubles(actual), Fixtures.doubles(expected))
    val threshold = Tolerances.number("T-p.log10.threshold")
    val atol = Tolerances.number("T-p.log10.atol")
    // T-p where pprof_py's p-value exceeds 1e-8 (X-031).
    def pRatio(actual: Array[Double], expected: Array[Double]): Double =
      actual
        .zip(expected)
        .filter(_._1 > 1e-8)
        .map { case (a, e) =>
          if (a < threshold && e < threshold) 0.0
          else if (a < threshold || e < threshold) Double.PositiveInfinity
          else math.abs(math.log10(a) - math.log10(e)) / atol
        }
        .foldLeft(0.0)((x, y) => math.max(x, y))
    Fixtures.cases("linear").foreach { name =>
      val py = Fixtures.json(s"linear/$name/pprof_py.json")
      val r = Fixtures.json(s"linear/$name/r_linear.json")
      val fit = py.get("complete")
      val summary = py.get("summary")
      def check(label: String, cls: String, a: JsonNode, e: JsonNode): Unit = {
        val agreement = ratio(cls, a, e)
        assert(agreement <= 1.0, s"$name $label: pprof_py vs R is $agreement of $cls")
      }
      Seq(
        ("beta", "T-coef", "beta"),
        ("gamma", "T-coef", "gamma"),
        ("var_beta", "T-var", "var_beta"),
        ("var_gamma", "T-var", "var_gamma"),
        ("sigma", "T-fn", "sigma"),
        ("rss", "T-fn", "rss"),
        ("loglik", "T-fn", "loglik"),
        ("aic", "T-fn", "aic"),
        ("bic", "T-fn", "bic")
      ).foreach { case (q, cls, rq) => check(q, cls, fit.get(q), r.get(rq)) }
      check(
        "simplified var_gamma",
        "T-var",
        py.get("simplified").get("var_gamma"),
        r.get("var_gamma_simplified")
      )
      Seq(
        ("two_sided", "stat", "T-test", "t"),
        ("two_sided", "ci_lower", "T-coef", "ci_lower"),
        ("two_sided", "ci_upper", "T-coef", "ci_upper"),
        ("greater", "stat", "T-test", "greater_stat"),
        ("greater", "ci_lower", "T-coef", "greater_lower"),
        ("less", "stat", "T-test", "less_stat"),
        ("less", "ci_upper", "T-coef", "less_upper")
      ).foreach { case (alt, col, cls, rq) =>
        check(s"$alt $col", cls, summary.get(alt).get(col), r.get(rq))
      }
      check("predict", "T-meas", py.get("predict"), r.get("predict"))
      check("R^2", "T-fn", py.get("score"), r.get("r2"))
      Seq("within_xx", "within_xy")
        .foreach(q => check(q, "T-fn", py.get("function").get(q), r.get(q)))
      assertEquals(fit.get("df").asInt, r.get("df").asInt, name)
      Seq("two_sided" -> "p", "greater" -> "greater_p", "less" -> "less_p").foreach {
        case (alt, rq) =>
          val agreement =
            pRatio(Fixtures.doubles(summary.get(alt).get("p_value")), Fixtures.doubles(r.get(rq)))
          assert(agreement <= 1.0, s"$name $alt p-values: $agreement of T-p")
      }
      py.get("negative_controls").properties().asScala.foreach { control =>
        Seq(
          "beta" -> "T-coef",
          "gamma" -> "T-coef",
          "var_beta" -> "T-var",
          "var_gamma" -> "T-var",
          "sigma" -> "T-fn",
          "aic" -> "T-fn",
          "bic" -> "T-fn"
        ).foreach { case (q, cls) =>
          val miss = ratio(cls, control.getValue.get(q), fit.get(q))
          assert(miss >= Margin, s"$name $q: ${control.getKey} is $miss of $cls")
        }
        if (control.getValue.has("function")) Seq("within_xx", "within_xy").foreach { q =>
          val miss =
            ratio("T-fn", control.getValue.get("function").get(q), py.get("function").get(q))
          assert(miss >= Margin, s"$name $q: ${control.getKey} is $miss of T-fn")
        }
      }
    }
  }

  test(
    "linear slice 3b: pprof_py agrees with base R and R pprof where X-036 allows; controls fall far outside"
  ) {
    val critical = Map(
      "two_sided" -> 1.959963984540054,
      "mean" -> 1.959963984540054,
      "number" -> 1.959963984540054,
      "greater" -> 1.6448536269514722,
      "less" -> 1.6448536269514722,
      "critical" -> 2.5
    )
    val threshold = Tolerances.number("T-p.log10.threshold")
    val atol = Tolerances.number("T-p.log10.atol")
    val testRtol = Tolerances.number("T-test.rtol")
    val band = Tolerances.number("T-flag.boundary.rtol")
    def d(node: JsonNode): Array[Double] = Fixtures.doubles(node)
    // Elements finite in both; other positions must match, except pprof_py's NaN (X-035), which is counted.
    def masked(cls: String, a: Array[Double], e: Array[Double]): (Double, Int) = {
      assertEquals(a.length, e.length)
      val keep =
        a.indices.filter(i => java.lang.Double.isFinite(a(i)) && java.lang.Double.isFinite(e(i)))
      val untested = a.indices.count(i => a(i).isNaN && !e(i).isNaN)
      val mismatched = a.indices.exists { i =>
        val finite = java.lang.Double.isFinite(a(i)) && java.lang.Double.isFinite(e(i))
        !finite && !(a(i).isNaN && !e(i).isNaN) && !(a(i) == e(i) || (a(i).isNaN && e(i).isNaN))
      }
      val ratio =
        if (keep.isEmpty) 0.0
        else Tolerances(cls).worstRatio(keep.map(a).toArray, keep.map(e).toArray)
      (if (mismatched) Double.PositiveInfinity else ratio, untested)
    }
    // §8.4's T-p, skipping pprof_py's NaN (X-035).
    def pRatio(a: Array[Double], e: Array[Double]): Double =
      a.zip(e)
        .filterNot(_._1.isNaN)
        .map { case (x, y) =>
          if (y >= threshold) math.abs(x - y) / (testRtol * math.abs(y))
          else if (y == 0.0) { if (x < 1e-300) 0.0 else Double.PositiveInfinity }
          else if (x > 0) math.abs(math.log10(x) - math.log10(y)) / atol
          else Double.PositiveInfinity
        }
        .foldLeft(0.0)((u, v) => math.max(u, v))
    def flagsOutside(f: Array[Double], r: Array[Double], z: Array[Double], c: Double): Int =
      f.indices.count(i => !f(i).isNaN && f(i) != r(i) && math.abs(math.abs(z(i)) - c) > band * c)
    Fixtures.cases("linear").foreach { name =>
      val py = Fixtures.json(s"linear/$name/inference_pprof_py.json")
      val r = Fixtures.json(s"linear/$name/inference_r.json")
      def agree(label: String, cls: String, a: Array[Double], e: Array[Double]): Int = {
        val (ratio, untested) = masked(cls, a, e)
        assert(ratio <= 1.0, s"$name $label: $ratio of $cls")
        untested
      }
      Seq("median", "mean").foreach(ref =>
        agree(s"reference $ref", "T-coef", d(py.get("references").get(ref)), d(r.get(s"ref_$ref")))
      )
      val two = py.get("tests").get("two_sided")
      var untested = 0
      py.get("tests").properties().asScala.foreach { entry =>
        val v = entry.getKey
        val t = entry.getValue
        Seq(
          ("z_raw", "z", "T-test"),
          ("ci_lower", "lower", "T-coef"),
          ("ci_upper", "upper", "T-coef")
        ).foreach { case (col, rq, cls) =>
          untested = math.max(
            untested,
            agree(s"test $v $col", cls, d(t.get(col)), d(r.get(s"test_${v}_$rq")))
          )
        }
        val p = pRatio(d(t.get("p_value")), d(r.get(s"test_${v}_p")))
        assert(p <= 1.0, s"$name test $v p-values: $p of T-p")
        val outside = flagsOutside(
          d(t.get("flag")),
          d(r.get(s"test_${v}_flag")),
          d(r.get(s"test_${v}_z")),
          critical(v)
        )
        assertEquals(outside, 0, s"$name test $v flags")
      }
      assertEquals(
        untested,
        if (name == "lin-extreme") 1 else 0,
        s"$name: providers untested by pprof_py (X-035)"
      )
      if (py.has("subset")) {
        val subset = py.get("subset")
        def first(node: JsonNode): Seq[String] = (0 until 5).map(i => node.get(i).asText())
        assertEquals(first(subset.get("providers")), first(py.get("providers")))
        Seq("z_raw", "p_value", "flag", "ci_lower", "ci_upper").foreach(col =>
          assertEquals(first(subset.get(col)), first(two.get(col)), s"$name subset $col")
        )
      }
      Seq("lower", "upper").foreach(side =>
        agree(
          s"gamma interval $side",
          "T-coef",
          d(py.get("intervals").get("gamma").get(side)),
          d(r.get(s"gamma_$side"))
        )
      )
      py.get("intervals").properties().asScala.filter(_.getKey.startsWith("sm_")).foreach { sm =>
        for (kind <- Seq("indirect", "direct"); side <- Seq("lower", "upper"))
          agree(
            s"${sm.getKey} $kind $side",
            "T-meas",
            d(sm.getValue.get(kind).get(side)),
            d(r.get(s"${sm.getKey}_${kind}_$side"))
          )
      }
      py.get("measures").properties().asScala.foreach { ref =>
        ref.getValue.properties().asScala.foreach { kind =>
          kind.getValue.properties().asScala.foreach { col =>
            val key =
              s"measures_${ref.getKey}_${kind.getKey}_${col.getKey.replace(kind.getKey + "_", "")}"
            val expected = d(r.get(key))
            val direct = kind.getKey == "direct" && col.getKey == "observed"
            agree(
              s"measures ${ref.getKey} ${kind.getKey} ${col.getKey}",
              "T-meas",
              d(col.getValue),
              if (direct) expected.take(1) else expected
            )
          }
        }
      }
      val nullValue = d(two.get("null_value"))(0)
      val statistic =
        d(py.get("estimate")).zip(d(py.get("se"))).map { case (e, s) => (e - nullValue) / s }
      agree("R pprof statistic", "T-test", statistic, d(r.get("rpprof_stat")))
      assertEquals(
        flagsOutside(
          d(two.get("flag")),
          d(r.get("rpprof_flag")),
          d(r.get("test_two_sided_z")),
          critical("two_sided")
        ),
        0,
        s"$name R pprof flags"
      )
      val median = py.get("measures").get("median")
      Seq(
        ("indirect", "indirect_difference", "rpprof_indirect_difference"),
        ("indirect", "observed", "rpprof_indirect_observed"),
        ("indirect", "expected", "rpprof_indirect_expected"),
        ("direct", "direct_difference", "rpprof_direct_difference"),
        ("direct", "expected", "rpprof_direct_expected")
      ).foreach { case (kind, col, key) =>
        agree(s"R pprof $kind $col", "T-meas", d(median.get(kind).get(col)), d(r.get(key)))
      }
      val c = py.get("negative_controls")
      Seq(
        ("z_raw", "T-test", d(c.get("reference_shifted").get("z_raw")), d(two.get("z_raw"))),
        (
          "indirect_difference",
          "T-meas",
          d(c.get("reference_shifted").get("indirect_difference")),
          d(median.get("indirect").get("indirect_difference"))
        ),
        ("ci_lower", "T-coef", d(c.get("level_094").get("ci_lower")), d(two.get("ci_lower"))),
        (
          "gamma upper",
          "T-coef",
          d(c.get("level_094").get("gamma_upper")),
          d(py.get("intervals").get("gamma").get("upper"))
        )
      ).foreach { case (label, cls, a, e) =>
        val miss = masked(cls, a, e)._1
        assert(miss >= Margin, s"$name control $label: $miss of $cls")
      }
      assert(
        pRatio(d(c.get("reference_shifted").get("p_value")), d(two.get("p_value"))) >= Margin,
        s"$name control p"
      )
    }
  }
}
