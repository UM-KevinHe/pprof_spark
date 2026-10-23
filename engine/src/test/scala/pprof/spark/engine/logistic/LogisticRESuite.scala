package pprof.spark.engine.logistic

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.DataFrame
import pprof.spark.engine.data.InvalidInputException
import pprof.spark.numerics.kernels.{LogisticRE => Kernel}
import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** Slice 4b-1 (docs/spec/logistic/random-effect-estimation.md §10, D-58, D-59). Every case of `fixtures/logistic-re`
  * with nAGQ = 1 and 0 is fitted by the kernel on the driver against pprof_py's tightened optimum at T-est (T-opt where
  * σ̂ < 0.1) and lme4's at T-opt, with the optimality check of D-59. The engine's distributed fit then matches the
  * kernel's on three cases, with its fitted values, predictions (X-044) and residuals against the references. It is
  * invariant to row order, partitioning and shifting a feature, and refuses what spec §5 refuses.
  */
class LogisticRESuite extends SparkSuite {

  override val munitTimeout: scala.concurrent.duration.Duration =
    scala.concurrent.duration.Duration(10, "min")

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"logistic-re/$name/input.csv").toString)

  private def setup(name: String): (LogisticRESpec, Boolean) = {
    val c = Fixtures.json(s"logistic-re/$name/case.json")
    val features = c.get("features").elements().asScala.map(_.asText()).toSeq
    val spec = LogisticRESpec(
      "y",
      features,
      "provider",
      if (c.has("offset")) Some("o") else None,
      Some("id")
    )
    (spec, !c.has("intercept") || c.get("intercept").asBoolean())
  }

  private def h(node: JsonNode): Array[Double] = Fixtures.doubles(node)

  private def ratio(cls: String, actual: Seq[Double], expected: Array[Double]): Double =
    Tolerances(cls).worstRatio(actual.toArray, expected)

  /** A case's rows as one driver-side block in key order, with the engine's centring (§3). */
  private final class Local(
      val blocks: Kernel.LocalBlocks,
      val rows: Long,
      val m: Int,
      val p: Int,
      val centre: Array[Double],
      val ybar: Double
  )

  private def local(name: String): Local = {
    val (spec, intercept) = setup(name)
    val lines = Files
      .readAllLines(Fixtures.root.resolve(s"logistic-re/$name/input.csv"))
      .asScala
      .toVector
      .filter(_.nonEmpty)
    val header = lines.head.split(",").toVector
    val records = lines.tail.map(_.split(",", -1))
    val key = header.indexOf("provider")
    val numeric = records.forall(_(key).matches("-?\\d+"))
    val order =
      records.map(_(key)).distinct.sortWith((a, b) => if (numeric) a.toLong < b.toLong else a < b)
    val rank = order.zipWithIndex.toMap
    val sorted = records.zipWithIndex.sortBy { case (r, i) => (rank(r(key)), i) }.map(_._1)
    val p = spec.features.size + (if (intercept) 1 else 0)
    val xs = sorted.map(r =>
      (if (intercept) Vector(1.0) else Vector()) ++ spec.features.map(f =>
        r(header.indexOf(f)).toDouble
      )
    )
    val centre =
      Array.tabulate(p)(k => if (!intercept || k == 0) 0.0 else xs.map(_(k)).sum / xs.length)
    val offset = header.indexOf("o")
    val y = header.indexOf("y")
    val values = sorted
      .zip(xs)
      .flatMap { case (r, x) =>
        x ++ Vector(
          if (offset >= 0 && spec.offset.nonEmpty) r(offset).toDouble else 0.0,
          r(y).toDouble
        )
      }
      .toArray
    val starts = sorted.indices.filter(i => i == 0 || sorted(i)(key) != sorted(i - 1)(key)).toArray
    new Local(
      new Kernel.LocalBlocks(Seq(Kernel.Rows(starts, values, p)), p, centre),
      sorted.length.toLong,
      order.length,
      p,
      centre,
      sorted.map(_(y).toDouble).sum / sorted.length
    )
  }

  private def centred(l: Local, intercept: Boolean, beta: Array[Double]): Array[Double] = {
    val out = beta.clone()
    if (intercept) out(0) = beta(0) + (1 until l.p).map(k => l.centre(k) * beta(k)).sum
    out
  }

  private val cases = Seq(
    "lgre-base",
    "lgre-offset",
    "lgre-nointercept",
    "lgre-provider-level",
    "lgre-extreme",
    "lgre-small-sigma",
    "lgre-shifted",
    "lgre-text",
    "lgre-many",
    "lgre-fractional"
  )

  test(
    "every case, nAGQ 1 and 0: the kernel against pprof_py (T-est) and lme4 (T-opt), and the optimality check"
  ) {
    for (name <- cases; (stage, nagq1) <- Seq("nagq1" -> true, "nagq0" -> false)) {
      val (_, intercept) = setup(name)
      val l = local(name)
      val fit = Kernel.fit(l.blocks, l.rows, l.m, l.p, intercept, l.centre, l.ybar, nagq1)
      assert(fit.converged, s"$name $stage: ${fit.warnings}")
      val q = Map(
        "sigma" -> Seq(fit.sigma),
        "beta" -> fit.beta.toSeq,
        "blups" -> fit.blups.toSeq,
        "condvar" -> fit.conditionalVariances.toSeq,
        "var_beta" -> fit.covariance.toSeq,
        "deviance" -> Seq(fit.deviance),
        "loglik" -> Seq(fit.logLikelihood),
        "aic" -> Seq(fit.aic),
        "bic" -> Seq(fit.bic),
        "pwrss" -> Seq(fit.pwrss),
        "ldL2" -> Seq(fit.logDetH),
        "ussq" -> Seq(fit.ussq)
      )
      val py = Fixtures.json(s"logistic-re/$name/pprof_py.json").get(stage).get("tight")
      val r = Fixtures.json(s"logistic-re/$name/r_lme4.json").get(stage).get("tight")
      val cls = if (fit.sigma < 0.1) "T-opt" else "T-est" // a likelihood flat in σ near 0 (D-59)
      val lme4 =
        name != "lgre-fractional" && name != "lgre-shifted" // another model; conditioning (D-58)
      q.foreach { case (k, v) =>
        val value = ratio(cls, v, h(py.get(k)))
        assert(value <= 1.0, s"$name $stage $k against pprof_py: $value of $cls")
        if (lme4) {
          val other = ratio("T-opt", v, h(r.get(k)))
          assert(other <= 1.0, s"$name $stage $k against lme4: $other of T-opt")
        }
      }
      if (nagq1)
        (Seq("pprof_py" -> py) ++ (if (name != "lgre-fractional") Seq("lme4" -> r) else Nil))
          .foreach { case (who, t) =>
            val other = l.blocks
              .laplace(h(t.get("sigma")).head, centred(l, intercept, h(t.get("beta"))))
              .deviance
            assert(
              fit.deviance <= other + 1e-10 * math.abs(other),
              s"$name: ${fit.deviance} > $other at $who's optimum"
            )
          }
      else
        assert(
          fit.deviance <= h(py.get("deviance")).head * (1.0 + 1e-10),
          s"$name nAGQ = 0 against pprof_py"
        )
    }
  }

  for (name <- Seq("lgre-base", "lgre-offset", "lgre-text")) {
    test(
      s"$name: the engine's fit equals the kernel's, and its fitted values, predictions and residuals match"
    ) {
      val (spec, intercept) = setup(name)
      val df = input(name)
      val l = local(name)
      val text = Fixtures.json(s"logistic-re/$name/case.json").get("text").asBoolean()
      for ((stage, nagq1) <- Seq("nagq1" -> true, "nagq0" -> false)) {
        val fit =
          LogisticRE.fit(df, spec, LogisticREOptions(nagq1 = nagq1, includeIntercept = intercept))
        val kernel = Kernel.fit(l.blocks, l.rows, l.m, l.p, intercept, l.centre, l.ybar, nagq1)
        val blups =
          fit.providers.orderBy(spec.provider).select("blup").collect().toSeq.map(_.getDouble(0))
        Seq(
          "beta" -> (fit.estimates, kernel.beta.toSeq),
          "sigma and deviance" -> (
            Seq(fit.sigma, fit.deviance),
            Seq(kernel.sigma, kernel.deviance)
          ),
          "covariance" -> (fit.covariance, kernel.covariance.toSeq),
          "blups" -> (blups, kernel.blups.toSeq)
        ).foreach { case (what, (a, b)) =>
          val value = ratio("T-part.objective", a, b.toArray)
          assert(value <= 1.0, s"$name $stage $what: engine against kernel $value of T-part")
        }
        val head = df.orderBy("id").limit(100)
        def column(frame: DataFrame, c: String) =
          frame.orderBy("id").select(c).collect().toSeq.map(_.getDouble(0))
        val rows = LogisticRE.fitted(head, fit)
        val q = Map(
          "fitted" -> column(rows, "fitted"),
          "pearson" -> column(rows, "pearson"),
          "deviance_residuals" -> column(rows, "deviance_residual"),
          "predict_fixed" -> column(LogisticRE.predict(head, fit), "prediction"),
          "predict_re" -> column(LogisticRE.predict(head, fit, useRe = true), "prediction"),
          "predict_link_re" -> column(
            LogisticRE.predict(head, fit, useRe = true, `type` = "link"),
            "prediction"
          )
        )
        val py = Fixtures.json(s"logistic-re/$name/pprof_py.json").get(stage).get("tight")
        val r = Fixtures.json(s"logistic-re/$name/r_lme4.json").get(stage).get("tight")
        q.foreach { case (k, v) =>
          if (text || !k.startsWith("predict_re") && !k.startsWith("predict_link")) { // X-044
            val value = ratio("T-est", v, h(py.get(k)))
            assert(value <= 1.0, s"$name $stage $k against pprof_py: $value of T-est")
          }
          val other = ratio("T-opt", v, h(r.get(k)))
          assert(other <= 1.0, s"$name $stage $k against lme4: $other of T-opt")
        }
      }
    }
  }

  test(
    "lgre-shifted moves only the intercept from lgre-base, and the fit is invariant to row order and partitioning"
  ) {
    val (spec, _) = setup("lgre-base")
    val base = LogisticRE.fit(input("lgre-base"), spec)
    val shifted = LogisticRE.fit(input("lgre-shifted"), spec)
    def close(a: Double, b: Double): Boolean = math.abs(a - b) <= 1e-8 * math.max(math.abs(b), 1.0)
    assert(close(shifted.sigma, base.sigma) && close(shifted.deviance, base.deviance))
    assert(
      shifted.estimates.drop(1).zip(base.estimates.drop(1)).forall { case (a, b) => close(a, b) }
    )
    assert(close(shifted.estimates(0) + 1000.0 * shifted.estimates(1), base.estimates(0)))
    val shuffled =
      input("lgre-base").orderBy(org.apache.spark.sql.functions.rand(11L)).repartition(7)
    val again = LogisticRE.fit(shuffled, spec)
    def bits(v: Seq[Double]) = v.map(java.lang.Double.doubleToRawLongBits)
    def blups(f: LogisticREFit) =
      f.providers.orderBy("provider").select("blup").collect().toSeq.map(_.getDouble(0))
    assertEquals(bits(again.estimates), bits(base.estimates))
    assertEquals(bits(again.covariance), bits(base.covariance))
    assertEquals(bits(Seq(again.sigma, again.deviance)), bits(Seq(base.sigma, base.deviance)))
    assertEquals(again.fingerprint, base.fingerprint)
    assertEquals(bits(blups(again)), bits(blups(base)))
  }

  test("outcomes outside [0, 1], missing values, aliased features and one provider are refused") {
    val (spec, _) = setup("lgre-base")
    val df = input("lgre-base")
    import org.apache.spark.sql.functions.{col, lit, when}
    val outside = df.withColumn("y", when(col("id") < 3, lit(2.0)).otherwise(col("y")))
    val bad = intercept[InvalidInputException](LogisticRE.fit(outside, spec)).getMessage
    assert(bad.contains("has 3 values outside [0, 1]"), bad)
    val missing = df.withColumn("x1", when(col("id") < 4, lit(null)).otherwise(col("x1")))
    assert(
      intercept[InvalidInputException](LogisticRE.fit(missing, spec)).getMessage.contains("x1")
    )
    val aliased = df.withColumn("x3", col("x1") * 2.0)
    val named =
      intercept[LogisticREAliasingException](
        LogisticRE.fit(aliased, spec.copy(features = Seq("x1", "x2", "x3")))
      )
    assert(named.features.nonEmpty && named.features.forall(Set("x1", "x3")), named.getMessage)
    val single = df.withColumn("provider", lit(1))
    assert(
      intercept[InvalidInputException](LogisticRE.fit(single, spec)).getMessage.contains("provider")
    )
  }
}
