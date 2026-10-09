package pprof.spark.engine.linear

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** Metamorphic and layout properties of the linear fit and `score` (docs/spec/linear/fixed-effect-estimation.md §11). */
class LinearBehaviourSuite extends SparkSuite {

  private val spec = LinearSpec("y", Seq("x1", "x2", "x3"), "provider", Some("id"))

  private def input(name: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Fixtures.root.resolve(s"linear/$name/input.csv").toString)

  private def check(cls: String, what: String, actual: Seq[Double], expected: Seq[Double]): Unit = {
    val ratio = Tolerances(cls).worstRatio(actual.toArray, expected.toArray)
    assert(ratio <= 1.0, s"$what: worst ratio $ratio under $cls")
  }

  private def gamma(fit: LinearFit): Seq[Double] =
    fit.providers.orderBy(spec.provider).select("gamma").collect().map(_.getDouble(0)).toSeq

  test("adding c to the outcome moves every effect by c and leaves beta and sigma unchanged") {
    val df = input("lin-base")
    val a = LinearFE.fit(df, spec)
    val b = LinearFE.fit(df.withColumn("y", col("y") + 0.5), spec)
    check("T-part.parameters", "beta", b.estimates, a.estimates)
    check("T-part.parameters", "gamma - c", gamma(b).map(_ - 0.5), gamma(a))
    check("T-part.objective", "sigma", Seq(b.sigma), Seq(a.sigma))
  }

  test("scaling a feature by s divides its coefficient by s") {
    val df = input("lin-base")
    val a = LinearFE.fit(df, spec)
    val b = LinearFE.fit(df.withColumn("x2", col("x2") * 4.0), spec)
    check("T-part.parameters", "beta", b.estimates.updated(1, b.estimates(1) * 4.0), a.estimates)
    check("T-part.parameters", "gamma", gamma(b), gamma(a))
    check("T-part.objective", "sigma", Seq(b.sigma), Seq(a.sigma))
  }

  test(
    "duplicating every record leaves beta and gamma unchanged; sigma follows the degrees of freedom"
  ) {
    val df = input("lin-base")
    val a = LinearFE.fit(df, spec)
    val b = LinearFE.fit(df.union(df.withColumn("id", col("id") + 1000000L)), spec)
    check("T-part.parameters", "beta", b.estimates, a.estimates)
    check("T-part.parameters", "gamma", gamma(b), gamma(a))
    val expected = 2.0 * a.rss / (2.0 * a.rows - a.providerCount - spec.features.size)
    check("T-fn", "sigma squared", Seq(b.sigma * b.sigma), Seq(expected))
  }

  test("smaller blocks change the layout and the results only within T-part") {
    val df = input("lin-many")
    val a = LinearFE.fit(df, spec)
    val b = LinearFE.fit(df, spec, LinearOptions(blocks = BlockOptions(targetBlockBytes = 4096L)))
    assert(
      b.layout.blockCount > a.layout.blockCount,
      s"${b.layout.blockCount} blocks against ${a.layout.blockCount}"
    )
    check("T-part.parameters", "beta", b.estimates, a.estimates)
    check("T-part.parameters", "gamma", gamma(b), gamma(a))
    check("T-part.objective", "sigma", Seq(b.sigma), Seq(a.sigma))
  }

  test("score: the training rows give pprof_py's R²; any data is invariant to partitioning") {
    val df = input("lin-text")
    val fit = LinearFE.fit(df, spec)
    val score = LinearFE.score(df, fit)
    check(
      "T-fn",
      "R² vs pprof_py",
      Seq(score),
      Fixtures.doubles(Fixtures.json("linear/lin-text/pprof_py.json").get("score")).toSeq
    )
    check("T-fn", "R² vs the fit", Seq(score), Seq(fit.r2))
    val subset = df.filter(col("id") % 3 === 0)
    val again = LinearFE.score(subset.repartition(5).sortWithinPartitions(col("x1").desc), fit)
    assertEquals(
      java.lang.Double.doubleToRawLongBits(again),
      java.lang.Double.doubleToRawLongBits(LinearFE.score(subset, fit))
    )
  }
}
