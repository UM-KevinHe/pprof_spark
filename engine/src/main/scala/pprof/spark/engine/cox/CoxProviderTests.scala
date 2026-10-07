package pprof.spark.engine.cox

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

import pprof.spark.numerics.PoissonTests

/** The method of a provider test (Phase 1d specification §2). */
sealed abstract class TestMethod(val name: String) extends Product with Serializable

object TestMethod {

  /** The two-sided mid-p test with the theoretical null, pprof_py's default. */
  case object MidP extends TestMethod("midp")

  /** The exact Poisson test, with χ²-based or Byar limits. */
  case object Exact extends TestMethod("exact")
}

/** Provider tests of the indirect standardized ratio against 1 (Phase 1d specification §2, §3), as
  * pprof_py's `CoxPH.test` with the theoretical null. The empirical null is deferred (§4, Later).
  */
object CoxProviderTests {

  /** pprof_py's `PROVIDER_TEST_COLUMNS`, then observed, expected and person-time. */
  val Columns: Seq[String] = Seq(
    "estimate",
    "se",
    "null_value",
    "transformed",
    "se_transformed",
    "null_transformed",
    "z_raw",
    "null_mean",
    "null_sd",
    "null_group",
    "z_adjusted",
    "p_value",
    "flag",
    "ci_lower",
    "ci_upper",
    "observed",
    "expected",
    "person_time"
  )

  /** One row per provider of `df`, which must be the data `fit` was fitted to (API-3), with the
    * provider column and [[Columns]]. Flags are +1 or −1 when the p-value is below 1 − level.
    */
  def test(
      df: DataFrame,
      fit: CoxFit,
      provider: String,
      method: TestMethod = TestMethod.MidP,
      level: Double = 0.95,
      providers: Option[Seq[Any]] = None
  ): DataFrame = {
    require(level > 0.0 && level < 1.0, s"level must lie in (0, 1), got $level")
    val measures =
      CoxMeasures.standardized(df, fit, provider, Seq(Standardization.Indirect), providers)(
        Standardization.Indirect
      )
    val name = method.name
    val compute = udf((observed: Long, expected: Double) =>
      PoissonTests.test(observed.toDouble, expected, name, level)
    )
    val nan = lit(Double.NaN)
    measures
      .withColumn("__pprof_test", compute(col("observed"), col("expected")))
      .select(
        col(provider),
        col("__pprof_test.estimate").as("estimate"),
        nan.as("se"),
        lit(1.0).as("null_value"),
        nan.as("transformed"),
        nan.as("se_transformed"),
        nan.as("null_transformed"),
        col("__pprof_test.zRaw").as("z_raw"),
        lit(0.0).as("null_mean"),
        lit(1.0).as("null_sd"),
        lit(null).cast(IntegerType).as("null_group"),
        col("__pprof_test.zRaw").as("z_adjusted"),
        col("__pprof_test.pValue").as("p_value"),
        col("__pprof_test.flag").as("flag"),
        col("__pprof_test.ciLower").as("ci_lower"),
        col("__pprof_test.ciUpper").as("ci_upper"),
        col("observed").cast(DoubleType).as("observed"),
        col("expected"),
        col("person_time")
      )
  }

}
