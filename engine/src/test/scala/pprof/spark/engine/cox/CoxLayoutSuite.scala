package pprof.spark.engine.cox

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.cox.CoxFixtures._
import pprof.spark.testkit.{SparkSuite, Tolerances}

/** Distributed correctness of Cox fits beyond the default one-block layout (OI-67, round 79):
  * reference parity with many blocks, bitwise reproducibility across `spark.sql.shuffle.partitions`
  * and input order, and a skewed stratum-size distribution with an oversized stratum (§9.6).
  */
class CoxLayoutSuite extends SparkSuite {

  /** 36 rows per block at p = 3, so each 600-row stratified fixture spans about twenty blocks. */
  private val manyBlocks = BlockOptions(targetBlockBytes = 2048L)

  for (name <- Stratified; ties <- Methods)
    test(
      s"many blocks: function-level values and tight fits match pprof_py and R: $name, ${ties.name}"
    ) {
      val options = CoxOptions(ties = ties, blocks = manyBlocks)
      for (point <- Seq("beta_zero", "beta_fixed")) {
        val beta = doubles(reference(name, "pprof_py", ties).get(point), "beta")
        val e = CoxPH.evaluateAt(frame(spark, name), spec(name), beta, options)
        for (source <- Seq("pprof_py", "r_survival")) {
          val node = reference(name, source, ties).get(point)
          val what = s"$name ${ties.name} $point vs $source, many blocks"
          check("T-fn", s"$what loglik", Seq(e.value), doubles(node, "loglik"))
          check("T-fn", s"$what score", e.gradient.toSeq, doubles(node, "score"))
          check("T-fn", s"$what information", e.information.toSeq, doubles(node, "information"))
        }
      }
      val fit = CoxPH.fit(frame(spark, name), spec(name), tight(ties).copy(blocks = manyBlocks))
      assert(fit.converged, fit.message)
      assert(fit.layout.blockCount > 1, s"expected several blocks: ${fit.layout}")
      for (source <- Seq("pprof_py", "r_survival")) {
        val node = reference(name, source, ties).get("tight")
        val what = s"$name ${ties.name} vs $source, ${fit.layout.blockCount} blocks"
        check("T-coef", s"$what coefficients", fit.estimates, doubles(node, "coef"))
        check("T-var", s"$what standard errors", fit.standardErrors, doubles(node, "se"))
        check("T-var", s"$what covariance", fit.covariance, doubles(node, "covariance"))
        check("T-fn", s"$what loglik", Seq(fit.logLikelihood), doubles(node, "loglik"))
      }
    }

  /** Every bit of a fit with per-row robust variance and of its baseline hazard. */
  private def everything(df: DataFrame, name: String, ties: Ties): Seq[Long] = {
    val fit = CoxPH.fit(df, spec(name), CoxOptions(ties = ties, blocks = manyBlocks, robust = true))
    val baseline = CoxPH
      .baseline(df, fit)
      .orderBy("stratum", "time")
      .collect()
      .toSeq
      .flatMap(r =>
        Seq(
          r.getAs[Double]("time"),
          r.getAs[Double]("hazard_increment"),
          r.getAs[Double]("cumulative_hazard")
        )
      )
    bits(fit.estimates ++ fit.covariance ++ fit.information ++ baseline :+ fit.logLikelihood)
  }

  for (name <- Seq("rc-stratified-weights-offset", "lt-weights-offset"); ties <- Methods)
    test(
      s"R0: shuffle partitions 1, 4 and 7 and reversed, repartitioned input leave every bit: $name, ${ties.name}"
    ) {
      val expected =
        withSqlConf("spark.sql.shuffle.partitions" -> "4")(
          everything(frame(spark, name), name, ties)
        )
      val one =
        withSqlConf("spark.sql.shuffle.partitions" -> "1")(
          everything(frame(spark, name), name, ties)
        )
      val seven = withSqlConf("spark.sql.shuffle.partitions" -> "7")(
        everything(frame(spark, name, reverse = true).repartition(3), name, ties)
      )
      assertEquals(one, expected, "shuffle partitions 1 against 4")
      assertEquals(seven, expected, "shuffle partitions 7, reversed and repartitioned, against 4")
    }

  private val skewedSpec =
    CoxSpec("time", "event", Seq("x1", "x2"), strata = Some("stratum"), rowId = Some("id"))

  /** A deterministic skewed data set: one stratum of 400 rows, 10 of 40 and 40 of 4. */
  private lazy val skewedRows: Vector[Row] = {
    val sizes = Seq(400) ++ Seq.fill(10)(40) ++ Seq.fill(40)(4)
    val random = new scala.util.Random(79L)
    var id = 0L
    sizes.zipWithIndex.flatMap { case (size, stratum) =>
      (0 until size).map { _ =>
        val x1 = (random.nextInt(129) - 64) / 32.0
        val x2 = random.nextInt(2).toDouble
        val time = 1.0 + random.nextInt(40) + (if (x1 > 0.0) 0 else random.nextInt(20))
        val event = if (random.nextInt(10) < 7) 1 else 0
        id += 1L
        Row(id, stratum.toLong, time, event, x1, x2)
      }
    }.toVector
  }

  private def skewed(reverse: Boolean = false): DataFrame =
    spark.createDataFrame(
      (if (reverse) skewedRows.reverse else skewedRows).asJava,
      StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField("stratum", LongType, nullable = false),
          StructField("time", DoubleType, nullable = false),
          StructField("event", IntegerType, nullable = false),
          StructField("x1", DoubleType, nullable = false),
          StructField("x2", DoubleType, nullable = false)
        )
      )
    )

  test("skewed strata: an oversized stratum gets its own block, and the fit is reproducible") {
    // 2048 / (8 (p + 4)) = 42 rows per block at p = 2: the 400-row stratum is oversized and the
    // other 560 rows fill ceil(560 / 42) = 14 shared blocks.
    val options = CoxOptions(blocks = manyBlocks)
    val many = CoxPH.fit(skewed(), skewedSpec, options)
    assert(many.converged, many.message)
    assertEquals(many.layout.targetRowsPerBlock, 42)
    assertEquals(many.layout.oversizedGroups, 1)
    assertEquals(many.layout.largestBlockRows, 400L)
    assertEquals(many.layout.blockCount, 15)
    assert(many.layout.smallestBlockRows > 0L, s"${many.layout}")
    val shuffled = withSqlConf("spark.sql.shuffle.partitions" -> "7")(
      CoxPH.fit(skewed(reverse = true).repartition(5), skewedSpec, options)
    )
    assertEquals(
      bits(shuffled.estimates ++ shuffled.covariance :+ shuffled.logLikelihood),
      bits(many.estimates ++ many.covariance :+ many.logLikelihood)
    )
    val one = CoxPH.fit(skewed(), skewedSpec)
    assertEquals(one.layout.blockCount, 1)
    assert(
      Tolerances("T-part.parameters")
        .worstRatio(many.estimates.toArray, one.estimates.toArray) <= 1.0
    )
    assert(
      Tolerances("T-part.objective")
        .worstRatio(Array(many.logLikelihood), Array(one.logLikelihood)) <= 1.0
    )
  }

  test("the stratum budget: a stratum beyond maxStratumBytes fails before any shuffle (C3, D-49)") {
    assertEquals(CoxOptions().maxStratumRows, Long.MaxValue)
    assertEquals(CoxOptions().maxStratumBytes, 4L * 1024 * 1024 * 1024)
    val name = "rc-unstratified"
    val rows = frame(spark, name).count()
    val bytes = rows * 8L * (features(name).size + 4)
    val e = intercept[IllegalArgumentException](
      CoxPH.fit(frame(spark, name), spec(name), CoxOptions(maxStratumBytes = bytes - 1L))
    )
    assert(e.getMessage.contains("maxStratumBytes"), e.getMessage)
    assert(e.getMessage.contains("TimeRange"), e.getMessage)
    assert(CoxPH.fit(frame(spark, name), spec(name), CoxOptions(maxStratumBytes = bytes)).converged)
  }

  test(
    "blocks grow until the partials fit the driver budget, and later calls find the same plan (L5)"
  ) {
    val name = "rc-stratified"
    val p = features(name).size
    val partial = 8L * (2L + p + p * (p + 1) / 2)
    val requested = CoxOptions(blocks = manyBlocks)
    val before = CoxPH.fit(frame(spark, name), spec(name), requested)
    val budget = before.layout.blockCount * partial - 1L
    val grown = CoxPH.fit(
      frame(spark, name),
      spec(name),
      requested.copy(blocks = manyBlocks.copy(driverBudgetBytes = budget))
    )
    assert(grown.converged, grown.message)
    assert(grown.layout.blockCount * partial <= budget, s"${grown.layout}")
    assert(grown.layout.targetRowsPerBlock > before.layout.targetRowsPerBlock, s"${grown.layout}")
    assert(
      Tolerances("T-part.parameters")
        .worstRatio(grown.estimates.toArray, before.estimates.toArray) <= 1.0
    )
    assert(CoxPH.baseline(frame(spark, name), grown).count() > 0L)
  }
}
