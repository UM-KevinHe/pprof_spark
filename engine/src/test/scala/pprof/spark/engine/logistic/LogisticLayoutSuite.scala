package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import pprof.spark.engine.backend.{BlockOptions, DriverLimitExceededException}
import pprof.spark.engine.logistic.LogisticFixtures._
import pprof.spark.numerics.{Cholesky, InMemorySerbinPasses, Serbin, SerbinOptions}
import pprof.spark.numerics.kernels.{LogisticFE => Kernel}
import pprof.spark.testkit.{SparkSuite, Tolerances}

/** Distributed correctness of the logistic fixed-effect fit beyond the default one-block layout
  * (OI-71, round 79): reference parity with many blocks, bitwise reproducibility across
  * `spark.sql.shuffle.partitions` and input order, skewed provider sizes (§9.6), the line-search
  * ladder against a sequential search, twenty features, and twenty thousand providers.
  */
class LogisticLayoutSuite extends SparkSuite {

  private def bits(values: Seq[Double]): Seq[Long] = values.map(java.lang.Double.doubleToLongBits)

  /** 73 rows per block at p = 3. */
  private val manyBlocks = BlockOptions(targetBlockBytes = 4096L)

  for (name <- Cases)
    test(s"many blocks: the distributed fit matches pprof_py's default fit: $name") {
      val fit = LogisticFE.fit(frame(spark, name), spec(name), LogisticOptions(blocks = manyBlocks))
      assert(fit.layout.blockCount > 1, s"expected several blocks: ${fit.layout}")
      val ref = json(name, "pprof_py").get("default")
      assert(fit.converged)
      assertEquals(fit.iterations, ref.get("iterations").asInt)
      check("T-coef", s"$name beta", fit.estimates, doubles(ref, "beta"))
      check("T-coef", s"$name gamma", fit.providers.gamma.toSeq, doubles(ref, "gamma"))
      check("T-var", s"$name var_beta", fit.covariance, doubles(ref, "var_beta"))
      check("T-var", s"$name var_gamma", fit.providers.varGamma.toSeq, doubles(ref, "var_gamma"))
      check(
        "T-var",
        s"$name var_case_mix",
        fit.providers.varCaseMix.toSeq,
        doubles(ref, "var_case_mix")
      )
      check("T-fn", s"$name loglik", Seq(fit.loglik), doubles(ref, "loglik"))
    }

  /** Every bit of a fit, with its robust variances and AUC when present, and of its exact tests. */
  private def everything(df: DataFrame, name: String): Seq[Long] = {
    val fit = LogisticFE.fit(df, spec(name), LogisticOptions(blocks = manyBlocks))
    val tests = LogisticProviderTests.test(df, fit).collect().toSeq.flatMap { r =>
      Seq(
        r.getAs[Double]("z_raw"),
        r.getAs[Double]("p_value"),
        r.getAs[Double]("ci_lower"),
        r.getAs[Double]("ci_upper")
      )
    }
    bits(
      fit.estimates ++ fit.covariance ++ fit.robustCovariance.getOrElse(Vector.empty[Double]) ++
        fit.providers.gamma ++ fit.providers.varCaseMix ++ fit.providers.robustVarCaseMix ++
        tests ++ Seq(fit.loglik, fit.auc.getOrElse(Double.NaN))
    )
  }

  for (name <- Seq("lfe-clustered", "lfe-binomial"))
    test(
      s"R0: shuffle partitions 1, 4 and 7 and reversed, repartitioned input leave every bit: $name"
    ) {
      val expected =
        withSqlConf("spark.sql.shuffle.partitions" -> "4")(everything(frame(spark, name), name))
      val one =
        withSqlConf("spark.sql.shuffle.partitions" -> "1")(everything(frame(spark, name), name))
      val seven = withSqlConf("spark.sql.shuffle.partitions" -> "7")(
        everything(frame(spark, name, reverse = true).repartition(3), name)
      )
      assertEquals(one, expected, "shuffle partitions 1 against 4")
      assertEquals(seven, expected, "shuffle partitions 7, reversed and repartitioned, against 4")
    }

  private val twoFeatures = LogisticSpec("y", Seq("x1", "x2"), "provider", rowId = Some("id"))

  private val twoFeatureSchema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("provider", LongType, nullable = false),
      StructField("y", IntegerType, nullable = false),
      StructField("x1", DoubleType, nullable = false),
      StructField("x2", DoubleType, nullable = false)
    )
  )

  /** A deterministic skewed data set: one provider of 600 records, 10 of 60 and 60 of 12. */
  private lazy val skewedRows: Vector[Row] = {
    val sizes = Seq(600) ++ Seq.fill(10)(60) ++ Seq.fill(60)(12)
    val random = new scala.util.Random(79L)
    var id = 0L
    sizes.zipWithIndex.flatMap { case (size, provider) =>
      val effect = (provider % 7 - 3) * 0.25
      (0 until size).map { _ =>
        val x1 = (random.nextInt(129) - 64) / 32.0
        val x2 = random.nextInt(2).toDouble
        val pi = 1.0 / (1.0 + math.exp(-(effect + 0.8 * x1 - 0.5 * x2)))
        val y = if (random.nextDouble() < pi) 1 else 0
        id += 1L
        Row(id, provider.toLong, y, x1, x2)
      }
    }.toVector
  }

  private def skewed(reverse: Boolean = false): DataFrame =
    spark.createDataFrame(
      (if (reverse) skewedRows.reverse else skewedRows).asJava,
      twoFeatureSchema
    )

  test("skewed providers: an oversized provider gets its own block, and the fit is reproducible") {
    // 4096 / (8 (p + 4)) = 85 rows per block at p = 2: the 600-record provider is oversized and the
    // other 1,320 records fill ceil(1320 / 85) = 16 shared blocks.
    val options = LogisticOptions(blocks = manyBlocks)
    val many = LogisticFE.fit(skewed(), twoFeatures, options)
    assert(many.converged)
    assertEquals(many.providers.size, 71)
    assertEquals(many.layout.targetRowsPerBlock, 85)
    assertEquals(many.layout.oversizedGroups, 1)
    assertEquals(many.layout.largestBlockRows, 600L)
    assertEquals(many.layout.blockCount, 17)
    val shuffled = withSqlConf("spark.sql.shuffle.partitions" -> "7")(
      LogisticFE.fit(skewed(reverse = true).repartition(5), twoFeatures, options)
    )
    assertEquals(
      bits(shuffled.estimates ++ shuffled.providers.gamma),
      bits(many.estimates ++ many.providers.gamma)
    )
    val one = LogisticFE.fit(skewed(), twoFeatures)
    assertEquals(one.layout.blockCount, 1)
    assert(
      Tolerances("T-part.parameters")
        .worstRatio(many.estimates.toArray, one.estimates.toArray) <= 1.0
    )
    assert(
      Tolerances("T-part.parameters").worstRatio(many.providers.gamma, one.providers.gamma) <= 1.0
    )
  }

  /** SerBIN with a plain sequential Armijo search, one log-likelihood per trial step length: the
    * reference for the eight-step ladder (Phase 2a specification §11).
    */
  private def sequential(
      rows: Kernel.Rows,
      gamma0: Array[Double],
      beta0: Array[Double],
      options: SerbinOptions
  ): (Vector[Double], Array[Double], Array[Double]) = {
    val p = beta0.length
    var gamma = gamma0.clone()
    var beta = beta0.clone()
    var criterion = Double.PositiveInfinity
    var iteration = 0
    val steps = Vector.newBuilder[Double]
    while (iteration <= options.maxIter && criterion >= options.tol) {
      iteration += 1
      val schur = Kernel.schur(rows, gamma, beta)
      val deltaBeta = Cholesky.factor(schur.packed, p).solve(schur.rhs)
      var lambda = schur.scoreTerm
      (0 until p).foreach(j => lambda += schur.rhs(j) * deltaBeta(j))
      val deltaGamma = Kernel.trial(rows, gamma, beta, deltaBeta, Array.emptyDoubleArray).deltaGamma
      def change(v: Double): Double =
        Kernel.loglik(
          rows,
          Array.tabulate(gamma.length)(k => gamma(k) + v * deltaGamma(k)),
          Array.tabulate(p)(j => beta(j) + v * deltaBeta(j))
        ) - schur.loglik
      var v = 1.0
      while ({
        val predicted = Serbin.Armijo * v * lambda
        change(v) < predicted && !Serbin.belowNoise(predicted, schur.loglik)
      }) v *= Serbin.Shrink
      steps += v
      gamma = Serbin.bounded(
        Array.tabulate(gamma.length)(k => gamma(k) + v * deltaGamma(k)),
        options.bound
      )
      val updated = Array.tabulate(p)(j => beta(j) + v * deltaBeta(j))
      criterion = (0 until p).map(j => math.abs(beta(j) - updated(j))).max
      beta = updated
    }
    (steps.result(), gamma, beta)
  }

  test("the eight-step ladder accepts exactly the step lengths of a sequential line search") {
    // From β = (10, −10, 10) and γ = 0 the first full Newton step is rejected eight times, so the
    // search needs a second ladder pass; the fit still converges to the default fit's estimates.
    val rows = block("lfe-base")
    val gamma0 = Array.fill(rows.providers)(0.0)
    val beta0 = Array(10.0, -10.0, 10.0)
    val options = SerbinOptions()
    val ladder = Serbin.fit(gamma0, beta0, new InMemorySerbinPasses(Seq(rows)), options)
    assert(
      ladder.trace.exists(_.evaluated > Serbin.Ladder),
      s"step lengths evaluated: ${ladder.trace.map(_.evaluated).mkString(", ")}"
    )
    val (steps, gamma, beta) = sequential(rows, gamma0, beta0, options)
    assertEquals(bits(ladder.trace.map(_.step)), bits(steps))
    assertEquals(bits(ladder.beta.toSeq), bits(beta.toSeq))
    assertEquals(bits(ladder.gamma.toSeq), bits(gamma.toSeq))
    val default = json("lfe-base", "pprof_py").get("default")
    check("T-coef", "beta from a distant start", ladder.beta.toSeq, doubles(default, "beta"))
  }

  test("twenty features: the distributed fit equals the in-memory SerBIN at a stationary point") {
    val p = 20
    val names = (0 until p).map(j => s"f$j")
    val random = new scala.util.Random(20L)
    val data = (0 until 40 * 30).map { i =>
      val provider = (i / 30).toLong
      val x = Array.fill(p)((random.nextInt(129) - 64) / 64.0)
      val eta =
        (provider % 5 - 2) * 0.3 + x.indices.map(j => x(j) * (if (j % 2 == 0) 0.25 else -0.2)).sum
      val y = if (random.nextDouble() < 1.0 / (1.0 + math.exp(-eta))) 1 else 0
      (i.toLong, provider, y, x)
    }
    val schema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("provider", LongType, nullable = false),
        StructField("y", IntegerType, nullable = false)
      ) ++ names.map(StructField(_, DoubleType, nullable = false))
    )
    val df = spark.createDataFrame(
      data.map { case (id, provider, y, x) => Row.fromSeq(Seq[Any](id, provider, y) ++ x) }.asJava,
      schema
    )
    val wide = LogisticSpec("y", names, "provider", rowId = Some("id"))
    val one = LogisticFE.fit(df, wide)
    assertEquals(one.layout.blockCount, 1)
    val rows = Kernel.Rows(
      (0 until 40).map(_ * 30).toArray,
      data.map(_._3.toDouble).toArray,
      Array.fill(data.size)(1.0),
      data.flatMap(_._4).toArray,
      p
    )
    val yBar = data.map(_._3).sum.toDouble / data.size
    val memory = Serbin.fit(
      Array.fill(40)(StrictMath.log(yBar / (1.0 - yBar))),
      new Array[Double](p),
      new InMemorySerbinPasses(Seq(rows)),
      SerbinOptions()
    )
    assertEquals(bits(one.estimates), bits(memory.beta.toSeq))
    assertEquals(bits(one.providers.gamma.toSeq), bits(memory.gamma.toSeq))
    val schur = Kernel.schur(rows, memory.gamma, memory.beta)
    val newton = Cholesky.factor(schur.packed, p).solve(schur.rhs).map(math.abs).max
    assert(newton < 1e-7, s"the Newton step at the estimates is $newton")
    val many =
      LogisticFE.fit(df, wide, LogisticOptions(blocks = BlockOptions(targetBlockBytes = 8192L)))
    assert(many.layout.blockCount > 1, s"${many.layout}")
    assert(
      Tolerances("T-part.parameters")
        .worstRatio(many.estimates.toArray, one.estimates.toArray) <= 1.0
    )
    assert(
      Tolerances("T-part.parameters").worstRatio(many.providers.gamma, one.providers.gamma) <= 1.0
    )
  }

  test("twenty thousand providers: the effects stay on the driver, under maxProvidersOnDriver") {
    val m = 20000
    val df = spark
      .range(m.toLong * 12L)
      .toDF("id")
      .withColumn("provider", (col("id") / 12).cast("long"))
      .withColumn("x1", (col("id") * 37 % 64 - 32) / 32.0)
      .withColumn("x2", (col("id") % 2).cast("double"))
      .withColumn(
        "y",
        expr(
          "CASE WHEN pmod(hash(id, 79), 10000) < " +
            "10000 / (1 + exp(-((provider % 9 - 4) * 0.25 + 0.5 * x1 - 0.4 * x2))) THEN 1 ELSE 0 END"
        )
      )
    val fit = LogisticFE.fit(df, twoFeatures)
    assertEquals(fit.providers.size, m)
    assert(fit.converged, fit.warnings.mkString("; "))
    assertEquals(LogisticFE.providerTable(spark, fit).count(), m.toLong)
    val e = intercept[DriverLimitExceededException](
      LogisticFE.fit(df, twoFeatures, LogisticOptions(maxProvidersOnDriver = m - 1))
    )
    assert(e.getMessage.contains("maxProvidersOnDriver"), e.getMessage)
  }
}
