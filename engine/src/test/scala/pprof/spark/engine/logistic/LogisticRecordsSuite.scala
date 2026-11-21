package pprof.spark.engine.logistic

import java.util.SplittableRandom

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}

import pprof.spark.engine.GoldenRunsSuite.{expit, mix, normal}
import pprof.spark.engine.backend.{BlockOptions, RowRecords}
import pprof.spark.engine.layout.GroupSizes
import pprof.spark.testkit.SparkSuite

/** Chunked row storage (docs/spec/performance/step2-chunks.md §3, round 178): a block's records, read in order, give
  * every result to the bit whatever their size; and the tests and measures rebuild their fit's layout (§4).
  */
class LogisticRecordsSuite extends SparkSuite {

  private val spec =
    LogisticSpec(
      "y",
      Seq("x1", "x2", "x3"),
      "provider",
      rowId = Some("id"),
      cluster = Some("cluster")
    )
  private val options = LogisticOptions(blocks = BlockOptions(targetBlockBytes = 4096L))

  /** 1,200 Bernoulli rows: 24 providers of 50 and 37 clusters. Blocks of 4 KB hold one provider each, blocks of
    * 16 KB five, so that providers also start inside a record of 7 rows.
    */
  private lazy val frame: DataFrame = {
    val rows = (0 until 1200).map { i =>
      val rng = new SplittableRandom(mix(31L, i.toLong))
      val provider = (i % 24).toLong
      val effect = -0.3 + 0.6 * normal(new SplittableRandom(mix(131L, provider)))
      val x = Array(normal(rng), if (rng.nextDouble() < 0.4) 1.0 else 0.0, normal(rng))
      val probability = expit(effect + 0.4 * x(0) - 0.6 * x(1) + 0.3 * x(2))
      val y = if (rng.nextDouble() < probability) 1 else 0
      Row.fromSeq(Seq[Any](i.toLong, provider, y, s"c${i % 37}") ++ x.toSeq)
    }
    spark
      .createDataFrame(
        rows.asJava,
        StructType(
          Seq(
            StructField("id", LongType, nullable = false),
            StructField("provider", LongType, nullable = false),
            StructField("y", IntegerType, nullable = false),
            StructField("cluster", StringType, nullable = false)
          ) ++ Seq("x1", "x2", "x3").map(StructField(_, DoubleType, nullable = false))
        )
      )
      .repartition(5, col("id"))
  }

  private def bits(values: Iterable[Double]): Vector[Long] =
    values.toVector.map(java.lang.Double.doubleToRawLongBits)

  private def outputs(f: LogisticFit): Vector[Long] =
    bits(
      Seq(f.loglik) ++ f.auc ++ f.coefficients.map(_.estimate) ++ f.coefficients.map(
        _.standardError
      ) ++
        f.covariance ++ f.providers.gamma ++ f.providers.varGamma ++ f.providers.varCaseMix ++
        f.providers.robustVarCaseMix ++ f.providers.robustVarFixedBeta ++
        f.robustCovariance.getOrElse(Vector.empty)
    ) :+ f.iterations.toLong

  /** Every row of a table, doubles as their bits, in a fixed order. */
  private def table(df: DataFrame): Vector[String] =
    df.collect()
      .toVector
      .map(
        _.toSeq
          .map {
            case d: Double => java.lang.Long.toHexString(java.lang.Double.doubleToRawLongBits(d))
            case other     => String.valueOf(other)
          }
          .mkString("|")
      )
      .sorted

  private def inRecordsOf7[A](body: => A): A = RowRecords.rowsOverride.withValue(Some(7))(body)

  test("records of 7 rows give every result of the fit to the bit") {
    Seq(4096L, 16384L).foreach { bytes =>
      val o = options.copy(blocks = BlockOptions(targetBlockBytes = bytes))
      val fit = LogisticFE.fit(frame, spec, o)
      assert(fit.robustCovariance.nonEmpty && fit.auc.nonEmpty)
      assertEquals(
        outputs(inRecordsOf7(LogisticFE.fit(frame, spec, o))),
        outputs(fit),
        s"blocks of $bytes"
      )
    }
  }

  test("records of 7 rows give the covariate tests, provider tests and measures to the bit") {
    val fit = LogisticFE.fit(frame, spec, options)
    def results(): Vector[String] =
      (LogisticFE
        .covariateTests(frame, fit, "score") ++ LogisticFE.covariateTests(frame, fit, "lr"))
        .map(_.toString) ++
        table(LogisticProviderTests.test(frame, fit, method = "score")) ++
        table(LogisticProviderTests.test(frame, fit)) ++
        Seq("binned", "exact").flatMap { method =>
          LogisticStandardization
            .measures(frame, fit, method = method)
            .toVector
            .sortBy(_._1)
            .flatMap { case (kind, df) =>
              table(df).map(s"$method $kind: " + _)
            }
        }
    assertEquals(inRecordsOf7(results()), results())
  }

  test("a block's records hold 7 rows but the last, from the block's first row on") {
    val input = LogisticValidation.validate(frame, spec)
    val sizes = GroupSizes.collectGroupSizes(input.frame, options.blocks).sortBy(_._1)
    val (plan, groups) = LogisticFE.layout(sizes, 3, options.blocks)
    val workingSet = inRecordsOf7(LogisticWorkingSet.build(input, plan, groups, options.blocks))
    try {
      val byBlock = workingSet.records.collect().groupBy(_.blockId)
      assertEquals(byBlock.size, plan.blockCount)
      byBlock.values.foreach { records =>
        val sorted = records.sortBy(_.record).toVector
        assertEquals(sorted.map(_.record), sorted.indices.toVector)
        assert(sorted.forall(_.records == sorted.length))
        assert(sorted.init.forall(_.y.length == 7) && sorted.last.y.length <= 7)
        assertEquals(sorted.map(_.firstRow), sorted.indices.toVector.map(_ * 7))
        assertEquals(sorted.map(_.x.length), sorted.map(_.y.length * 3))
      }
    } finally workingSet.release()
    assertEquals(RowRecords.default(180), 182)
    assertEquals(RowRecords.default(25), 1310)
  }

  test("blocks are read only from their records, complete and in order") {
    def record(block: Int, i: Int, of: Int) =
      LogisticChunk(
        block,
        i,
        of,
        i,
        1,
        if (i == 0) Array(block) else Array.emptyIntArray,
        if (i == 0) Array(0) else Array.emptyIntArray,
        Array(i.toLong),
        Array(0.0),
        Array(1.0),
        Array(0.5),
        Array.emptyIntArray
      )
    val blocks =
      LogisticBlocks.assemble(Iterator(record(0, 0, 2), record(0, 1, 2), record(1, 0, 1))).toVector
    assertEquals(blocks.map(_.rowCount), Vector(2, 1))
    assertEquals(blocks.map(b => b.rowId(b.rowCount - 1)), Vector(1L, 0L))
    intercept[IllegalStateException](LogisticBlocks.assemble(Iterator(record(0, 1, 2))).next())
    intercept[IllegalStateException](
      LogisticBlocks.assemble(Iterator(record(0, 0, 2), record(1, 0, 1))).next()
    )
    intercept[IllegalStateException](LogisticBlocks.assemble(Iterator(record(0, 0, 2))).next())
  }

  test("beyond the driver budget the tests and measures rebuild their fit's layout") {
    val partial = 8L * (6 + 3 + 2)
    val tight = options.copy(blocks = options.blocks.copy(driverBudgetBytes = 8 * partial))
    val fit = LogisticFE.fit(frame, spec, tight)
    assert(fit.layout.blocksPerGroup > 1, fit.layout.toString)
    val (workingSet, _) = LogisticFE.trainingWorkingSet(frame, fit)
    try {
      assertEquals(workingSet.plan.blockCount, fit.layout.blockCount)
      assertEquals(workingSet.groups.blocksPerGroup, fit.layout.blocksPerGroup)
    } finally workingSet.release()
    val grouped = LogisticFE.covariateTests(frame, fit, "score")
    val single = LogisticFE.covariateTests(frame, LogisticFE.fit(frame, spec, options), "score")
    grouped.zip(single).foreach { case (a, b) =>
      val (x, y) = (
        a.productIterator.collect { case d: Double => d }.toVector,
        b.productIterator.collect { case d: Double =>
          d
        }.toVector
      )
      x.zip(y).foreach { case (u, v) =>
        assert(math.abs(u - v) <= 1e-8 * math.max(1.0, math.abs(v)), s"$a against $b")
      }
    }
  }
}
