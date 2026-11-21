package pprof.spark.engine.cox

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

import pprof.spark.engine.GoldenRunsSuite.{mix, normal}
import pprof.spark.engine.backend.{BlockOptions, RowRecords}
import pprof.spark.testkit.SparkSuite

/** Chunked row storage for Cox (docs/spec/performance/step2-chunks.md §3, round 180): a block's records, read in
  * order, give every result to the bit whatever their size, with ties, left truncation, weights, offsets and
  * clusters, under both tie methods.
  */
class CoxRecordsSuite extends SparkSuite {

  private val spec = CoxSpec(
    "time",
    "event",
    Seq("x1", "x2", "x3"),
    strata = Some("provider"),
    rowId = Some("id"),
    weight = Some("w"),
    offset = Some("o"),
    entry = Some("start"),
    cluster = Some("cluster")
  )

  /** 1,600 rows: 20 strata of 80 with tied times, late entries, some zero weights, and 31 clusters. Blocks of 4 KB
    * hold one stratum each and blocks of 16 KB three, so that strata also start inside a record of 7 rows.
    */
  private lazy val frame: DataFrame = {
    val rows = (0 until 1600).map { i =>
      val rng = new SplittableRandom(mix(41L, i.toLong))
      val x = Array(normal(rng), if (rng.nextDouble() < 0.4) 1.0 else 0.0, normal(rng))
      val time = 1.0 + rng.nextInt(12)
      val start =
        if (rng.nextDouble() < 0.3) time - 1.0 - rng.nextInt(math.max(1, time.toInt - 1)) else 0.0
      val w = if (rng.nextDouble() < 0.05) 0.0 else 0.5 + rng.nextDouble()
      Row.fromSeq(
        Seq[Any](
          i.toLong,
          (i % 20).toLong,
          time,
          if (rng.nextDouble() < 0.5) 1 else 0,
          math.max(0.0, start),
          w,
          0.2 * (rng.nextDouble() - 0.5),
          s"c${i % 31}"
        ) ++ x.toSeq
      )
    }
    spark
      .createDataFrame(
        rows.asJava,
        StructType(
          Seq(
            StructField("id", LongType, nullable = false),
            StructField("provider", LongType, nullable = false),
            StructField("time", DoubleType, nullable = false),
            StructField("event", IntegerType, nullable = false),
            StructField("start", DoubleType, nullable = false),
            StructField("w", DoubleType, nullable = false),
            StructField("o", DoubleType, nullable = false),
            StructField("cluster", StringType, nullable = false)
          ) ++ Seq("x1", "x2", "x3").map(StructField(_, DoubleType, nullable = false))
        )
      )
      .repartition(5, col("id"))
  }

  /** Every number in a result, doubles as their bits, in field order. */
  private def numbers(a: Any): Vector[Long] = a match {
    case d: Double           => Vector(java.lang.Double.doubleToRawLongBits(d))
    case i: Int              => Vector(i.toLong)
    case l: Long             => Vector(l)
    case b: Boolean          => Vector(if (b) 1L else 0L)
    case values: Array[_]    => values.toVector.flatMap(numbers)
    case values: Iterable[_] => values.toVector.flatMap(numbers)
    case p: Product          => p.productIterator.toVector.flatMap(numbers)
    case _                   => Vector.empty
  }

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

  private def options(ties: Ties, bytes: Long) =
    CoxOptions(ties = ties, blocks = BlockOptions(targetBlockBytes = bytes))

  test("records of 7 rows give every result of the fit to the bit, both tie methods") {
    for (ties <- Seq(Ties.Breslow, Ties.Efron); bytes <- Seq(4096L, 16384L)) {
      val o = options(ties, bytes)
      val fit = CoxPH.fit(frame, spec, o)
      assert(fit.converged)
      assertEquals(
        numbers(inRecordsOf7(CoxPH.fit(frame, spec, o))),
        numbers(fit),
        s"$ties, blocks of $bytes"
      )
    }
  }

  test(
    "records of 7 rows give the baseline, residuals, reference point, measures and tests to the bit"
  ) {
    val fit = CoxPH.fit(frame, spec, options(Ties.Efron, 16384L))
    def results(): Vector[String] =
      table(CoxPH.baseline(frame, fit)) ++
        table(CoxPH.baseline(frame, fit, BaselineReference.Means)).map("means: " + _) ++
        table(CoxPH.residuals(frame, fit)) ++
        numbers(CoxPH.referencePoint(frame, fit)).map(_.toString) ++
        CoxMeasures
          .standardized(
            frame,
            fit,
            "provider",
            Seq(Standardization.Indirect, Standardization.Direct)
          )
          .toVector
          .sortBy(_._1.name)
          .flatMap { case (kind, df) => table(df).map(kind.name + ": " + _) } ++
        table(CoxProviderTests.test(frame, fit, "provider"))
    assertEquals(inRecordsOf7(results()), results())
  }

  test("a block's records hold 7 rows but the last, with each stratum's entry order") {
    val o = options(Ties.Efron, 16384L)
    CoxPH.withWorkingSet(frame, spec, o) { prepared =>
      val byBlock =
        inRecordsOf7(CoxPH.withWorkingSet(frame, spec, o)(_.workingSet.records.collect()))
          .groupBy(_.blockId)
      assertEquals(byBlock.size, prepared.plan.blockCount)
      byBlock.values.foreach { records =>
        val sorted = records.sortBy(_.record).toVector
        assertEquals(sorted.map(_.record), sorted.indices.toVector)
        assert(sorted.forall(_.records == sorted.length))
        assert(sorted.init.forall(_.time.length == 7) && sorted.last.time.length <= 7)
        assertEquals(sorted.map(_.firstRow), sorted.indices.toVector.map(_ * 7))
        val starts = sorted.flatMap(_.groupStart)
        val order = sorted.flatMap(_.entryOrder)
        val ends = starts.drop(1) :+ order.length
        starts.zip(ends).foreach { case (from, until) =>
          assertEquals(order.slice(from, until).sorted, (from until until).toVector)
        }
      }
    }
  }

  test("Cox blocks are read only from their records, complete and in order") {
    def record(block: Int, i: Int, of: Int) =
      CoxChunk(
        block,
        i,
        of,
        i,
        1,
        if (i == 0) Array(block) else Array.emptyIntArray,
        if (i == 0) Array(0) else Array.emptyIntArray,
        Array(i.toLong),
        Array(1.0),
        Array(true),
        Array(1.0),
        Array(0.0),
        Array(0.5),
        Array(0.0),
        Array(i),
        Array("c")
      )
    val blocks =
      CoxBlocks.assemble(Iterator(record(0, 0, 2), record(0, 1, 2), record(1, 0, 1))).toVector
    assertEquals(blocks.map(_.rowCount), Vector(2, 1))
    assertEquals(blocks.map(b => b.rowId(b.rowCount - 1)), Vector(1L, 0L))
    intercept[IllegalStateException](CoxBlocks.assemble(Iterator(record(0, 1, 2))).next())
    intercept[IllegalStateException](
      CoxBlocks.assemble(Iterator(record(0, 0, 2), record(1, 0, 1))).next()
    )
    intercept[IllegalStateException](CoxBlocks.assemble(Iterator(record(0, 0, 2))).next())
  }
}
