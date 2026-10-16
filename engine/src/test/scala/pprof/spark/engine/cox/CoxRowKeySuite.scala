package pprof.spark.engine.cox

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, xxhash64}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import pprof.spark.engine.cox.CoxFixtures.bits
import pprof.spark.engine.data.InvalidInputException
import pprof.spark.testkit.SparkSuite

/** Composite row keys (Cox specification §4a; production finding C2, D-49): counting-process data
  * keyed by (patient, start), with no synthetic identifier and nothing materialized.
  */
class CoxRowKeySuite extends SparkSuite {

  /** 300 patients with 1 to 4 intervals each; a patient's next interval starts where the last
    * ended, so (patient, start) identifies a row and patient alone does not.
    */
  private lazy val data: Vector[Row] = {
    val random = new scala.util.Random(87L)
    (0 until 300).flatMap { patient =>
      val x1 = (random.nextInt(129) - 64) / 64.0
      val x2 = random.nextInt(2).toDouble
      var start = if (random.nextInt(10) < 3) random.nextInt(30).toDouble else 0.0
      (0 until 1 + random.nextInt(4)).map { _ =>
        val stop = start + 1.0 + random.nextInt(100)
        val row = Row(patient.toLong, start, stop, if (random.nextInt(10) < 4) 1 else 0, x1, x2)
        start = stop
        row
      }
    }.toVector
  }

  private val schema = StructType(
    Seq(
      StructField("patient", LongType, nullable = false),
      StructField("start", DoubleType, nullable = false),
      StructField("stop", DoubleType, nullable = false),
      StructField("event", IntegerType, nullable = false),
      StructField("x1", DoubleType, nullable = false),
      StructField("x2", DoubleType, nullable = false)
    )
  )

  private def frame(reverse: Boolean = false): DataFrame =
    spark.createDataFrame((if (reverse) data.reverse else data).asJava, schema)

  private def hashed: DataFrame =
    frame().withColumn("row_id", xxhash64(col("patient"), col("start")))

  private val keyed = CoxSpec(
    "stop",
    "event",
    Seq("x1", "x2"),
    entry = Some("start"),
    rowKey = Seq("patient", "start")
  )

  private val byHash = keyed.copy(rowKey = Nil, rowId = Some("row_id"))

  test("a row key's hash is the row identifier: the same bits as an explicit hashed column") {
    val byKey = CoxPH.fit(frame(), keyed)
    val byId = CoxPH.fit(hashed, byHash)
    assertEquals(
      bits(byKey.estimates :+ byKey.logLikelihood),
      bits(byId.estimates :+ byId.logLikelihood)
    )
    assertEquals(byKey.fingerprint, byId.fingerprint)
  }

  test("later calls accept the data recomputed in another order, with nothing materialized") {
    val fit = CoxPH.fit(frame(), keyed)
    def cumulative(df: DataFrame) =
      bits(
        CoxPH
          .baseline(df, fit)
          .orderBy("stratum", "time")
          .collect()
          .toSeq
          .map(_.getAs[Double]("cumulative_hazard"))
      )
    assertEquals(cumulative(frame(reverse = true).repartition(5)), cumulative(frame()))
  }

  test("residuals carry the key columns and equal those keyed by the hash itself") {
    val fit = CoxPH.fit(frame(), keyed)
    val residuals = CoxPH.residuals(frame(), fit)
    assertEquals(residuals.columns.take(3).toSeq, Seq("patient", "start", "martingale"))
    val rows = residuals.withColumn("h", xxhash64(col("patient"), col("start"))).collect()
    assertEquals(rows.length, data.length)
    assertEquals(
      rows.map(r => (r.getAs[Long]("patient"), r.getAs[Double]("start"))).distinct.length,
      data.length
    )
    val expected = CoxPH
      .residuals(hashed, CoxPH.fit(hashed, byHash))
      .collect()
      .map(r => r.getAs[Long]("row_id") -> r.getAs[Double]("martingale"))
      .toMap
    rows.foreach { r =>
      assertEquals(
        bits(Seq(r.getAs[Double]("martingale"))),
        bits(Seq(expected(r.getAs[Long]("h"))))
      )
    }
  }

  test("a key that repeats is refused, naming its columns; rowId and rowKey together are refused") {
    val e =
      intercept[InvalidInputException](CoxPH.fit(frame(), keyed.copy(rowKey = Seq("patient"))))
    assert(e.getMessage.contains("(patient)") && e.getMessage.contains("repeat"), e.getMessage)
    intercept[IllegalArgumentException](keyed.copy(rowId = Some("patient")))
    intercept[IllegalArgumentException](keyed.copy(rowKey = Seq("patient", "patient")))
  }

  test("saved models keep the row key, and their residuals still join back") {
    val fit = CoxPH.fit(frame(), keyed)
    val path = Files.createTempDirectory("pprof-cox-row-key").resolve("model").toString
    CoxFitIO.save(spark, fit, path)
    val loaded = CoxFitIO.load(spark, path)
    assertEquals(loaded.spec.rowKey, Seq("patient", "start"))
    assertEquals(CoxPH.residuals(frame(), loaded).count(), data.length.toLong)
  }
}
