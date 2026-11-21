package pprof.spark.engine

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{
  DataType,
  DoubleType,
  IntegerType,
  LongType,
  StructField,
  StructType
}

import pprof.spark.engine.cox.{CoxPH, CoxSpec}
import pprof.spark.engine.data.{InputProblem, InvalidInputException}
import pprof.spark.engine.linear.{LinearFE, LinearRE, LinearRESpec, LinearSpec}
import pprof.spark.engine.logistic.{LogisticFE, LogisticRE, LogisticRESpec, LogisticSpec}
import pprof.spark.testkit.SparkSuite

/** Duplicate and missing row identifiers in each model's validation, whose distinct counts run as jobs of their own
  * (OI-109 for Cox and logistic fixed effects, OI-114 for the others): rows 1 to 3 share identifier 0 with row 0, so
  * three are duplicates, and rows 4 and 5 have none.
  */
class DuplicateRowsSuite extends SparkSuite {

  private def frame(rows: Seq[Row], columns: Seq[(String, DataType, Boolean)]): DataFrame =
    spark.createDataFrame(
      rows.asJava,
      StructType(columns.map { case (n, t, nullable) => StructField(n, t, nullable) })
    )

  private def id(i: Int): Any = if (i == 4 || i == 5) null else if (i <= 3) 0L else i.toLong

  private def problems(body: => Any): Seq[InputProblem] =
    intercept[InvalidInputException](body).problems

  test("Cox: duplicate and missing row identifiers are counted exactly") {
    val rows = (0 until 40).map(i => Row(id(i), 1.0 + (i % 7), i % 2, (i % 5) / 5.0))
    val df = frame(
      rows,
      Seq(
        ("id", LongType, true),
        ("time", DoubleType, false),
        ("event", IntegerType, false),
        ("x1", DoubleType, false)
      )
    )
    val found = problems(CoxPH.fit(df, CoxSpec("time", "event", Seq("x1"), rowId = Some("id"))))
    assert(found.contains(InputProblem.InvalidValues("id", 2L, 0L)), found.mkString("; "))
    assert(found.contains(InputProblem.DuplicateRowIds("id", 3L)), found.mkString("; "))
  }

  test("Cox: duplicate composite row keys are counted exactly") {
    val rows = (0 until 30).map(i =>
      Row(if (i <= 2) 0L else i.toLong, 0L, 1.0 + (i % 7), i % 2, (i % 5) / 5.0)
    )
    val df = frame(
      rows,
      Seq(
        ("a", LongType, false),
        ("b", LongType, false),
        ("time", DoubleType, false),
        ("event", IntegerType, false),
        ("x1", DoubleType, false)
      )
    )
    val found = problems(CoxPH.fit(df, CoxSpec("time", "event", Seq("x1"), rowKey = Seq("a", "b"))))
    assert(
      found.contains(InputProblem.DuplicateRowKeys(Seq("a", "b"), 2L, collision = false)),
      found.mkString("; ")
    )
  }

  test("logistic: duplicate and missing row identifiers are counted exactly") {
    val rows = (0 until 40).map(i => Row(id(i), (i % 4).toLong, i % 2, (i % 5) / 5.0))
    val df = frame(
      rows,
      Seq(
        ("id", LongType, true),
        ("provider", LongType, false),
        ("y", IntegerType, false),
        ("x1", DoubleType, false)
      )
    )
    val found =
      problems(LogisticFE.fit(df, LogisticSpec("y", Seq("x1"), "provider", rowId = Some("id"))))
    assert(found.contains(InputProblem.InvalidValues("id", 2L, 0L)), found.mkString("; "))
    assert(found.contains(InputProblem.DuplicateRowIds("id", 3L)), found.mkString("; "))
  }

  private def outcomes(continuous: Boolean): DataFrame =
    frame(
      (0 until 40).map(i =>
        Row(id(i), (i % 4).toLong, if (continuous) (i % 7).toDouble else i % 2, (i % 5) / 5.0)
      ),
      Seq(
        ("id", LongType, true),
        ("provider", LongType, false),
        ("y", if (continuous) DoubleType else IntegerType, false),
        ("x1", DoubleType, false)
      )
    )

  private def checkCounted(found: Seq[InputProblem]): Unit = {
    assert(found.contains(InputProblem.InvalidValues("id", 2L, 0L)), found.mkString("; "))
    assert(found.contains(InputProblem.DuplicateRowIds("id", 3L)), found.mkString("; "))
  }

  test("linear fixed effects: duplicate and missing row identifiers are counted exactly (OI-114)") {
    checkCounted(
      problems(
        LinearFE.fit(
          outcomes(continuous = true),
          LinearSpec("y", Seq("x1"), "provider", Some("id"))
        )
      )
    )
  }

  test(
    "linear random effects: duplicate and missing row identifiers are counted exactly (OI-114)"
  ) {
    val spec = LinearRESpec("y", Seq("x1"), "provider", rowId = Some("id"))
    checkCounted(problems(LinearRE.fit(outcomes(continuous = true), spec)))
  }

  test(
    "logistic random effects: duplicate and missing row identifiers are counted exactly (OI-114)"
  ) {
    val spec = LogisticRESpec("y", Seq("x1"), "provider", rowId = Some("id"))
    checkCounted(problems(LogisticRE.fit(outcomes(continuous = false), spec)))
  }
}
