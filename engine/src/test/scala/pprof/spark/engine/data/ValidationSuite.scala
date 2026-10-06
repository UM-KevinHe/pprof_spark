package pprof.spark.engine.data

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}

import pprof.spark.engine.data.InputProblem._
import pprof.spark.testkit.SparkSuite

class ValidationSuite extends SparkSuite {

  private val schema = StructType(
    Seq(
      StructField("provider", StringType),
      StructField("age", DoubleType),
      StructField("visits", IntegerType),
      StructField("id", LongType),
      StructField("label", StringType)
    )
  )

  private def frame(rows: Row*): DataFrame = spark.createDataFrame(rows.asJava, schema)

  test("schema problems are reported together, before any Spark job") {
    val error = intercept[InvalidInputException] {
      Validation.validate(frame(), InputSpec("provider", Seq("age", "label", "nope")))
    }
    assertEquals(
      error.problems.toSet,
      Set[InputProblem](UnsupportedType("label", "string", "a numeric type"), MissingColumn("nope"))
    )
  }

  test("invalid values are counted per column and never revealed") {
    val df = frame(
      Row("a", 1.5, 1, 1L, "x"),
      Row(null, Double.NaN, 2, 2L, "x"),
      Row("b", Double.PositiveInfinity, null, 3L, "x"),
      Row("b", null, 4, 3L, "x")
    )
    val error = intercept[InvalidInputException] {
      Validation.validate(df, InputSpec("provider", Seq("age", "visits"), Some("id")))
    }
    assertEquals(
      error.problems.toSet,
      Set[InputProblem](
        InvalidValues("provider", 1L, 0L),
        InvalidValues("age", 1L, 2L),
        InvalidValues("visits", 1L, 0L),
        DuplicateRowIds("id", 1L)
      )
    )
    assert(
      !error.getMessage.contains("1.5") && !error.getMessage.contains("Infinity"),
      error.getMessage
    )
  }

  test("empty input is rejected") {
    val error = intercept[InvalidInputException](
      Validation.validate(frame(), InputSpec("provider", Seq("age")))
    )
    assertEquals(error.problems, Seq[InputProblem](EmptyInput))
  }

  test("accepted columns are converted to the internal form") {
    val input = Validation.validate(
      frame(Row("a", 1.5, 2, 7L, "x")),
      InputSpec("provider", Seq("age", "visits"), Some("id"))
    )
    assertEquals(input.rowCount, 1L)
    assert(input.groupKeyIsText)
    val row = input.frame.collect().head
    assertEquals(row.getString(0), "a")
    assertEquals(row.getSeq[Double](1).toSeq, Seq(1.5, 2.0))
    assertEquals(row.getLong(2), 7L)
  }
}
