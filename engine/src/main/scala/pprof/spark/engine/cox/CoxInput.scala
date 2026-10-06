package pprof.spark.engine.cox

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{array, coalesce, count, countDistinct, lit, sum, when}
import org.apache.spark.sql.types.{
  BooleanType,
  DataType,
  DoubleType,
  LongType,
  NumericType,
  StringType
}

import pprof.spark.engine.data.{InputProblem, InvalidInputException, Validation}

/** Column roles of a Cox fit (Cox specification §4): exit time, event indicator and features, and
  * optionally strata and a row identifier. Every subject enters at time 0.
  */
final case class CoxSpec(
    time: String,
    event: String,
    features: Seq[String],
    strata: Option[String] = None,
    rowId: Option[String] = None
) {
  require(features.nonEmpty, "at least one feature column is required")
  require(
    columns.distinct.size == columns.size,
    s"each column may have one role only: ${columns.mkString(", ")}"
  )

  def columns: Seq[String] = Seq(time, event) ++ features ++ strata.toSeq ++ rowId.toSeq
}

/** A validated Cox input: the internal columns only, with its row and event counts. */
final case class CoxInput(
    frame: DataFrame,
    spec: CoxSpec,
    rowCount: Long,
    eventCount: Long,
    strataKeyIsText: Boolean
)

/** Cox input validation (Cox specification §4): one aggregation job, every problem reported at
  * once with counts and never values (NN-7), and no row dropped.
  */
object CoxValidation {

  val TimeColumn: String = "__pprof_time"
  val EventColumn: String = "__pprof_event"

  def validate(df: DataFrame, spec: CoxSpec): CoxInput = {
    val types = df.schema.fields.map(f => f.name -> f.dataType).toMap
    val structural = schemaProblems(types, spec)
    if (structural.nonEmpty) throw new InvalidInputException(structural)
    val aggregates = valueCounts(types, spec)
    val counts = df.agg(aggregates.head, aggregates.tail: _*).collect().head
    var position = 0
    def next(): Long = {
      val value = counts.getLong(position)
      position += 1
      value
    }
    val rows = next()
    val problems = Vector.newBuilder[InputProblem]
    val timeNulls = next()
    val timeNonFinite = next()
    val timeNonPositive = next()
    if (timeNulls > 0 || timeNonFinite > 0)
      problems += InputProblem.InvalidValues(spec.time, timeNulls, timeNonFinite)
    if (timeNonPositive > 0) problems += InputProblem.NonPositiveValues(spec.time, timeNonPositive)
    val eventNulls = next()
    val eventNonBinary = next()
    val events = next()
    if (eventNulls > 0) problems += InputProblem.InvalidValues(spec.event, eventNulls, 0L)
    if (eventNonBinary > 0) problems += InputProblem.NonBinaryValues(spec.event, eventNonBinary)
    spec.features.foreach { name =>
      val nulls = next()
      val nonFinite = next()
      if (nulls > 0 || nonFinite > 0) problems += InputProblem.InvalidValues(name, nulls, nonFinite)
    }
    spec.strata.foreach { name =>
      val nulls = next()
      if (nulls > 0) problems += InputProblem.InvalidValues(name, nulls, 0L)
    }
    spec.rowId.foreach { name =>
      val nulls = next()
      val duplicates = (rows - nulls) - next()
      if (nulls > 0) problems += InputProblem.InvalidValues(name, nulls, 0L)
      if (duplicates > 0) problems += InputProblem.DuplicateRowIds(name, duplicates)
    }
    if (rows == 0) problems += InputProblem.EmptyInput
    val found = problems.result()
    if (found.nonEmpty) throw new InvalidInputException(found)
    if (events == 0) throw new InvalidInputException(Seq(InputProblem.NoEvents))

    val strataKeyIsText = spec.strata.exists(name => types(name) == StringType)
    val group = spec.strata match {
      case Some(name) if strataKeyIsText => Validation.column(name)
      case Some(name)                    => Validation.column(name).cast(LongType)
      case None                          => lit(0L)
    }
    val event = Validation.column(spec.event)
    val frame = df.select(
      group.as(Validation.GroupColumn),
      Validation.column(spec.time).cast(DoubleType).as(TimeColumn),
      (if (types(spec.event) == BooleanType) event else event === 1).as(EventColumn),
      array(spec.features.map(name => Validation.column(name).cast(DoubleType)): _*)
        .as(Validation.FeaturesColumn),
      spec.rowId
        .fold(lit(0L))(name => Validation.column(name).cast(LongType))
        .as(Validation.RowIdColumn)
    )
    CoxInput(frame, spec, rows, events, strataKeyIsText)
  }

  private def schemaProblems(types: Map[String, DataType], spec: CoxSpec): Seq[InputProblem] = {
    def check(name: String, accepted: DataType => Boolean, expected: String): Option[InputProblem] =
      types.get(name) match {
        case None                            => Some(InputProblem.MissingColumn(name))
        case Some(found) if !accepted(found) =>
          Some(InputProblem.UnsupportedType(name, found.simpleString, expected))
        case Some(_) => None
      }
    val numeric = (t: DataType) => t.isInstanceOf[NumericType]
    Seq(
      check(spec.time, numeric, "a numeric type"),
      check(
        spec.event,
        t => Validation.isIntegral(t) || t == BooleanType,
        "an integral or boolean type"
      )
    ).flatten ++
      spec.features.flatMap(name => check(name, numeric, "a numeric type")) ++
      spec.strata.toSeq.flatMap(name =>
        check(name, t => Validation.isIntegral(t) || t == StringType, "an integral or string type")
      ) ++
      spec.rowId.toSeq.flatMap(name => check(name, Validation.isIntegral, "an integral type"))
  }

  private def valueCounts(types: Map[String, DataType], spec: CoxSpec): Seq[Column] = {
    def countWhere(condition: Column): Column =
      coalesce(sum(when(condition, 1L).otherwise(0L)), lit(0L))
    val time = Validation.column(spec.time)
    val event = Validation.column(spec.event)
    val eventCounts =
      if (types(spec.event) == BooleanType)
        Seq(countWhere(event.isNull), countWhere(lit(false)), countWhere(event === true))
      else
        Seq(
          countWhere(event.isNull),
          countWhere(event.isNotNull && !event.isin(0, 1)),
          countWhere(event === 1)
        )
    val features = spec.features.flatMap { name =>
      val value = Validation.column(name)
      Seq(countWhere(value.isNull), countWhere(Validation.nonFinite(value, types(name))))
    }
    val strata = spec.strata.toSeq.map(name => countWhere(Validation.column(name).isNull))
    val rowId = spec.rowId.toSeq.flatMap { name =>
      Seq(countWhere(Validation.column(name).isNull), countDistinct(Validation.column(name)))
    }
    Seq(
      count(lit(1)),
      countWhere(time.isNull),
      countWhere(Validation.nonFinite(time, types(spec.time))),
      countWhere(time <= 0)
    ) ++ eventCounts ++ features ++ strata ++ rowId
  }
}
