package pprof.spark.engine.linear

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{array, coalesce, count, countDistinct, lit, sum, when}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, NumericType, StringType}
import pprof.spark.engine.data.{InputProblem, InvalidInputException, Validation}

/** Validated input in the canonical columns (§5). */
final case class LinearInput(
    frame: DataFrame,
    spec: LinearSpec,
    rowCount: Long,
    providerKeyIsText: Boolean
)

/** Validation once over every input row, every problem reported at once, with counts and never values (T6). */
object LinearValidation {

  val OutcomeColumn: String = "__pprof_y"

  /** With `outcomeRequired = false` (prediction) the outcome is neither checked nor read. */
  def validate(df: DataFrame, spec: LinearSpec, outcomeRequired: Boolean = true): LinearInput = {
    val types = df.schema.fields.map(f => f.name -> f.dataType).toMap
    val structural = schemaProblems(types, spec, outcomeRequired)
    if (structural.nonEmpty) throw new InvalidInputException(structural)
    val aggregates = valueCounts(types, spec, outcomeRequired).zipWithIndex.map { case (c, i) =>
      c.as(s"count_$i")
    }
    val counts = df.agg(aggregates.head, aggregates.tail: _*).collect().head
    var position = 0
    def next(): Long = {
      val value = counts.getLong(position)
      position += 1
      value
    }
    val rows = next()
    val problems = Vector.newBuilder[InputProblem]
    if (outcomeRequired) {
      val nulls = next()
      val nonFinite = next()
      if (nulls > 0 || nonFinite > 0)
        problems += InputProblem.InvalidValues(spec.outcome, nulls, nonFinite)
    }
    spec.features.foreach { name =>
      val nulls = next()
      val nonFinite = next()
      if (nulls > 0 || nonFinite > 0) problems += InputProblem.InvalidValues(name, nulls, nonFinite)
    }
    val providerNulls = next()
    if (providerNulls > 0) problems += InputProblem.InvalidValues(spec.provider, providerNulls, 0L)
    spec.rowId.foreach { name =>
      val nulls = next()
      val duplicates = (rows - nulls) - next()
      if (nulls > 0) problems += InputProblem.InvalidValues(name, nulls, 0L)
      if (duplicates > 0) problems += InputProblem.DuplicateRowIds(name, duplicates)
    }
    if (rows == 0) problems += InputProblem.EmptyInput
    val found = problems.result()
    if (found.nonEmpty) throw new InvalidInputException(found)

    val providerKeyIsText = types(spec.provider) == StringType
    val provider = Validation.column(spec.provider)
    val frame = df.select(
      (if (providerKeyIsText) provider else provider.cast(LongType)).as(Validation.GroupColumn),
      (if (outcomeRequired) Validation.column(spec.outcome).cast(DoubleType) else lit(0.0))
        .as(OutcomeColumn),
      array(spec.features.map(name => Validation.column(name).cast(DoubleType)): _*)
        .as(Validation.FeaturesColumn),
      spec.rowId
        .fold(lit(0L))(name => Validation.column(name).cast(LongType))
        .as(Validation.RowIdColumn)
    )
    LinearInput(frame, spec, rows, providerKeyIsText)
  }

  private def schemaProblems(
      types: Map[String, DataType],
      spec: LinearSpec,
      outcomeRequired: Boolean
  ): Seq[InputProblem] = {
    def check(name: String, accepted: DataType => Boolean, expected: String): Option[InputProblem] =
      types.get(name) match {
        case None                            => Some(InputProblem.MissingColumn(name))
        case Some(found) if !accepted(found) =>
          Some(InputProblem.UnsupportedType(name, found.simpleString, expected))
        case Some(_) => None
      }
    val numeric = (t: DataType) => t.isInstanceOf[NumericType]
    (if (spec.features.isEmpty) Seq(InputProblem.NoFeatures) else Seq.empty) ++
      (if (outcomeRequired) check(spec.outcome, numeric, "a numeric type").toSeq else Seq.empty) ++
      check(
        spec.provider,
        t => Validation.isIntegral(t) || t == StringType,
        "an integral or string type"
      ).toSeq ++
      spec.features.flatMap(name => check(name, numeric, "a numeric type")) ++
      spec.rowId.toSeq.flatMap(name => check(name, Validation.isIntegral, "an integral type"))
  }

  private def valueCounts(
      types: Map[String, DataType],
      spec: LinearSpec,
      outcomeRequired: Boolean
  ): Seq[Column] = {
    def countWhere(condition: Column): Column =
      coalesce(sum(when(condition, 1L).otherwise(0L)), lit(0L))
    val outcome =
      if (!outcomeRequired) Seq.empty
      else {
        val value = Validation.column(spec.outcome)
        Seq(countWhere(value.isNull), countWhere(Validation.nonFinite(value, types(spec.outcome))))
      }
    val features = spec.features.flatMap { name =>
      val value = Validation.column(name)
      Seq(countWhere(value.isNull), countWhere(Validation.nonFinite(value, types(name))))
    }
    val rowId = spec.rowId.toSeq.flatMap { name =>
      val value = Validation.column(name)
      Seq(countWhere(value.isNull), countDistinct(value))
    }
    Seq(count(lit(1))) ++ outcome ++ features ++ Seq(
      countWhere(Validation.column(spec.provider).isNull)
    ) ++ rowId
  }
}
