package pprof.spark.engine.logistic

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{
  array,
  coalesce,
  count,
  countDistinct,
  lit,
  max,
  min,
  sum,
  when
}
import org.apache.spark.sql.types.{
  BooleanType,
  DataType,
  DoubleType,
  LongType,
  NumericType,
  StringType
}
import pprof.spark.engine.data.{InputProblem, InvalidInputException, Validation}

/** Validated logistic input in the engine's internal form (Phase 2a specification §4). */
final case class LogisticInput(
    frame: DataFrame,
    spec: LogisticSpec,
    rowCount: Long,
    providerKeyIsText: Boolean
)

/** One job of counts over every input row, before screening, as pprof_py validates (§4). Messages
  * carry column names and counts, never values (T6).
  */
object LogisticValidation {

  val OutcomeColumn: String = "__pprof_y"
  val TrialsColumn: String = "__pprof_n"

  def validate(df: DataFrame, spec: LogisticSpec): LogisticInput = {
    val types = df.schema.fields.map(f => f.name -> f.dataType).toMap
    val structural = schemaProblems(types, spec)
    if (structural.nonEmpty) throw new InvalidInputException(structural)
    val aggregates = valueCounts(types, spec).zipWithIndex.map { case (c, i) => c.as(s"count_$i") }
    val counts = df.agg(aggregates.head, aggregates.tail: _*).collect().head
    var position = 0
    def next(): Long = {
      val value = counts.getLong(position)
      position += 1
      value
    }
    def nextDouble(): Option[Double] = {
      val value = if (counts.isNullAt(position)) None else Some(counts.getDouble(position))
      position += 1
      value
    }
    val rows = next()
    val problems = Vector.newBuilder[InputProblem]
    val outcomeNulls = next()
    if (outcomeNulls > 0) problems += InputProblem.InvalidValues(spec.outcome, outcomeNulls, 0L)
    spec.trials match {
      case None =>
        val nonBinary = next()
        if (nonBinary > 0) problems += InputProblem.NonBinaryValues(spec.outcome, nonBinary)
      case Some(trials) =>
        val negative = next()
        val above = next()
        val trialNulls = next()
        val nonPositive = next()
        if (negative > 0) problems += InputProblem.NegativeValues(spec.outcome, negative)
        if (above > 0) problems += InputProblem.EventsExceedTrials(spec.outcome, trials, above)
        if (trialNulls > 0) problems += InputProblem.InvalidValues(trials, trialNulls, 0L)
        if (nonPositive > 0) problems += InputProblem.NonPositiveValues(trials, nonPositive)
    }
    spec.features.foreach { name =>
      val nulls = next()
      val nonFinite = next()
      val low = nextDouble()
      val high = nextDouble()
      if (nulls > 0 || nonFinite > 0) problems += InputProblem.InvalidValues(name, nulls, nonFinite)
      else if (low.isDefined && low == high) problems += InputProblem.ConstantFeature(name)
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
    val outcome = Validation.column(spec.outcome)
    val frame = df.select(
      (if (providerKeyIsText) provider else provider.cast(LongType)).as(Validation.GroupColumn),
      (if (types(spec.outcome) == BooleanType) when(outcome, 1.0).otherwise(0.0)
       else outcome.cast(DoubleType)).as(OutcomeColumn),
      spec.trials.fold(lit(1.0))(name => Validation.column(name).cast(DoubleType)).as(TrialsColumn),
      array(spec.features.map(name => Validation.column(name).cast(DoubleType)): _*)
        .as(Validation.FeaturesColumn),
      spec.rowId
        .fold(lit(0L))(name => Validation.column(name).cast(LongType))
        .as(Validation.RowIdColumn)
    )
    LogisticInput(frame, spec, rows, providerKeyIsText)
  }

  private def schemaProblems(
      types: Map[String, DataType],
      spec: LogisticSpec
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
      Seq(
        check(
          spec.outcome,
          t => Validation.isIntegral(t) || (spec.trials.isEmpty && t == BooleanType),
          if (spec.trials.isEmpty) "an integral or boolean type" else "an integral type"
        ),
        check(
          spec.provider,
          t => Validation.isIntegral(t) || t == StringType,
          "an integral or string type"
        )
      ).flatten ++
      spec.trials.toSeq.flatMap(name => check(name, Validation.isIntegral, "an integral type")) ++
      spec.features.flatMap(name => check(name, numeric, "a numeric type")) ++
      spec.rowId.toSeq.flatMap(name => check(name, Validation.isIntegral, "an integral type"))
  }

  private def valueCounts(types: Map[String, DataType], spec: LogisticSpec): Seq[Column] = {
    def countWhere(condition: Column): Column =
      coalesce(sum(when(condition, 1L).otherwise(0L)), lit(0L))
    val outcome = Validation.column(spec.outcome)
    val outcomeChecks = spec.trials match {
      case None if types(spec.outcome) == BooleanType => Seq(countWhere(lit(false)))
      case None       => Seq(countWhere(outcome.isNotNull && outcome =!= 0 && outcome =!= 1))
      case Some(name) =>
        val trials = Validation.column(name)
        Seq(
          countWhere(outcome < 0),
          countWhere(outcome > trials),
          countWhere(trials.isNull),
          countWhere(trials < 1)
        )
    }
    val features = spec.features.flatMap { name =>
      val value = Validation.column(name)
      Seq(
        countWhere(value.isNull),
        countWhere(Validation.nonFinite(value, types(name))),
        min(value.cast(DoubleType)),
        max(value.cast(DoubleType))
      )
    }
    val rowId = spec.rowId.toSeq.flatMap { name =>
      val value = Validation.column(name)
      Seq(countWhere(value.isNull), countDistinct(value))
    }
    Seq(count(lit(1)), countWhere(outcome.isNull)) ++ outcomeChecks ++ features ++
      Seq(countWhere(Validation.column(spec.provider).isNull)) ++ rowId
  }
}
