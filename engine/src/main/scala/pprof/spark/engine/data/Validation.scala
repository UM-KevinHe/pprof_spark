package pprof.spark.engine.data

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{
  array,
  coalesce,
  col,
  count,
  countDistinct,
  isnan,
  lit,
  sum,
  when
}
import org.apache.spark.sql.types.{
  ByteType,
  DataType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  NumericType,
  ShortType,
  StringType
}

/** One reason why an input failed validation. Messages name columns and counts, never values
  * (NN-7, §12).
  */
sealed abstract class InputProblem(val column: String, val description: String)
    extends Product
    with Serializable {
  def message: String = if (column.isEmpty) description else s"column $column $description"
}

object InputProblem {
  final case class MissingColumn(name: String) extends InputProblem(name, "is missing")
  final case class UnsupportedType(name: String, found: String, expected: String)
      extends InputProblem(name, s"has type $found; expected $expected")
  final case class InvalidValues(name: String, nulls: Long, nonFinite: Long)
      extends InputProblem(name, s"has $nulls null and $nonFinite NaN or infinite values")
  final case class DuplicateRowIds(name: String, duplicates: Long)
      extends InputProblem(name, s"has $duplicates duplicated identifiers")
  case object EmptyInput extends InputProblem("", "the input has no rows")
  final case class NonPositiveValues(name: String, count: Long)
      extends InputProblem(name, s"has $count values that are not positive")
  final case class NonBinaryValues(name: String, count: Long)
      extends InputProblem(name, s"has $count values other than 0 and 1")
  case object NoEvents
      extends InputProblem("", "the input has no events, so the partial likelihood is constant")
}

/** The input violates its data contract; `problems` lists every violation found. */
final class InvalidInputException(val problems: Seq[InputProblem])
    extends IllegalArgumentException(problems.map(_.message).mkString("invalid input: ", "; ", ""))

/** Input in the engine's internal form: the group key as a long or a string, the features as an
  * array of doubles, and the row identifier (0 when none was given).
  */
final case class ValidatedInput(
    frame: DataFrame,
    spec: InputSpec,
    rowCount: Long,
    groupKeyIsText: Boolean
)

object Validation {

  val GroupColumn: String = "__pprof_group"
  val FeaturesColumn: String = "__pprof_x"
  val RowIdColumn: String = "__pprof_row_id"

  /** Checks the schema eagerly, then counts invalid values in one aggregation (§5.5, §7.2), and
    * fails with every problem found. The aggregation is one Spark job; it is Connect-compatible.
    */
  def validate(df: DataFrame, spec: InputSpec): ValidatedInput = {
    val types = df.schema.fields.map(f => f.name -> f.dataType).toMap
    val structural = schemaProblems(types, spec)
    if (structural.nonEmpty) throw new InvalidInputException(structural)

    val aggregates = valueCounts(types, spec)
    val counts = df.agg(aggregates.head, aggregates.tail: _*).collect().head
    val rows = counts.getLong(0)
    val problems = Vector.newBuilder[InputProblem]
    val groupNulls = counts.getLong(1)
    if (groupNulls > 0) problems += InputProblem.InvalidValues(spec.groupCol, groupNulls, 0L)
    spec.featureCols.zipWithIndex.foreach { case (name, k) =>
      val nulls = counts.getLong(2 + 2 * k)
      val nonFinite = counts.getLong(3 + 2 * k)
      if (nulls > 0 || nonFinite > 0) problems += InputProblem.InvalidValues(name, nulls, nonFinite)
    }
    spec.rowIdCol.foreach { name =>
      val base = 2 + 2 * spec.featureCols.size
      val nulls = counts.getLong(base)
      val duplicates = (rows - nulls) - counts.getLong(base + 1)
      if (nulls > 0) problems += InputProblem.InvalidValues(name, nulls, 0L)
      if (duplicates > 0) problems += InputProblem.DuplicateRowIds(name, duplicates)
    }
    if (rows == 0) problems += InputProblem.EmptyInput
    val found = problems.result()
    if (found.nonEmpty) throw new InvalidInputException(found)

    val groupKeyIsText = types(spec.groupCol) == StringType
    val groupKey =
      if (groupKeyIsText) column(spec.groupCol) else column(spec.groupCol).cast(LongType)
    val features = array(spec.featureCols.map(name => column(name).cast(DoubleType)): _*)
    val rowId = spec.rowIdCol.fold(lit(0L))(name => column(name).cast(LongType))
    val frame = df.select(
      groupKey.as(GroupColumn),
      features.as(FeaturesColumn),
      rowId.as(RowIdColumn)
    )
    ValidatedInput(frame, spec, rows, groupKeyIsText)
  }

  private def schemaProblems(types: Map[String, DataType], spec: InputSpec): Seq[InputProblem] = {
    def check(name: String, accepted: DataType => Boolean, expected: String) =
      types.get(name) match {
        case None                            => Some(InputProblem.MissingColumn(name))
        case Some(found) if !accepted(found) =>
          Some(InputProblem.UnsupportedType(name, found.simpleString, expected))
        case Some(_) => None
      }
    check(
      spec.groupCol,
      t => isIntegral(t) || t == StringType,
      "an integral or string type"
    ).toSeq ++
      spec.featureCols.flatMap(name =>
        check(name, _.isInstanceOf[NumericType], "a numeric type")
      ) ++
      spec.rowIdCol.toSeq.flatMap(name => check(name, isIntegral, "an integral type"))
  }

  private def valueCounts(types: Map[String, DataType], spec: InputSpec): Seq[Column] = {
    def countWhere(condition: Column): Column =
      coalesce(sum(when(condition, 1L).otherwise(0L)), lit(0L))
    val features = spec.featureCols.flatMap { name =>
      Seq(countWhere(column(name).isNull), countWhere(nonFinite(column(name), types(name))))
    }
    val rowId = spec.rowIdCol.toSeq.flatMap { name =>
      Seq(countWhere(column(name).isNull), countDistinct(column(name)))
    }
    Seq(count(lit(1)), countWhere(column(spec.groupCol).isNull)) ++ features ++ rowId
  }

  private[engine] def nonFinite(value: Column, dataType: DataType): Column = dataType match {
    case DoubleType | FloatType =>
      isnan(value) || value === Double.PositiveInfinity || value === Double.NegativeInfinity
    case _ => lit(false)
  }

  private[engine] def isIntegral(dataType: DataType): Boolean = dataType match {
    case ByteType | ShortType | IntegerType | LongType => true
    case _                                             => false
  }

  /** A column reference that tolerates dots and other special characters in names. */
  private[engine] def column(name: String): Column = col("`" + name.replace("`", "``") + "`")
}
