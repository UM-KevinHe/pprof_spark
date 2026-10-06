package pprof.spark.engine.cox

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, coalesce, col, exp, last, least, lit, sum, when}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, NumericType, StringType}

import pprof.spark.engine.data.{InputProblem, InvalidInputException, Validation}

/** Predictions from a Cox fit as DataFrame columns (Phase 1b specification §3 and §5): the linear
  * predictor xᵀβ̂ + o (uncentered), the relative hazard exp(min(η, 700)) as pprof_py's `safe_exp`,
  * and, at each row's own time and stratum, the cumulative hazard Λ₀ₛ(t)·exp(min(η, 700)) and the
  * survival exp(−Λ). Λ₀ₛ is right-continuous: 0 before the stratum's first event time and constant
  * after its last. Spark's EXP uses StrictMath (§8.1). Rows with missing or non-finite inputs, or a
  * stratum without a baseline, fail with counts and never values (NN-7).
  */
object CoxPrediction {

  val LinearPredictor: String = "linear_predictor"
  val RelativeHazard: String = "relative_hazard"
  val CumulativeHazard: String = "cumulative_hazard"
  val Survival: String = "survival"

  private val Clip = 700.0
  private val Key = "__pprof_key"
  private val Time = "__pprof_time"
  private val Kind = "__pprof_kind"
  private val Cumulative = "__pprof_cumulative"

  def linearPredictor(fit: CoxFit, df: DataFrame): DataFrame = {
    validate(fit, df, None, None)
    df.withColumn(LinearPredictor, eta(fit))
  }

  def relativeHazard(fit: CoxFit, df: DataFrame): DataFrame = {
    validate(fit, df, None, None)
    df.withColumn(RelativeHazard, exp(least(eta(fit), lit(Clip))))
  }

  /** Adds the cumulative hazard at each row's time in `timeCol`. */
  def cumulativeHazard(
      fit: CoxFit,
      baseline: DataFrame,
      df: DataFrame,
      timeCol: String
  ): DataFrame = {
    val keyType = baseline.schema("stratum").dataType
    validate(fit, df, Some(timeCol), Some(baseline -> keyType))
    val rows = df
      .withColumn(Key, key(fit, keyType))
      .withColumn(Time, Validation.column(timeCol).cast(DoubleType))
      .withColumn(Kind, lit(1))
    val steps = baseline.select(
      col("stratum").as(Key),
      col("time").as(Time),
      lit(0).as(Kind),
      col("cumulative_hazard").as(Cumulative)
    )
    val window = Window
      .partitionBy(Key)
      .orderBy(col(Time), col(Kind))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    rows
      .unionByName(steps, allowMissingColumns = true)
      .withColumn(Cumulative, last(col(Cumulative), ignoreNulls = true).over(window))
      .filter(col(Kind) === 1)
      .withColumn(
        CumulativeHazard,
        coalesce(col(Cumulative), lit(0.0)) * exp(least(eta(fit), lit(Clip)))
      )
      .drop(Key, Time, Kind, Cumulative)
  }

  /** Adds the cumulative hazard and the survival exp(−Λ) at each row's time in `timeCol`. */
  def survival(fit: CoxFit, baseline: DataFrame, df: DataFrame, timeCol: String): DataFrame =
    cumulativeHazard(fit, baseline, df, timeCol).withColumn(Survival, exp(-col(CumulativeHazard)))

  private def eta(fit: CoxFit): Column = {
    val linear = fit.spec.features
      .zip(fit.estimates)
      .map { case (name, b) => Validation.column(name).cast(DoubleType) * lit(b) }
      .reduce(_ + _)
    fit.spec.offset.fold(linear)(name => linear + Validation.column(name).cast(DoubleType))
  }

  private def key(fit: CoxFit, keyType: DataType): Column =
    fit.spec.strata.fold(lit(0L).cast(keyType)) { name =>
      Validation.column(name).cast(if (keyType == StringType) StringType else LongType)
    }

  private def validate(
      fit: CoxFit,
      df: DataFrame,
      timeCol: Option[String],
      baseline: Option[(DataFrame, DataType)]
  ): Unit = {
    val types = df.schema.fields.map(f => f.name -> f.dataType).toMap
    val numeric = fit.spec.features ++ fit.spec.offset.toSeq ++ timeCol.toSeq
    val needed = numeric ++ (if (baseline.isDefined) fit.spec.strata.toSeq else Seq.empty)
    val structural = needed.flatMap { name =>
      types.get(name) match {
        case None => Some(InputProblem.MissingColumn(name))
        case Some(t) if numeric.contains(name) && !t.isInstanceOf[NumericType] =>
          Some(InputProblem.UnsupportedType(name, t.simpleString, "a numeric type"))
        case Some(_) => None
      }
    }
    if (structural.nonEmpty) throw new InvalidInputException(structural)
    def countWhere(condition: Column): Column =
      coalesce(sum(when(condition, 1L).otherwise(0L)), lit(0L))
    val counts = numeric.zipWithIndex.flatMap { case (name, i) =>
      val value = Validation.column(name)
      Seq(
        countWhere(value.isNull).as(s"nulls_$i"),
        countWhere(Validation.nonFinite(value, types(name))).as(s"nonfinite_$i")
      )
    }
    val row = df.agg(counts.head, counts.tail: _*).collect().head
    val problems = Vector.newBuilder[InputProblem]
    numeric.zipWithIndex.foreach { case (name, i) =>
      val nulls = row.getLong(2 * i)
      val nonFinite = row.getLong(2 * i + 1)
      if (nulls > 0 || nonFinite > 0) problems += InputProblem.InvalidValues(name, nulls, nonFinite)
    }
    baseline.foreach { case (table, keyType) =>
      val known = table.select(col("stratum").as(Key)).distinct()
      val unknown =
        df.select(key(fit, keyType).as(Key)).join(broadcast(known), Seq(Key), "left_anti").count()
      if (unknown > 0)
        problems += InputProblem.UnknownStrata(fit.spec.strata.getOrElse("stratum"), unknown)
    }
    val found = problems.result()
    if (found.nonEmpty) throw new InvalidInputException(found)
  }
}
