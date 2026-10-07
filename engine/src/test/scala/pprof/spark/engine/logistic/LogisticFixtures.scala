package pprof.spark.engine.logistic

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
import pprof.spark.numerics.kernels.{LogisticFE => Kernel}
import pprof.spark.testkit.{Fixtures, Tolerances}

/** The logistic fixed-effect fixtures (fixtures/logistic, round 26) as test inputs and references. */
object LogisticFixtures {

  val Cases: Seq[String] = Fixtures.cases("logistic")
  val Features: Seq[String] = Seq("x1", "x2", "x3")

  def table(name: String): Fixtures.Table = Fixtures.csv(s"logistic/$name/input.csv")
  def binomial(name: String): Boolean = table(name).columns.contains("n")
  def json(name: String, source: String): JsonNode = Fixtures.json(s"logistic/$name/$source.json")
  def doubles(node: JsonNode, key: String): Array[Double] = Fixtures.doubles(node.get(key))
  def ints(node: JsonNode, key: String): Seq[Long] = {
    val array = node.get(key)
    (0 until array.size).map(i => array.get(i).asLong)
  }
  def booleans(node: JsonNode, key: String): Seq[Boolean] = {
    val array = node.get(key)
    (0 until array.size).map(i => array.get(i).asBoolean)
  }

  def spec(name: String): LogisticSpec =
    LogisticSpec("y", Features, "provider", if (binomial(name)) Some("n") else None, Some("id"))

  /** The input as a DataFrame: integral identifiers, outcome and trials, double features. */
  def frame(spark: SparkSession, name: String, reverse: Boolean = false): DataFrame = {
    val t = table(name)
    val trials = binomial(name)
    val rows = t.rows.map { r =>
      def at(column: String) = r(t.columns.indexOf(column))
      Row.fromSeq(
        Seq[Any](at("id").toLong, at("provider").toLong, at("y").toInt) ++
          (if (trials) Seq(at("n").toInt) else Seq.empty) ++ Features.map(at)
      )
    }
    val schema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("provider", LongType, nullable = false),
        StructField("y", IntegerType, nullable = false)
      ) ++ (if (trials) Seq(StructField("n", IntegerType, nullable = false)) else Seq.empty) ++
        Features.map(StructField(_, DoubleType, nullable = false))
    )
    spark.createDataFrame((if (reverse) rows.reverse else rows).asJava, schema)
  }

  /** The screened rows (providers with more than 10 records) as one in-memory block, providers by
    * key and rows by identifier: the engine's canonical order.
    */
  def block(name: String): Kernel.Rows = {
    val t = table(name)
    val provider = t.column("provider")
    val ids = t.column("id")
    val ys = t.column("y")
    val ns = if (binomial(name)) t.column("n") else Array.fill(provider.length)(1.0)
    val xs = Features.map(t.column)
    val counts = provider.groupBy(identity).view.mapValues(_.length).toMap
    val order =
      provider.indices.filter(i => counts(provider(i)) > 10).sortBy(i => (provider(i), ids(i)))
    val starts =
      order.indices.filter(k => k == 0 || provider(order(k)) != provider(order(k - 1))).toArray
    Kernel.Rows(
      starts,
      order.map(ys(_)).toArray,
      order.map(ns(_)).toArray,
      order.flatMap(i => xs.map(_(i))).toArray,
      Features.size
    )
  }

  def check(cls: String, what: String, actual: Seq[Double], expected: Array[Double]): Unit = {
    val ratio = Tolerances(cls).worstRatio(actual.toArray, expected)
    assert(ratio <= 1.0, s"$what: worst ratio $ratio under $cls")
  }

  /** Two-sided p-values where the reference is accurate. pprof_py computes 2(1 − Φ(abs(z))), whose
    * absolute error of a few ε makes its relative error exceed a tenth of T-test below p = 1e-6;
    * there the z statistics, compared under T-test, decide (T-p: "document the reference
    * algorithm's accuracy limit").
    */
  def checkPValues(what: String, actual: Seq[Double], expected: Array[Double]): Unit =
    actual.indices.filter(i => expected(i) >= 1e-6).foreach { i =>
      check("T-test", s"$what p-value $i", Seq(actual(i)), Array(expected(i)))
    }
}
