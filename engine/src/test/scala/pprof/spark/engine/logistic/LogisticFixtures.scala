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
    LogisticSpec(
      "y",
      Features,
      "provider",
      if (binomial(name)) Some("n") else None,
      Some("id"),
      if (clustered(name)) Some("patient") else None
    )

  def clustered(name: String): Boolean = table(name).columns.contains("patient")

  /** The input as a DataFrame: integral identifiers, outcome and trials, double features. */
  def frame(spark: SparkSession, name: String, reverse: Boolean = false): DataFrame = {
    val t = table(name)
    val trials = binomial(name)
    val patients = clustered(name)
    val rows = t.rows.map { r =>
      def at(column: String) = r(t.columns.indexOf(column))
      Row.fromSeq(
        Seq[Any](at("id").toLong, at("provider").toLong, at("y").toInt) ++
          (if (patients) Seq(at("patient").toLong) else Seq.empty) ++
          (if (trials) Seq(at("n").toInt) else Seq.empty) ++ Features.map(at)
      )
    }
    val schema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("provider", LongType, nullable = false),
        StructField("y", IntegerType, nullable = false)
      ) ++ (if (patients) Seq(StructField("patient", LongType, nullable = false))
            else Seq.empty) ++ (if (trials) Seq(StructField("n", IntegerType, nullable = false))
                                else Seq.empty) ++
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

  /** A column of a provider-level table aligned to `providers` (integral keys): NaN where a provider has no
    * row or a null value.
    */
  def alignedColumn(table: DataFrame, name: String, providers: Seq[Long]): Array[Double] = {
    val byKey = table
      .select("provider", name)
      .collect()
      .map(r =>
        r.getAs[Number](0).longValue -> (if (r.isNullAt(1)) Double.NaN
                                         else r.getAs[Number](1).doubleValue)
      )
      .toMap
    providers.map(k => byKey.getOrElse(k, Double.NaN)).toArray
  }

  /** A provider-test table against pprof_py's columns in a fixture node whose rows follow `providers`: the same
    * providers untested; z, and z_adjusted where the fixture has it, within T-test; p-values where pprof_py's
    * are accurate; interval ends within T-coef, or identical where not finite; and the same flags except where
    * the statistic that decides them lies within 1e-8 of the critical value. `reliable` leaves out providers
    * whose reference z is outside pprof_py's accuracy (X-024).
    */
  def checkTestTable(
      what: String,
      table: DataFrame,
      ref: JsonNode,
      providers: Seq[Long],
      reliable: Int => Boolean = _ => true,
      critical: Double = 1.959963984540054
  ): Unit = {
    def ours(name: String) = alignedColumn(table, name, providers)
    val z = ours("z_raw")
    val expected = doubles(ref, "z")
    assert(
      z.indices.forall(i => z(i).isNaN == expected(i).isNaN),
      s"$what: the untested providers differ"
    )
    val compared = expected.indices.filter(i => expected(i).isFinite && reliable(i))
    check("T-test", s"$what z", compared.map(z), compared.map(expected).toArray)
    val deciding =
      if (!ref.has("z_adjusted")) z
      else {
        val adjusted = ours("z_adjusted")
        check(
          "T-test",
          s"$what z_adjusted",
          compared.map(adjusted),
          compared.map(doubles(ref, "z_adjusted")).toArray
        )
        adjusted
      }
    checkPValues(what, compared.map(ours("p_value")), compared.map(doubles(ref, "p")).toArray)
    Seq("ci_lower", "ci_upper").foreach { side =>
      val (a, e) = (ours(side), doubles(ref, side))
      compared.filterNot(i => e(i).isFinite).foreach { i =>
        assert(
          java.lang.Double.compare(a(i), e(i)) == 0,
          s"$what $side of provider ${providers(i)}: ${a(i)}, pprof_py ${e(i)}"
        )
      }
      val finite = compared.filter(i => e(i).isFinite)
      check("T-coef", s"$what $side", finite.map(a), finite.map(e).toArray)
    }
    val byKey = table
      .select("provider", "flag")
      .collect()
      .map(r =>
        r.getAs[Number](0).longValue -> (if (r.isNullAt(1)) None
                                         else Some(r.getAs[Number](1).intValue))
      )
      .toMap
    val flags = providers.map(k => byKey.getOrElse(k, None))
    val node = ref.get("flag")
    val theirs =
      providers.indices.map(i => if (node.get(i).isNull) None else Some(node.get(i).asInt))
    providers.indices.filter(i => flags(i) != theirs(i)).foreach { i =>
      assert(
        math.abs(math.abs(deciding(i)) - critical) <= 1e-8 * critical,
        s"$what: the flag of provider ${providers(i)} differs away from the threshold: ${flags(i)}, pprof_py ${theirs(i)}"
      )
    }
  }
}
