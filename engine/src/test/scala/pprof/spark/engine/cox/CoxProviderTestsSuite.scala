package pprof.spark.engine.cox

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** Provider tests (Phase 1d specification §2, §7) against pprof_py's `CoxPH.test` at its estimates. */
class CoxProviderTestsSuite extends SparkSuite {

  private val cases = Seq(
    "tiny-ties",
    "rc-unstratified",
    "rc-stratified",
    "rc-stratified-weights-offset",
    "lt-stratified",
    "lt-weights-offset"
  )
  private val methods = Seq[Ties](Ties.Breslow, Ties.Efron)

  private def tight(ties: Ties) = CoxOptions(ties = ties, eps = 1e-11, maxIterations = 100)

  private def info(name: String): JsonNode = Fixtures.json(s"cox/$name/case.json")

  private def flag(name: String, key: String): Boolean =
    Option(info(name).get(key)).exists(_.asBoolean)

  private def features(name: String): Seq[String] = {
    val node = info(name).get("features")
    (0 until node.size).map(i => node.get(i).asText())
  }

  private def spec(name: String): CoxSpec =
    CoxSpec(
      "time",
      "event",
      features(name),
      if (flag(name, "stratified")) Some("stratum") else None,
      Some("id"),
      if (flag(name, "weighted")) Some("weight") else None,
      if (flag(name, "offset")) Some("offset") else None,
      if (flag(name, "truncated")) Some("entry") else None
    )

  private def providerColumn(name: String): String =
    if (flag(name, "stratified")) "stratum" else "provider"

  private def frame(name: String): DataFrame = {
    val table = Fixtures.csv(s"cox/$name/input.csv")
    val extra = if (table.columns.contains("entry")) Seq("entry") else Seq.empty
    val names = Seq("id", "stratum", "time", "event", "weight", "offset") ++ extra ++ features(name)
    val index = names.map(table.columns.indexOf(_))
    val rows = table.rows.map { r =>
      Row.fromSeq(
        Seq[Any](r(index(0)).toLong, r(index(1)).toLong, r(index(2)), r(index(3)).toInt) ++
          index.drop(4).map(r(_))
      )
    }
    val schema = StructType(
      Seq(
        StructField("id", LongType),
        StructField("stratum", LongType),
        StructField("time", DoubleType),
        StructField("event", IntegerType),
        StructField("weight", DoubleType),
        StructField("offset", DoubleType)
      ) ++ (extra ++ features(name)).map(StructField(_, DoubleType))
    )
    spark.createDataFrame(rows.asJava, schema).withColumn("provider", col("id") % 10L)
  }

  private def reference(name: String, ties: Ties): JsonNode =
    Fixtures.json(s"cox/$name/pprof_py.json").get(ties.name)

  private def doubles(node: JsonNode, key: String): Array[Double] = Fixtures.doubles(node.get(key))

  private def check(
      cls: String,
      what: String,
      actual: Seq[Double],
      expected: Array[Double]
  ): Unit = {
    val (finite, infinite) = actual.indices.partition(i => !expected(i).isInfinite)
    infinite.foreach(i => assertEquals(actual(i), expected(i), s"$what at $i"))
    val ratio = Tolerances(cls).worstRatio(finite.map(actual).toArray, finite.map(expected).toArray)
    assert(ratio <= 1.0, s"$what: worst ratio $ratio under $cls")
  }

  for (name <- cases; ties <- methods)
    test(s"mid-p and exact provider tests against pprof_py at its estimates: $name, ${ties.name}") {
      val coef = doubles(reference(name, ties).get("tight"), "coef")
      val fitted = CoxPH.fit(frame(name), spec(name), tight(ties))
      val fit = fitted.copy(coefficients = fitted.coefficients.zip(coef).map { case (c, b) =>
        c.copy(estimate = b)
      })
      val provider = providerColumn(name)
      val threshold = Tolerances.number("T-p.log10.threshold")
      val atol = Tolerances.number("T-p.log10.atol")
      for ((method, key) <- Seq(TestMethod.MidP -> "midp", TestMethod.Exact -> "exact")) {
        val expected = reference(name, ties).get("tests").get(key)
        val rows =
          CoxProviderTests.test(frame(name), fit, provider, method).orderBy(provider).collect()
        val what = s"$name ${ties.name} $key"
        assertEquals(
          rows.map(_.getAs[Long](provider)).toSeq,
          (0 until expected.get("provider").size).map(i => expected.get("provider").get(i).asLong)
        )
        assertEquals(
          rows.map(_.getAs[Int]("flag")).toSeq,
          (0 until expected.get("flag").size).map(i => expected.get("flag").get(i).asInt),
          s"$what flags"
        )
        check(
          "T-base",
          s"$what estimate",
          rows.map(_.getAs[Double]("estimate")).toSeq,
          doubles(expected, "estimate")
        )
        check(
          "T-test",
          s"$what z",
          rows.map(_.getAs[Double]("z_raw")).toSeq,
          doubles(expected, "z_raw")
        )
        val p = doubles(expected, "p_value")
        rows.map(_.getAs[Double]("p_value")).zip(p).foreach { case (ours, theirs) =>
          if (theirs < threshold)
            assert(
              math.abs(StrictMath.log10(ours) - StrictMath.log10(theirs)) <= atol,
              s"$what p $ours vs $theirs"
            )
          else assert(Tolerances("T-test").accepts(ours, theirs), s"$what p $ours vs $theirs")
        }
        check(
          "T-base",
          s"$what lower",
          rows.map(_.getAs[Double]("ci_lower")).toSeq,
          doubles(expected, "ci_lower")
        )
        check(
          "T-base",
          s"$what upper",
          rows.map(_.getAs[Double]("ci_upper")).toSeq,
          doubles(expected, "ci_upper")
        )
        assert(
          rows.forall(r =>
            r.getAs[Double]("se").isNaN && r.getAs[Double]("null_value") == 1.0 && r.isNullAt(
              r.fieldIndex("null_group")
            )
          )
        )
      }
    }

  test("the result has pprof_py's columns, and a level outside (0, 1) fails") {
    val name = "rc-stratified"
    val fit = CoxPH.fit(frame(name), spec(name))
    val out = CoxProviderTests.test(frame(name), fit, "stratum")
    assertEquals(out.columns.toSeq, "stratum" +: CoxProviderTests.Columns)
    intercept[IllegalArgumentException](
      CoxProviderTests.test(frame(name), fit, "stratum", level = 1.0)
    )
  }
}
