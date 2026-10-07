package pprof.spark.engine.cox

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, concat, lit, lower, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import pprof.spark.engine.data.{InputProblem, InvalidInputException}
import pprof.spark.testkit.{Fixtures, SparkSuite, Tolerances}

/** Robust variance, per row and clustered (Phase 1c specification §2, §4, §5). R is the reference
  * everywhere; pprof_py too, except for Breslow on (start, stop] data (X-015), where the sandwich of
  * its own dfbeta residuals stands in.
  */
class CoxRobustSuite extends SparkSuite {

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

  /** The fixture input plus `cluster` = id mod 40, as the fixtures' clustered variances use. */
  private def frame(name: String, reverse: Boolean = false): DataFrame = {
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
    spark
      .createDataFrame((if (reverse) rows.reverse else rows).asJava, schema)
      .withColumn("cluster", col("id") % 40L)
  }

  private def reference(name: String, source: String, ties: Ties): JsonNode =
    Fixtures.json(s"cox/$name/$source.json").get(ties.name)

  private def doubles(node: JsonNode, key: String): Array[Double] = Fixtures.doubles(node.get(key))

  private def check(
      cls: String,
      what: String,
      actual: Seq[Double],
      expected: Array[Double]
  ): Unit = {
    val ratio = Tolerances(cls).worstRatio(actual.toArray, expected)
    assert(ratio <= 1.0, s"$what: worst ratio $ratio under $cls")
  }

  private def bits(values: Seq[Double]): Seq[Long] = values.map(java.lang.Double.doubleToLongBits)

  for (name <- cases; ties <- methods)
    test(
      s"robust variance per row and clustered against R and pprof_py (T-var): $name, ${ties.name}"
    ) {
      val plain = CoxPH.fit(frame(name), spec(name), tight(ties))
      val perRow = CoxPH.fit(frame(name), spec(name), tight(ties).copy(robust = true))
      val clustered =
        CoxPH.fit(frame(name), spec(name).copy(cluster = Some("cluster")), tight(ties))
      val what = s"$name ${ties.name}"
      assert(perRow.robust && clustered.robust && !plain.robust)
      assertEquals(bits(perRow.naiveCovariance), bits(plain.covariance))
      assertEquals(bits(perRow.estimates), bits(plain.estimates))
      assertEquals(perRow.clusters, plain.observations)
      assertEquals(
        clustered.clusters,
        Fixtures
          .json(s"cox/$name/pprof_py.json")
          .get(ties.name)
          .get("residuals")
          .get("clusters")
          .asLong
      )
      val r = reference(name, "r_survival", ties).get("tight")
      check("T-var", s"$what per row vs R", perRow.covariance, doubles(r, "robust_per_row"))
      check("T-var", s"$what clustered vs R", clustered.covariance, doubles(r, "robust_clustered"))
      val py = reference(name, "pprof_py", ties).get("residuals")
      val x015 = flag(name, "truncated") && ties == Ties.Breslow
      def pyRobust(kind: String): Array[Double] =
        if (!x015) doubles(py, s"robust_$kind")
        else {
          val v = doubles(py, "naive_covariance")
          val b = doubles(py, s"robust_${kind}_from_dfbeta")
          // pprof_py stores the dfbeta sandwich D'D = V B V directly
          assert(v.length == b.length)
          b
        }
      check("T-var", s"$what per row vs pprof_py", perRow.covariance, pyRobust("per_row"))
      check("T-var", s"$what clustered vs pprof_py", clustered.covariance, pyRobust("clustered"))
      val se = perRow.standardErrors
      val diagonal = features(name).indices
        .map(j => perRow.covariance(j * features(name).size - j * (j - 1) / 2))
      assertEquals(bits(se), bits(diagonal.map(math.sqrt)))
    }

  test("clusters of one row give the per-row robust variance; renaming clusters changes nothing") {
    val name = "rc-stratified-weights-offset"
    val perRow = CoxPH.fit(frame(name), spec(name), CoxOptions(robust = true))
    val single = CoxPH.fit(
      frame(name).withColumn("cluster", col("id")),
      spec(name).copy(cluster = Some("cluster"))
    )
    check("T-var", "clusters of one row", single.covariance, perRow.covariance.toArray)
    val base = CoxPH.fit(frame(name), spec(name).copy(cluster = Some("cluster")))
    val renamed = CoxPH.fit(
      frame(name)
        .withColumn("cluster", concat(lit("c-"), (col("cluster") * 7L + 3L).cast("string"))),
      spec(name).copy(cluster = Some("cluster"))
    )
    check("T-var", "renamed clusters", renamed.covariance, base.covariance.toArray)
    assertEquals(renamed.clusters, base.clusters)
  }

  test("R0: clustered robust variance is bitwise identical across row orders and partitions") {
    val name = "lt-weights-offset"
    val s = spec(name).copy(cluster = Some("cluster"))
    for (ties <- methods) {
      val a = CoxPH.fit(frame(name), s, CoxOptions(ties = ties))
      val b = CoxPH.fit(frame(name, reverse = true).repartition(7), s, CoxOptions(ties = ties))
      assertEquals(bits(b.covariance), bits(a.covariance))
      assertEquals(b.fingerprint, a.fingerprint)
    }
  }

  test("per-row robust variance on data with entry times warns; a cluster column does not") {
    val name = "lt-stratified"
    val perRow = CoxPH.fit(frame(name), spec(name), CoxOptions(robust = true))
    assert(perRow.warnings.exists(_.contains("own cluster")), perRow.warnings.toString)
    val clustered = CoxPH.fit(frame(name), spec(name).copy(cluster = Some("cluster")))
    assert(clustered.warnings.isEmpty, clustered.warnings.toString)
  }

  test("a cluster column with nulls fails with counts") {
    val name = "rc-stratified"
    val withNulls = frame(name).withColumn(
      "cluster",
      when(col("id") < 5L, lit(null)).otherwise(lower(col("cluster").cast("string")))
    )
    val e = intercept[InvalidInputException](
      CoxPH.fit(withNulls, spec(name).copy(cluster = Some("cluster")))
    )
    assertEquals(e.problems, Seq[InputProblem](InputProblem.InvalidValues("cluster", 5L, 0L)))
  }

  test("a robust fit saves and loads bit for bit (format version 3)") {
    val name = "lt-weights-offset"
    val fit = CoxPH.fit(
      frame(name),
      spec(name).copy(cluster = Some("cluster")),
      CoxOptions(ties = Ties.Efron)
    )
    val path = Files.createTempDirectory("pprof-cox").resolve("model").toString
    CoxFitIO.save(spark, fit, path)
    val loaded = CoxFitIO.load(spark, path)
    assertEquals(loaded, fit)
    assertEquals(bits(loaded.naiveCovariance), bits(fit.naiveCovariance))
    assertEquals((loaded.robust, loaded.clusters), (true, 40L))
  }
}
