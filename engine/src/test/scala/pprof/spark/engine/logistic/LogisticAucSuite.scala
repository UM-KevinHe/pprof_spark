package pprof.spark.engine.logistic

import java.util.SplittableRandom

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.layout.GroupKey
import pprof.spark.numerics.kernels.{LogisticFE => Kernel}
import pprof.spark.testkit.SparkSuite

/** The AUC pass against the exact Mann–Whitney statistic of the fitted π, computed on the driver with the same
  * kernel from the fit's effects and coefficients (Phase 2b §5; OI-119). Binary features give the fitted π few
  * distinct values, so exact ties fall across blocks and input partitions.
  */
class LogisticAucSuite extends SparkSuite {

  private val providers = 12
  private val perProvider = 150
  private val p = 3

  private val rows: Vector[Row] = (for {
    k <- 0 until providers
    i <- 0 until perProvider
  } yield {
    val id = (k * perProvider + i).toLong
    val rng = new SplittableRandom(0x9e3779b97f4a7c15L * (id + 1L))
    val x = Array.fill(p)(if (rng.nextDouble() < 0.5) 1.0 else 0.0)
    val eta = -0.4 + 0.15 * (k % 5) + 0.8 * x(0) - 0.6 * x(1) + 0.3 * x(2)
    val y = if (rng.nextDouble() < 1.0 / (1.0 + StrictMath.exp(-eta))) 1 else 0
    Row.fromSeq(Seq[Any](id, k.toLong, y) ++ x.toSeq)
  }).toVector

  private val schema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("provider", LongType, nullable = false),
      StructField("y", IntegerType, nullable = false)
    ) ++ (0 until p).map(j => StructField(s"x$j", DoubleType, nullable = false))
  )

  /** Twice the Mann–Whitney numerator over tie groups, as the engine counts it, then the AUC. */
  private def mannWhitney(pi: Array[Double], y: Array[Double]): Double = {
    val order = pi.indices.sortBy(i => pi(i))(Ordering.Double.TotalOrdering)
    var below = 0L
    var positives = 0L
    var negatives = 0L
    var twice = 0L
    var i = 0
    while (i < order.length) {
      var j = i
      var groupPositives = 0L
      var groupNegatives = 0L
      while (j < order.length && pi(order(j)) == pi(order(i))) {
        if (y(order(j)) > 0.5) groupPositives += 1L else groupNegatives += 1L
        j += 1
      }
      twice += groupPositives * (2L * below + groupNegatives)
      below += groupNegatives
      positives += groupPositives
      negatives += groupNegatives
      i = j
    }
    twice.toDouble / (2.0 * positives.toDouble * negatives.toDouble)
  }

  test(
    "the AUC equals the exact Mann-Whitney statistic of the fitted pi, with ties across blocks"
  ) {
    val df = spark.createDataFrame(rows.asJava, schema).repartition(7, col("id"))
    val spec = LogisticSpec("y", (0 until p).map(j => s"x$j"), "provider", rowId = Some("id"))
    val fit =
      LogisticFE.fit(df, spec, LogisticOptions(blocks = BlockOptions(targetBlockBytes = 2048L)))
    assert(fit.layout.blockCount > 3, fit.layout.toString)
    assertEquals(fit.providers.size, providers)
    val auc = fit.auc.getOrElse(fail("a fit with 0/1 outcomes has an AUC"))

    val position = fit.providers.keys.map(GroupKey.value).zipWithIndex.toMap
    val ordered = rows.sortBy(r => (position(r.getLong(1)), r.getLong(0)))
    val starts =
      Array.tabulate(providers)(k => ordered.indexWhere(r => position(r.getLong(1)) == k))
    val y = ordered.map(_.getInt(2).toDouble).toArray
    val x = ordered.flatMap(r => (0 until p).map(j => r.getDouble(3 + j))).toArray
    val pi = Kernel.fitted(
      Kernel.Rows(starts, y, Array.fill(y.length)(1.0), x, p),
      fit.providers.gamma,
      fit.coefficients.map(_.estimate).toArray
    )
    assert(pi.distinct.length * 10 < pi.length, s"${pi.distinct.length} distinct π: too few ties")
    assertEquals(
      java.lang.Double.doubleToRawLongBits(auc),
      java.lang.Double.doubleToRawLongBits(mannWhitney(pi, y)),
      s"engine $auc, reference ${mannWhitney(pi, y)}"
    )
  }
}
