package pprof.spark.numerics.kernels

import java.util.SplittableRandom

import munit.FunSuite
import pprof.spark.numerics.{ChunkedVector, NeumaierVector}

/** Per-time aggregates against the sweep (docs/spec/cox/per-time-aggregates.md §8, D-81): a stratum split into parts,
  * as blocks split it, gives the sweep's ℓ, U and I within 1e-12, scaled, under Breslow and Efron, with ties, late
  * entries on event times, zero weights and offsets.
  */
class CoxAggregatesSuite extends FunSuite {
  import CoxAggregatesSuite.Row

  private def draw(n: Int, p: Int, distinct: Int, late: Double, seed: Long): Vector[Row] = {
    val rng = new SplittableRandom(seed)
    Vector
      .fill(n) {
        val t = 1.0 + rng.nextInt(distinct)
        val entry = if (rng.nextDouble() < late) rng.nextInt(t.toInt).toDouble else 0.0
        Row(
          t,
          entry,
          rng.nextDouble() < 0.6,
          if (rng.nextDouble() < 0.05) 0.0 else 0.5 + rng.nextDouble(),
          0.2 * (rng.nextDouble() - 0.5),
          Array.fill(p)(rng.nextGaussian())
        )
      }
      .sortBy(r => (-r.time, !r.event))
  }

  /** Rows in canonical order, as a block holds them. */
  private def rowsOf(rows: Vector[Row], p: Int): CoxRows = {
    val entry = rows.map(_.entry).toArray
    CoxRows(
      rows.map(_.time).toArray,
      entry,
      rows.indices.sortBy(i => (-entry(i), i)).toArray,
      rows.map(_.event).toArray,
      rows.map(_.weight).toArray,
      rows.map(_.offset).toArray,
      rows.flatMap(_.x).toArray,
      p
    )
  }

  private def scaled(a: Array[Double], b: Array[Double]): Double =
    a.indices.map(i => math.abs(a(i) - b(i))).max / math.max(
      b.map(math.abs).max,
      Double.MinPositiveValue
    )

  /** ℓ, U and I from parts: totals merged in part order, the driver's step, then each part's Gram merged in order. */
  private def aggregated(
      parts: Seq[CoxRows],
      grid: Array[Double],
      p: Int,
      beta: Array[Double],
      center: Array[Double],
      efron: Boolean
  ): (Double, Array[Double], Array[Double]) = {
    val totals = new CoxTimeTotals(grid, p, efron)
    parts.foreach { part =>
      val local = new CoxTimeTotals(grid, p, efron)
      local.add(part, 0, part.rows, beta, center)
      totals.merge(local)
    }
    val evaluation = CoxAggregates.evaluate(totals)
    val q = Moments.packedLength(p)
    val g = new NeumaierVector(q)
    parts.foreach { part =>
      val local = new ChunkedVector(q)
      CoxAggregates.gram(part, 0, part.rows, beta, center, evaluation, local)
      g.add(local.values)
    }
    val gv = g.values
    (evaluation.value, evaluation.score, Array.tabulate(q)(k => gv(k) - evaluation.subtractive(k)))
  }

  test("a stratum split into parts gives the sweep's ℓ, U and I within 1e-12, Breslow and Efron") {
    for {
      (n, p, distinct, late, seed, parts) <- Seq(
        (600, 3, 40, 0.0, 11L, 3),
        (600, 3, 40, 0.5, 12L, 3),
        (900, 6, 12, 0.6, 13L, 7),
        (400, 4, 300, 0.4, 14L, 1)
      )
      efron <- Seq(false, true)
    } {
      val rows = draw(n, p, distinct, late, seed)
      val grid = (rows.map(_.time) ++ rows.map(_.entry)).distinct.sorted.toArray
      val beta = Array.tabulate(p)(j => 0.15 * ((j % 5) - 2))
      val center = Array.tabulate(p)(j => 0.05 * j)
      val whole = new CoxTotals(p)
      CoxStratum.add(rowsOf(rows, p), 0, n, beta, center, efron, whole)
      val split = (0 until parts).map(part =>
        rowsOf(rows.zipWithIndex.collect { case (r, i) if i % parts == part => r }, p)
      )
      val (value, score, information) = aggregated(split, grid, p, beta, center, efron)
      val label =
        s"n $n, p $p, $distinct times, $late late, $parts parts, ${if (efron) "Efron" else "Breslow"}"
      val dv = math.abs(value - whole.value.value) / math.abs(whole.value.value)
      assert(dv <= 1e-12, s"$label: ℓ scaled difference $dv")
      val du = scaled(score, whole.score.values)
      assert(du <= 1e-12, s"$label: U scaled difference $du")
      val di = scaled(information, whole.information.values)
      assert(di <= 1e-12, s"$label: I scaled difference $di")
    }
  }

  private def totalsOf(
      parts: Seq[CoxRows],
      grid: Array[Double],
      p: Int,
      beta: Array[Double],
      center: Array[Double],
      efron: Boolean
  ): CoxTimeTotals = {
    val totals = new CoxTimeTotals(grid, p, efron)
    parts.foreach { part =>
      val local = new CoxTimeTotals(grid, p, efron)
      local.add(part, 0, part.rows, beta, center)
      totals.merge(local)
    }
    totals
  }

  test("the baseline from parts matches the sweep's within 1e-12, Breslow and Efron (§6)") {
    for {
      (n, p, distinct, late, seed, k) <- Seq((600, 3, 40, 0.5, 21L, 3), (900, 4, 12, 0.6, 22L, 7))
      efron <- Seq(false, true)
    } {
      val rows = draw(n, p, distinct, late, seed)
      val grid = (rows.map(_.time) ++ rows.map(_.entry)).distinct.sorted.toArray
      val beta = Array.tabulate(p)(j => 0.15 * ((j % 5) - 2))
      val (wholeTimes, wholeIncrements) = CoxStratum.baseline(rowsOf(rows, p), 0, n, beta, efron)
      val parts = (0 until k).map(part =>
        rowsOf(rows.zipWithIndex.collect { case (r, i) if i % k == part => r }, p)
      )
      val (times, increments) =
        CoxAggregates.baseline(totalsOf(parts, grid, p, beta, new Array[Double](p), efron))
      assertEquals(times.toSeq, wholeTimes.toSeq)
      val d = scaled(increments, wholeIncrements)
      assert(d <= 1e-12, s"n $n, efron $efron: increments scaled difference $d")
    }
  }

  test("residuals from parts match the sweep's within 1e-12, Breslow and Efron (§6)") {
    for {
      (n, p, distinct, late, seed, k) <- Seq(
        (600, 3, 40, 0.5, 23L, 3),
        (900, 4, 12, 0.6, 24L, 7),
        (400, 3, 300, 0.4, 25L, 1)
      )
      efron <- Seq(false, true)
    } {
      val rows = draw(n, p, distinct, late, seed)
      val grid = (rows.map(_.time) ++ rows.map(_.entry)).distinct.sorted.toArray
      val beta = Array.tabulate(p)(j => 0.15 * ((j % 5) - 2))
      val center = Array.tabulate(p)(j => 0.05 * j)
      val (wholeMartingale, wholeScore) =
        CoxResiduals.stratum(rowsOf(rows, p), 0, n, beta, center, efron)
      val parts = (0 until k).map(part =>
        rows.zipWithIndex.collect { case (r, i) if i % k == part => (r, i) }
      )
      val partRows = parts.map(part => rowsOf(part.map(_._1), p))
      val tables = CoxAggregates.residualTables(totalsOf(partRows, grid, p, beta, center, efron))
      val martingale = new Array[Double](n)
      val score = new Array[Double](n * p)
      parts.zip(partRows).foreach { case (part, rowsOfPart) =>
        val (m, u) =
          CoxAggregates.residuals(rowsOfPart, 0, rowsOfPart.rows, beta, center, efron, tables)
        part.map(_._2).zipWithIndex.foreach { case (row, at) =>
          martingale(row) = m(at)
          (0 until p).foreach(j => score(row * p + j) = u(at * p + j))
        }
      }
      val label = s"n $n, $k parts, efron $efron"
      val dm = scaled(martingale, wholeMartingale)
      assert(dm <= 1e-12, s"$label: martingale scaled difference $dm")
      val du = scaled(score, wholeScore)
      assert(du <= 1e-12, s"$label: score scaled difference $du")
    }
  }

  test("totals merged in the same order give the same bits") {
    val rows = draw(500, 3, 30, 0.4, 15L)
    val grid = (rows.map(_.time) ++ rows.map(_.entry)).distinct.sorted.toArray
    val split = (0 until 4).map(part =>
      rowsOf(rows.zipWithIndex.collect { case (r, i) if i % 4 == part => r }, 3)
    )
    val beta = Array(0.2, -0.1, 0.05)
    val center = Array(0.0, 0.1, -0.1)
    val first = aggregated(split, grid, 3, beta, center, efron = true)
    val second = aggregated(split, grid, 3, beta, center, efron = true)
    def bits(a: Array[Double]) = a.toVector.map(java.lang.Double.doubleToRawLongBits)
    assertEquals(
      java.lang.Double.doubleToRawLongBits(first._1),
      java.lang.Double.doubleToRawLongBits(second._1)
    )
    assertEquals(bits(first._2), bits(second._2))
    assertEquals(bits(first._3), bits(second._3))
  }

  test("a stratum without events adds nothing, and a time off the grid is refused") {
    val rows = rowsOf(
      Vector(
        Row(3.0, 0.0, false, 1.0, 800.0, Array(1.0)),
        Row(2.0, 0.0, false, 1.0, 0.0, Array(-1.0))
      ),
      1
    )
    val (value, score, information) =
      aggregated(Seq(rows), Array(0.0, 2.0, 3.0), 1, Array(0.5), Array(0.0), efron = true)
    assertEquals((value, score.toSeq, information.toSeq), (0.0, Seq(0.0), Seq(0.0)))
    val totals = new CoxTimeTotals(Array(0.0, 3.0), 1, efron = false)
    intercept[IllegalArgumentException](totals.add(rows, 0, rows.rows, Array(0.5), Array(0.0)))
  }
}

object CoxAggregatesSuite {
  final case class Row(
      time: Double,
      entry: Double,
      event: Boolean,
      weight: Double,
      offset: Double,
      x: Array[Double]
  )
}
