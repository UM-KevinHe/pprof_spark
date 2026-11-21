package pprof.spark.numerics.kernels

import java.util.SplittableRandom

import scala.reflect.ClassTag

import munit.FunSuite

/** The Cox kernels read rows through a view (docs/spec/performance/step2-chunks.md §3): records of any size give the
  * same bits as flat rows, with ties, left truncation, zero weights and offsets, under both tie methods.
  */
class CoxRowsSuite extends FunSuite {

  private val p = 3

  /** Strata of the given sizes in canonical order: exit time descending, events first; with each stratum's entry
    * order, entry time descending and ties by row.
    */
  private def block(sizes: Seq[Int], seed: Long): (CoxRows, Array[Int]) = {
    val rng = new SplittableRandom(seed)
    val starts = sizes.scanLeft(0)(_ + _)
    val n = starts.last
    val time = new Array[Double](n)
    val event = new Array[Boolean](n)
    val weight = new Array[Double](n)
    val offset = new Array[Double](n)
    val entry = new Array[Double](n)
    val x = new Array[Double](n * p)
    val entryOrder = new Array[Int](n)
    sizes.indices.foreach { g =>
      val rows = Array
        .fill(sizes(g)) {
          val t = 1.0 + rng.nextInt(6)
          val e = if (rng.nextDouble() < 0.4) t - 0.5 - rng.nextInt(3) else 0.0
          (
            t,
            rng.nextDouble() < 0.6,
            if (rng.nextDouble() < 0.1) 0.0 else 0.5 + rng.nextDouble(),
            0.2 * (rng.nextDouble() - 0.5),
            e,
            Array.fill(p)(rng.nextDouble() * 2.0 - 1.0)
          )
        }
        .sortBy(r => (-r._1, !r._2))
      rows.indices.foreach { i =>
        val r = starts(g) + i
        val (t, d, w, o, e, xs) = rows(i)
        time(r) = t
        event(r) = d
        weight(r) = w
        offset(r) = o
        entry(r) = e
        System.arraycopy(xs, 0, x, r * p, p)
      }
      (starts(g) until starts(g + 1)).sortBy(r => (-entry(r), r)).copyToArray(entryOrder, starts(g))
    }
    (CoxRows(time, entry, entryOrder, event, weight, offset, x, p), starts.toArray)
  }

  private def inRecords(rows: CoxRows, size: Int): ChunkedCoxRows = {
    val n = rows.rows
    val records = (n + size - 1) / size
    def cut[T: ClassTag](a: Array[T], width: Int): Array[Array[T]] =
      Array.tabulate(records)(i => a.slice(i * size * width, math.min(n, (i + 1) * size) * width))
    new ChunkedCoxRows(
      p,
      size,
      cut(rows.time, 1),
      cut(rows.entry, 1),
      cut(rows.entryOrder, 1),
      cut(rows.event, 1),
      cut(rows.weight, 1),
      cut(rows.offset, 1),
      cut(rows.x, p)
    )
  }

  private def bits(values: Iterable[Double]): Vector[Long] =
    values.toVector.map(java.lang.Double.doubleToRawLongBits)

  /** Every Cox kernel's outputs over every stratum, as bit patterns, in a fixed order. */
  private def outputs(rows: CoxRowsView, starts: Array[Int]): Vector[Long] = {
    val beta = Array(0.4, -0.3, 0.2)
    val center = Array(0.05, -0.1, 0.0)
    val maxEta = (0 until rows.rows).map(r => CoxMeasures.eta(rows, r, beta)).max
    val (times, exits, entries, events) = CoxMeasures.timeTotals(rows, 0, rows.rows, beta, maxEta)
    val (eventTimes, cumulative, riskSets) = CoxMeasures.national(times, exits, entries, events)
    val perStratum = starts.indices.init.toVector.flatMap { g =>
      val (from, until) = (starts(g), starts(g + 1))
      Seq(false, true).flatMap { efron =>
        val totals = new CoxTotals(p)
        CoxStratum.add(rows, from, until, beta, center, efron, totals)
        val (bt, bh) = CoxStratum.baseline(rows, from, until, beta, efron)
        val (martingale, score) = CoxResiduals.stratum(rows, from, until, beta, center, efron)
        bits(Seq(totals.value.value) ++ totals.score.values ++ totals.information.values) ++
          bits(bt ++ bh ++ martingale ++ score)
      } ++ {
        val (observed, expected, personTime, direct) =
          CoxMeasures.provider(rows, from, until, beta, maxEta, eventTimes, cumulative, riskSets)
        bits(Seq(observed.toDouble, expected, personTime, direct))
      }
    }
    bits(
      times ++ exits ++ entries ++ events.map(_.toDouble) ++ eventTimes ++ cumulative ++ riskSets
    ) ++
      bits((0 until rows.rows).map(r => CoxMeasures.eta(rows, r, beta))) ++
      bits(Moments.columnSumsRows(rows.rows, p, rows.xs, rows.xBase)) ++ perStratum
  }

  test("every Cox kernel gives the same bits from records of any size as from flat rows") {
    val (flat, starts) = block(Seq(14, 1, 23, 9, 17), seed = 23L)
    val expected = outputs(flat, starts)
    assertEquals(
      bits(Moments.columnSumsRows(flat.rows, p, flat.xs, flat.xBase)),
      bits(Moments.columnSums(flat.x, p, 0, flat.rows))
    )
    Seq(1, 2, 3, 7, 13, flat.rows, flat.rows + 5).foreach { size =>
      assertEquals(outputs(inRecords(flat, size), starts), expected, s"records of $size rows")
    }
  }

  test("Cox records of the wrong sizes are refused") {
    val d = Array(Array(1.0, 1.0), Array(1.0))
    val i = Array(Array(0, 1), Array(2))
    val b = Array(Array(true, false), Array(true))
    val ok = new ChunkedCoxRows(1, 2, d, d, i, b, d, d, d)
    assertEquals(ok.rows, 3)
    assertEquals(ok.entryOrderAt(2), 2)
    intercept[IllegalArgumentException](new ChunkedCoxRows(1, 3, d, d, i, b, d, d, d))
  }
}
