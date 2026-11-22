package pprof.spark.numerics

import java.math.{BigDecimal => Big, MathContext}
import java.util.SplittableRandom

import munit.FunSuite

import pprof.spark.numerics.kernels.{CoxRows, CoxStratum, CoxTotals, LogisticFE, Moments}

/** Chunked compensated sums (docs/spec/performance/step2-chunks.md §5, D-78): their folds and reads, and sums that cross
  * many chunks against exact references, within D-78's 10⁻¹³ scaled.
  */
class ChunkedSumsSuite extends FunSuite {

  private val Context = new MathContext(60)
  private val L = Summation.ChunkLength

  private def exact(values: Iterable[Double]): Double =
    values.foldLeft(Big.ZERO)((s, v) => s.add(new Big(v), Context)).doubleValue

  /** max |a − b| over the largest |b|: D-09's scaled difference. */
  private def scaled(a: Array[Double], b: Array[Double]): Double =
    a.indices.map(i => math.abs(a(i) - b(i))).max / math.max(
      b.map(math.abs).max,
      Double.MinPositiveValue
    )

  test("a chunk folds after ChunkLength terms, reads include the pending chunk, and bits repeat") {
    def run(terms: Int): ChunkedVector = {
      val v = new ChunkedVector(2)
      (0 until terms).foreach { t =>
        v.pending(0) += 0.1 * (t + 1)
        v.pending(1) -= 1.0 / (t + 3)
        v.endTerm()
      }
      v
    }
    val full = run(L)
    assertEquals(full.pending.toSeq, Seq(0.0, 0.0))
    val more = run(L + 1)
    assertEquals(more.pending(0), 0.1 * (L + 1))
    val values = more.values
    assertEquals(more.pending.toSeq, Seq(0.0, 0.0))
    val again = run(L + 1)
    assertEquals(again.readAt(0) - again.pending(0), full.values(0))
    assertEquals(
      again.values.toSeq.map(java.lang.Double.doubleToRawLongBits),
      values.toSeq.map(java.lang.Double.doubleToRawLongBits)
    )
  }

  test("sums over many chunks with heavy cancellation stay within 1e-13 of exact, scaled") {
    val rng = new SplittableRandom(11L)
    val terms = Array.fill(20 * L + 17)(
      Array.fill(3)((rng.nextDouble() - 0.5) * math.pow(10, rng.nextInt(9) - 4))
    )
    val removed = terms.take(19 * L).map(_.map(-_))
    val all = terms ++ removed
    val v = new ChunkedVector(3)
    all.foreach { t =>
      (0 until 3).foreach(i => v.pending(i) += t(i))
      v.endTerm()
    }
    val reference = Array.tabulate(3)(i => exact(all.map(_(i))))
    val scale = Array.tabulate(3)(i => terms.map(t => math.abs(t(i))).max)
    (0 until 3).foreach { i =>
      assert(
        math.abs(v.values(i) - reference(i)) <= 1e-13 * scale(i),
        s"element $i: ${v.values(i)} against ${reference(i)}"
      )
    }
  }

  test("co-moments of 1,000 rows match exact co-moments within 1e-13, scaled") {
    val rng = new SplittableRandom(13L)
    val (n, p) = (1000, 4)
    val x = Array.fill(n * p)(rng.nextGaussian() * 3.0 + 10.0)
    val c = Moments.centred(x, p, 0, n)
    val mean = (0 until p).map(j =>
      (0 until n)
        .foldLeft(Big.ZERO)((s, r) => s.add(new Big(x(r * p + j)), Context))
        .divide(new Big(n), Context)
    )
    val m2 = (for (i <- 0 until p; j <- i until p)
      yield (0 until n)
        .foldLeft(Big.ZERO) { (s, r) =>
          s.add(
            new Big(x(r * p + i))
              .subtract(mean(i), Context)
              .multiply(new Big(x(r * p + j)).subtract(mean(j), Context), Context),
            Context
          )
        }
        .doubleValue).toArray
    assert(scaled(c.comoments, m2) <= 1e-13, s"scaled difference ${scaled(c.comoments, m2)}")
  }

  test(
    "the logistic Schur complement of a 1,200-row block matches an exact sum within 1e-13, scaled"
  ) {
    val rng = new SplittableRandom(17L)
    val (sizes, p) = (Seq(600, 350, 250), 3)
    val n = sizes.sum
    val x = Array.fill(n * p)(rng.nextGaussian())
    val y = Array.fill(n)(if (rng.nextDouble() < 0.4) 1.0 else 0.0)
    val trials = Array.fill(n)(1.0)
    val starts = sizes.scanLeft(0)(_ + _).init.toArray
    val rows = LogisticFE.Rows(starts, y, trials, x, p)
    val gamma = Array(-0.4, 0.2, 0.1)
    val beta = Array(0.3, -0.2, 0.25)
    val schur = LogisticFE.schur(rows, gamma, beta).packed
    val pi = LogisticFE.fitted(rows, gamma, beta)
    val q = pi.map(v => v * (1.0 - v))
    val reference = Array.fill(Moments.packedLength(p))(Big.ZERO)
    starts.indices.foreach { k =>
      val (from, until) = (starts(k), rows.end(k))
      val h = (from until until).foldLeft(Big.ZERO)((s, r) => s.add(new Big(q(r)), Context))
      val xbar = (0 until p).map(j =>
        (from until until)
          .foldLeft(Big.ZERO)((s, r) =>
            s.add(new Big(q(r)).multiply(new Big(x(r * p + j)), Context), Context)
          )
          .divide(h, Context)
      )
      var index = 0
      for (i <- 0 until p; j <- i until p) {
        (from until until).foreach { r =>
          val di = new Big(x(r * p + i)).subtract(xbar(i), Context)
          val dj = new Big(x(r * p + j)).subtract(xbar(j), Context)
          reference(index) =
            reference(index).add(new Big(q(r)).multiply(di, Context).multiply(dj, Context), Context)
        }
        index += 1
      }
    }
    val exactSchur = reference.map(_.doubleValue)
    assert(scaled(schur, exactSchur) <= 1e-13, s"scaled difference ${scaled(schur, exactSchur)}")
  }

  test(
    "Cox information over a truncated stratum of 700 rows matches an exact brute force within 1e-13, scaled"
  ) {
    val rng = new SplittableRandom(19L)
    val (n, p) = (700, 3)
    val raw = Array
      .fill(n) {
        val t = 1.0 + rng.nextInt(400)
        (
          t,
          rng.nextDouble() < 0.7,
          0.5 + rng.nextDouble(),
          if (rng.nextDouble() < 0.5) rng.nextDouble() * (t - 0.5) else 0.0,
          Array.fill(p)(rng.nextGaussian())
        )
      }
      .sortBy(r => (-r._1, !r._2))
    val time = raw.map(_._1)
    val event = raw.map(_._2)
    val weight = raw.map(_._3)
    val entry = raw.map(_._4)
    val offset = Array.fill(n)(0.0)
    val x = raw.flatMap(_._5)
    val entryOrder = (0 until n).sortBy(r => (-entry(r), r)).toArray
    val beta = Array(0.3, -0.2, 0.1)
    val center = Array(0.0, 0.0, 0.0)
    val totals = new CoxTotals(p)
    CoxStratum.add(
      CoxRows(time, entry, entryOrder, event, weight, offset, x, p),
      0,
      n,
      beta,
      center,
      false,
      totals
    )
    val risk = Array.tabulate(n)(r =>
      weight(r) * StrictMath.exp(
        (0 until p).foldLeft(0.0)((s, j) => s + (x(r * p + j) - center(j)) * beta(j)) + offset(r)
      )
    )
    val q = Moments.packedLength(p)
    val information = Array.fill(q)(Big.ZERO)
    time.distinct.filter(t => (0 until n).exists(r => time(r) == t && event(r))).foreach { t =>
      val atRisk = (0 until n).filter(r => entry(r) < t && t <= time(r))
      val dw = (0 until n)
        .filter(r => time(r) == t && event(r))
        .foldLeft(Big.ZERO)((s, r) => s.add(new Big(weight(r)), Context))
      val s0 = atRisk.foldLeft(Big.ZERO)((s, r) => s.add(new Big(risk(r)), Context))
      val s1 = (0 until p).map(j =>
        atRisk.foldLeft(Big.ZERO)((s, r) =>
          s.add(new Big(risk(r)).multiply(new Big(x(r * p + j)), Context), Context)
        )
      )
      var index = 0
      for (i <- 0 until p; j <- i until p) {
        val s2 = atRisk.foldLeft(Big.ZERO)((s, r) =>
          s.add(
            new Big(risk(r))
              .multiply(new Big(x(r * p + i)), Context)
              .multiply(new Big(x(r * p + j)), Context),
            Context
          )
        )
        val term = s2
          .divide(s0, Context)
          .subtract(s1(i).divide(s0, Context).multiply(s1(j).divide(s0, Context), Context), Context)
        information(index) = information(index).add(dw.multiply(term, Context), Context)
        index += 1
      }
    }
    val reference = information.map(_.doubleValue)
    assert(time.distinct.length > 2 * L / 3, s"${time.distinct.length} distinct times")
    assert(
      scaled(totals.information.values, reference) <= 1e-13,
      s"scaled difference ${scaled(totals.information.values, reference)}"
    )
  }
}
