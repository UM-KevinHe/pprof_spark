package pprof.spark.numerics.kernels

import java.math.{BigDecimal => Big, MathContext}
import java.util.SplittableRandom

import munit.FunSuite

/** The weighted-Gram information (docs/spec/cox/weighted-gram.md §9, D-80): against round 182's kernel, whose ℓ and U
  * it must match to the bit and whose information within 1e-13 scaled, and against exact references, Breslow and
  * Efron, with ties, late entries on event times, zero weights and offsets.
  */
class CoxGramSuite extends FunSuite {

  private val Context = new MathContext(60)

  /** One stratum in canonical order. `short` enters most rows just before exit in a long stratum, so that each
    * row's hazard is a small difference of large cumulative ones.
    */
  private def stratum(
      n: Int,
      p: Int,
      distinct: Int,
      late: Double,
      seed: Long,
      short: Boolean = false
  ): CoxRows = {
    val rng = new SplittableRandom(seed)
    val raw = Array
      .fill(n) {
        val t = 1.0 + rng.nextInt(distinct)
        val entry =
          if (rng.nextDouble() >= late) 0.0
          else if (short) math.max(0.0, t - 1.0 - rng.nextInt(2))
          else rng.nextInt(t.toInt).toDouble
        (
          t,
          rng.nextDouble() < 0.6,
          if (rng.nextDouble() < 0.05) 0.0 else 0.5 + rng.nextDouble(),
          0.2 * (rng.nextDouble() - 0.5),
          entry,
          Array.fill(p)(rng.nextGaussian())
        )
      }
      .sortBy(r => (-r._1, !r._2))
    val entry = raw.map(_._5)
    CoxRows(
      raw.map(_._1),
      entry,
      (0 until n).sortBy(i => (-entry(i), i)).toArray,
      raw.map(_._2),
      raw.map(_._3),
      raw.map(_._4),
      raw.flatMap(_._6),
      p
    )
  }

  private def bits(values: Array[Double]): Vector[Long] =
    values.toVector.map(java.lang.Double.doubleToRawLongBits)

  private def scaled(a: Array[Double], b: Array[Double]): Double =
    a.indices.map(i => math.abs(a(i) - b(i))).max / math.max(
      b.map(math.abs).max,
      Double.MinPositiveValue
    )

  private def fit(
      rows: CoxRows,
      beta: Array[Double],
      center: Array[Double],
      efron: Boolean,
      s2: Boolean
  ): CoxTotals = {
    val totals = new CoxTotals(rows.p)
    if (s2) CoxStratumS2.add(rows, 0, rows.rows, beta, center, efron, totals)
    else CoxStratum.add(rows, 0, rows.rows, beta, center, efron, totals)
    totals
  }

  /** I(β) from each event time's risk and tied sets in BigDecimal, Efron's steps included (weighted-gram.md §3). The
    * risk scores are the kernel's own doubles.
    */
  private def exact(
      rows: CoxRows,
      beta: Array[Double],
      center: Array[Double],
      efron: Boolean
  ): Array[Double] = {
    val (n, p) = (rows.rows, rows.p)
    val xc = Array.tabulate(n, p)((r, j) => rows.x(r * p + j) - center(j))
    val risk = Array.tabulate(n) { r =>
      var linear = 0.0
      var j = 0
      while (j < p) { linear += xc(r)(j) * beta(j); j += 1 }
      rows.weight(r) * StrictMath.exp(linear + rows.offset(r))
    }
    def big(v: Double) = new Big(v)
    val pairs = for (i <- 0 until p; j <- i until p) yield (i, j)
    val information = Array.fill(pairs.size)(Big.ZERO)
    val times =
      (0 until n).filter(r => rows.event(r) && rows.weight(r) > 0.0).map(rows.time).distinct
    times.foreach { t =>
      val atRisk =
        (0 until n).filter(r => rows.weight(r) > 0.0 && rows.entry(r) < t && t <= rows.time(r))
      val tied = atRisk.filter(r => rows.time(r) == t && rows.event(r))
      val d = tied.size
      def sums(set: Seq[Int]) = (
        set.foldLeft(Big.ZERO)((s, r) => s.add(big(risk(r)), Context)),
        Array.tabulate(p)(j =>
          set.foldLeft(Big.ZERO)((s, r) =>
            s.add(big(risk(r)).multiply(big(xc(r)(j)), Context), Context)
          )
        ),
        pairs.map { case (i, j) =>
          set.foldLeft(Big.ZERO)((s, r) =>
            s.add(
              big(risk(r)).multiply(big(xc(r)(i)), Context).multiply(big(xc(r)(j)), Context),
              Context
            )
          )
        }
      )
      val (s0, s1, s2) = sums(atRisk)
      val (t0, t1, t2) = sums(tied)
      val dw = tied.foldLeft(Big.ZERO)((s, r) => s.add(big(rows.weight(r)), Context))
      val steps =
        if (efron && d >= 2) (1 to d).map(k => new Big(k).divide(new Big(d), Context))
        else Seq(Big.ONE)
      val weight = if (efron && d >= 2) dw.divide(new Big(d), Context) else dw
      steps.foreach { f =>
        val phi = Big.ONE.subtract(f, Context)
        val a = s0.subtract(phi.multiply(t0, Context), Context)
        val m = Array.tabulate(p)(j =>
          s1(j).subtract(phi.multiply(t1(j), Context), Context).divide(a, Context)
        )
        pairs.indices.foreach { k =>
          val (i, j) = pairs(k)
          val term = s2(k)
            .subtract(phi.multiply(t2(k), Context), Context)
            .divide(a, Context)
            .subtract(m(i).multiply(m(j), Context), Context)
          information(k) = information(k).add(weight.multiply(term, Context), Context)
        }
      }
    }
    information.map(_.doubleValue)
  }

  test(
    "the information matches round 182's kernel within 1e-13, scaled, with ℓ and U identical to the bit"
  ) {
    for {
      (n, p, distinct, late, seed) <- Seq(
        (600, 3, 150, 0.0, 1L),
        (600, 3, 150, 0.4, 2L),
        (700, 8, 40, 0.5, 3L),
        (300, 5, 600, 0.3, 4L),
        (900, 4, 12, 0.6, 5L)
      )
      efron <- Seq(false, true)
    } {
      val rows = stratum(n, p, distinct, late, seed)
      val beta = Array.tabulate(p)(j => 0.1 * ((j % 5) - 2))
      val center = Array.tabulate(p)(j => 0.05 * j)
      val gram = fit(rows, beta, center, efron, s2 = false)
      val reference = fit(rows, beta, center, efron, s2 = true)
      val label = s"n $n, p $p, $distinct times, $late late, ${if (efron) "Efron" else "Breslow"}"
      assertEquals(bits(Array(gram.value.value)), bits(Array(reference.value.value)), label)
      assertEquals(bits(gram.score.values), bits(reference.score.values), label)
      val difference = scaled(gram.information.values, reference.information.values)
      assert(difference <= 1e-13, s"$label: scaled difference $difference")
    }
  }

  test(
    "the information matches exact references within 1e-13, scaled, Breslow and Efron, with late entries"
  ) {
    for {
      (n, p, distinct, late, seed, short) <- Seq(
        (400, 3, 40, 0.5, 6L, false),
        (400, 3, 300, 0.8, 7L, true)
      )
      efron <- Seq(false, true)
    } {
      val rows = stratum(n, p, distinct, late, seed, short)
      val beta = Array(0.3, -0.2, 0.1)
      val center = Array(0.05, -0.1, 0.0)
      val information = fit(rows, beta, center, efron, s2 = false).information.values
      val reference = exact(rows, beta, center, efron)
      val difference = scaled(information, reference)
      assert(
        difference <= 1e-13,
        s"$distinct times, short $short, efron $efron: scaled difference $difference"
      )
    }
  }

  test("a stratum without events adds nothing, even when a risk score overflows") {
    val rows = CoxRows(
      Array(3.0, 2.0),
      Array(0.0, 0.0),
      Array(0, 1),
      Array(false, false),
      Array(1.0, 1.0),
      Array(800.0, 0.0),
      Array(1.0, -1.0),
      1
    )
    val totals = fit(rows, Array(0.5), Array(0.0), efron = true, s2 = false)
    assertEquals(totals.information.values.toSeq, Seq(0.0))
    assertEquals(totals.value.value, 0.0)
  }
}
