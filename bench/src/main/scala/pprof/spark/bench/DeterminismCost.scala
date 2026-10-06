package pprof.spark.bench

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import pprof.spark.numerics.{NeumaierSum, NeumaierVector, Summation}

/** Spike S-05: what deterministic mode costs (§8.1, §8.2; ADR-0006). Microbenchmarks of
  * `StrictMath` against `java.lang.Math`, of pairwise and Neumaier summation against a plain loop,
  * of a Cox-shaped risk-set kernel in three variants, and of the driver's ordered reduction. Each
  * figure is the median of repeated runs after warm-up, in nanoseconds per element.
  *
  * Usage: sbt "bench/runMain pprof.spark.bench.DeterminismCost"
  * On GitHub Actions the table also goes to the job summary, and the key ratios become notices.
  */
object DeterminismCost {

  private final case class Row(workload: String, variant: String, nanos: Double, baseline: Double)

  @volatile private var sink = 0.0

  def main(args: Array[String]): Unit = {
    val rows = transcendental() ++ summation() ++ kernel(p = 10) ++ kernel(p = 1) ++ reduction()
    val environment =
      Seq("java.version", "java.vendor", "os.arch").map(sys.props(_)).mkString(", ") +
        s", ${Runtime.getRuntime.availableProcessors} processors"
    val table = (Seq(
      s"### Cost of deterministic mode (S-05): $environment",
      "",
      "| Workload | Variant | ns per element | Relative to the first variant |",
      "|---|---|---|---|"
    ) ++ rows.map(r =>
      f"| ${r.workload} | ${r.variant} | ${r.nanos}%.3f | ${r.nanos / r.baseline}%.2f |"
    ))
      .mkString("\n")
    println(table)
    sys.env.get("GITHUB_STEP_SUMMARY").foreach { path =>
      Files.write(
        Paths.get(path),
        (table + "\n").getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND
      )
    }
    if (sys.env.contains("GITHUB_ACTIONS")) {
      val ratios = rows
        .filter(r => r.nanos != r.baseline)
        .map(r => f"${r.workload}, ${r.variant}: ${r.nanos / r.baseline}%.2fx")
      println(s"::notice title=S-05 on $environment::${ratios.mkString("; ")}")
    }
    if (sink == 42.0) println("") // keeps results observable
  }

  /** Median nanoseconds per element over `runs` timed runs, after `warmups` untimed ones. */
  private def measure(elements: Long, warmups: Int = 7, runs: Int = 11)(
      body: () => Double
  ): Double = {
    var acc = 0.0
    (0 until warmups).foreach(_ => acc += body())
    val times = Array.fill(runs) {
      val start = System.nanoTime()
      acc += body()
      (System.nanoTime() - start).toDouble / elements
    }
    sink += acc
    java.util.Arrays.sort(times)
    times(runs / 2)
  }

  /** Deterministic pseudo-random values in [0, 1) without java.util.Random. */
  private def uniform(n: Int, salt: Long): Array[Double] = Array.tabulate(n) { i =>
    ((i.toLong + salt) * 0x9e3779b97f4a7c15L >>> 11).toDouble / (1L << 53).toDouble
  }

  private def transcendental(): Seq[Row] = {
    val n = 1 << 20
    val small = uniform(n, 1).map(u => -5.0 + 10.0 * u)
    val positive = uniform(n, 2).map(u => 0.5 + 99.5 * u)
    val fraction = uniform(n, 3).map(u => -0.5 + u)
    def pair(name: String, xs: Array[Double], fast: Double => Double, strict: Double => Double) = {
      val f = measure(n.toLong)(() => xs.foldLeft(0.0)((s, x) => s + fast(x)))
      val s = measure(n.toLong)(() => xs.foldLeft(0.0)((s, x) => s + strict(x)))
      Seq(Row(name, "java.lang.Math", f, f), Row(name, "StrictMath", s, f))
    }
    pair("exp", small, Math.exp, StrictMath.exp) ++
      pair("log", positive, Math.log, StrictMath.log) ++
      pair("log1p", fraction, Math.log1p, StrictMath.log1p) ++
      pair("expm1", fraction, Math.expm1, StrictMath.expm1)
  }

  private def summation(): Seq[Row] = {
    val n = 1 << 20
    val xs = uniform(n, 4).zipWithIndex.map { case (u, i) => Math.scalb(u - 0.5, i % 61 - 30) }
    val plain = measure(n.toLong) { () =>
      var s = 0.0
      var i = 0
      while (i < n) {
        s += xs(i)
        i += 1
      }
      s
    }
    val pairwise = measure(n.toLong)(() => Summation.pairwise(xs))
    val neumaier = measure(n.toLong) { () =>
      val s = new NeumaierSum
      var i = 0
      while (i < n) {
        s.add(xs(i))
        i += 1
      }
      s.value
    }
    Seq(
      Row("sum of 2^20 values", "plain loop", plain, plain),
      Row("sum of 2^20 values", "pairwise (within blocks)", pairwise, plain),
      Row("sum of 2^20 values", "Neumaier (across blocks)", neumaier, plain)
    )
  }

  /** One pass of a Cox-shaped kernel over `m` rows of `p` covariates: `w = exp(x . beta)`, then the
    * sums of `w`, `w x` and `w x x'` (packed). Variants: java.lang.Math with sequential sums (fast,
    * not reproducible across JVMs); StrictMath with sequential sums (reproducible, R2); StrictMath
    * with `Summation.pairwise` over one scratch column per statistic; and StrictMath with a fused
    * cascade that walks the rows once yet builds the same addition tree per statistic, so its bits
    * equal the scratch-column variant's (checked before timing).
    */
  private def kernel(p: Int): Seq[Row] = {
    val m = 1 << 16
    val x = uniform(m * p, 5L + p).map(u => 2.0 * u - 1.0)
    val beta = Array.tabulate(p)(j => 0.3 / (j + 1))
    val statistics = 1 + p + p * (p + 1) / 2
    val eta = new Array[Double](m)
    val w = new Array[Double](m)
    val scratch = new Array[Double](m)

    def linear(): Unit = {
      var r = 0
      while (r < m) {
        var acc = 0.0
        var j = 0
        while (j < p) {
          acc += x(r * p + j) * beta(j)
          j += 1
        }
        eta(r) = acc
        r += 1
      }
    }

    def sequential(strict: Boolean): Double = {
      linear()
      val sums = new Array[Double](statistics)
      var r = 0
      while (r < m) {
        val weight = if (strict) StrictMath.exp(eta(r)) else Math.exp(eta(r))
        sums(0) += weight
        var k = 1 + p
        var i = 0
        while (i < p) {
          val wx = weight * x(r * p + i)
          sums(1 + i) += wx
          var j = i
          while (j < p) {
            sums(k) += wx * x(r * p + j)
            k += 1
            j += 1
          }
          i += 1
        }
        r += 1
      }
      sums.sum
    }

    def weights(): Unit = {
      linear()
      var r = 0
      while (r < m) {
        w(r) = StrictMath.exp(eta(r))
        r += 1
      }
    }

    def scratchColumns(): Array[Double] = {
      weights()
      val sums = new Array[Double](statistics)
      sums(0) = Summation.pairwise(w)
      var k = 1 + p
      var i = 0
      while (i < p) {
        var r = 0
        while (r < m) {
          scratch(r) = w(r) * x(r * p + i)
          r += 1
        }
        sums(1 + i) = Summation.pairwise(scratch)
        var j = i
        while (j < p) {
          r = 0
          while (r < m) {
            scratch(r) = w(r) * x(r * p + i) * x(r * p + j)
            r += 1
          }
          sums(k) = Summation.pairwise(scratch)
          k += 1
          j += 1
        }
        i += 1
      }
      sums
    }

    /** Row `r`'s statistics, in the order of `scratchColumns`, written into or added to `out`. */
    def rowStatistics(r: Int, out: Array[Double], add: Boolean): Unit = {
      def put(k: Int, v: Double): Unit = if (add) out(k) += v else out(k) = v
      put(0, w(r))
      var k = 1 + p
      var i = 0
      while (i < p) {
        val wx = w(r) * x(r * p + i)
        put(1 + i, wx)
        var j = i
        while (j < p) {
          put(k, wx * x(r * p + j))
          k += 1
          j += 1
        }
        i += 1
      }
    }

    /** The addition tree of `Summation.pairwise`, for every statistic in one walk over rows. */
    def cascade(from: Int, until: Int, out: Array[Double]): Unit = {
      val n = until - from
      if (n <= Summation.PairwiseLeafSize) {
        rowStatistics(from, out, add = false)
        var r = from + 1
        while (r < until) {
          rowStatistics(r, out, add = true)
          r += 1
        }
      } else {
        val mid = from + n / 2
        val right = new Array[Double](statistics)
        cascade(from, mid, out)
        cascade(mid, until, right)
        var k = 0
        while (k < statistics) {
          out(k) += right(k)
          k += 1
        }
      }
    }

    def fused(): Array[Double] = {
      weights()
      val sums = new Array[Double](statistics)
      cascade(0, m, sums)
      sums
    }

    val reference = scratchColumns().map(java.lang.Double.doubleToRawLongBits)
    require(
      reference.sameElements(fused().map(java.lang.Double.doubleToRawLongBits)),
      "the fused cascade must reproduce the scratch-column bits"
    )
    val name = s"Cox-shaped kernel, p = $p (per row)"
    val fast = measure(m.toLong)(() => sequential(strict = false))
    val strict = measure(m.toLong)(() => sequential(strict = true))
    val columns = measure(m.toLong)(() => scratchColumns().sum)
    val cascaded = measure(m.toLong)(() => fused().sum)
    Seq(
      Row(name, "Math.exp, sequential sums", fast, fast),
      Row(name, "StrictMath.exp, sequential sums", strict, fast),
      Row(name, "StrictMath.exp, pairwise, one scratch column per statistic", columns, fast),
      Row(name, "StrictMath.exp, pairwise, fused cascade (same bits)", cascaded, fast)
    )
  }

  /** The driver's ordered reduction: 10,000 partials of 66 values (p = 10, packed). */
  private def reduction(): Seq[Row] = {
    val blocks = 10000
    val length = 66
    val partials = Array.tabulate(blocks)(b => uniform(length, 100L + b))
    val elements = blocks.toLong * length
    val plain = measure(elements) { () =>
      val acc = new Array[Double](length)
      partials.foreach { v =>
        var i = 0
        while (i < length) {
          acc(i) += v(i)
          i += 1
        }
      }
      acc.sum
    }
    val neumaier = measure(elements) { () =>
      val acc = new NeumaierVector(length)
      partials.foreach(acc.add)
      acc.values.sum
    }
    Seq(
      Row("reduce 10,000 partials of 66", "plain sums", plain, plain),
      Row("reduce 10,000 partials of 66", "Neumaier, block order", neumaier, plain)
    )
  }
}
