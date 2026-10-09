package pprof.spark.numerics

/** Count distributions of stage 3's provider tests in the three-stage model (Phase 2f-4 specification §2):
  * Gauss–Hermite mixtures over posterior cluster effects of Poisson-binomial distributions.
  */
object ClusteredPoissonBinomial {

  /** pprof_py's `_POSTERIOR_NODES`. */
  val PosteriorNodes: Int = 32

  private def sigmoid(x: Double): Double = 1.0 / (1.0 + StrictMath.exp(-x))

  /** Standard normal nodes √2·tₖ and weights wₖ/√π. */
  def standardNormalRule(n: Int): (Array[Double], Array[Double]) = {
    val rule = GaussHermite.rule(n)
    val root2 = StrictMath.sqrt(2.0)
    val rootPi = StrictMath.sqrt(StrictMath.PI)
    (rule.nodes.map(_ * root2), rule.weights.map(_ / rootPi))
  }

  /** The full Poisson-binomial distribution of the records' outcomes (exact recursion). */
  def poibinPmf(p: Array[Double]): Array[Double] = {
    val pmf = new Array[Double](p.length + 1)
    pmf(0) = 1.0
    var i = 0
    while (i < p.length) {
      var k = i + 1
      while (k >= 1) {
        pmf(k) = pmf(k) * (1.0 - p(i)) + pmf(k - 1) * p(i)
        k -= 1
      }
      pmf(0) *= 1.0 - p(i)
      i += 1
    }
    pmf
  }

  private def convolve(a: Array[Double], b: Array[Double]): Array[Double] = {
    val out = new Array[Double](a.length + b.length - 1)
    var i = 0
    while (i < a.length) {
      var j = 0
      while (j < b.length) {
        out(i + j) += a(i) * b(j)
        j += 1
      }
      i += 1
    }
    out
  }

  /** The `exact` null (pprof_py's `_clustered_pmf`): each cluster's effect drawn once from N(meanₕ, varₕ) and
    * shared by its records; per cluster a mixture over `nodes` standard normal nodes, convolved across the
    * clusters in index order. `eta` excludes the cluster effect; `cluster` indexes `mean` and `variance`.
    */
  def clusteredPmf(
      eta: Array[Double],
      cluster: Array[Int],
      mean: Array[Double],
      variance: Array[Double],
      nodes: Int = PosteriorNodes
  ): Array[Double] = {
    val (z, w) = standardNormalRule(nodes)
    var pmf = Array(1.0)
    cluster.distinct.sorted.foreach { h =>
      val rows = eta.indices.filter(i => cluster(i) == h)
      val sd = StrictMath.sqrt(math.max(variance(h), 0.0))
      val mixture = new Array[Double](rows.length + 1)
      z.indices.foreach { k =>
        val part = poibinPmf(rows.map(i => sigmoid(eta(i) + mean(h) + sd * z(k))).toArray)
        part.indices.foreach(j => mixture(j) += w(k) * part(j))
      }
      pmf = convolve(pmf, mixture)
    }
    pmf
  }

  /** Tails at the observed count by summing the distribution's entries (pprof_py's `_pmf_tails`). */
  def tails(pmf: Array[Double], observed: Int): PoissonBinomial.Tails = {
    val eq = if (observed >= 0 && observed < pmf.length) pmf(observed) else 0.0
    val gt = new NeumaierSum
    val lt = new NeumaierSum
    pmf.indices.foreach(k => if (k > observed) gt.add(pmf(k)) else if (k < observed) lt.add(pmf(k)))
    PoissonBinomial.Tails(
      gt.value + 0.5 * eq,
      lt.value + 0.5 * eq,
      gt.value + eq,
      lt.value + eq,
      gt.value,
      lt.value
    )
  }

  /** Each record's probability with its own effect N(0, varᵢ) integrated out (pprof_py's `RowMixture`). */
  def integratedProbabilities(
      eta: Array[Double],
      variance: Array[Double],
      nodes: Int = PosteriorNodes
  ): Array[Double] = {
    val (z, w) = standardNormalRule(nodes)
    eta.indices.map { i =>
      val sd = StrictMath.sqrt(math.max(variance(i), 0.0))
      z.indices.foldLeft(0.0)((acc, k) => acc + w(k) * sigmoid(eta(i) + sd * z(k)))
    }.toArray
  }

  /** Simulated tails (pprof_py's `resample_tails`): every record draws its own effect from N(meanᵢ, varᵢ)
    * and then its outcome, from counter-based uniforms keyed by (seed, provider, replicate, record) (X-025).
    */
  def resampledTails(
      observed: Int,
      eta: Array[Double],
      mean: Array[Double],
      variance: Array[Double],
      replicates: Int,
      seed: Long,
      provider: Long
  ): PoissonBinomial.Tails = {
    var ge = 0L
    var gt = 0L
    var le = 0L
    var lt = 0L
    var r = 0
    while (r < replicates) {
      var count = 0
      var i = 0
      while (i < eta.length) {
        val u1 = math.max(PoissonBinomial.uniform(seed, provider, r, 3L * i), 1e-300)
        val u2 = PoissonBinomial.uniform(seed, provider, r, 3L * i + 1L)
        val normal =
          StrictMath.sqrt(-2.0 * StrictMath.log(u1)) * StrictMath.cos(2.0 * StrictMath.PI * u2)
        val a = mean(i) + StrictMath.sqrt(math.max(variance(i), 0.0)) * normal
        if (PoissonBinomial.uniform(seed, provider, r, 3L * i + 2L) < sigmoid(eta(i) + a))
          count += 1
        i += 1
      }
      if (count >= observed) ge += 1L
      if (count > observed) gt += 1L
      if (count <= observed) le += 1L
      if (count < observed) lt += 1L
      r += 1
    }
    val n = replicates.toDouble
    PoissonBinomial.Tails((ge + gt) / 2.0 / n, (le + lt) / 2.0 / n, ge / n, le / n, gt / n, lt / n)
  }
}
