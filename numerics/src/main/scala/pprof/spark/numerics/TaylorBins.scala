package pprof.spark.numerics

/** Direct-standardization sums by binned Taylor expansions of the logistic function (Phase 2d
  * specification §3, X-026): rows are grouped into bins of width 0.1 on η and σ is expanded to order 8
  * around each bin's centre, so Σᵢ wᵢσ(γ + ηᵢ) and Σᵢ wᵢσ′(γ + ηᵢ) need only each bin's moments
  * M_{b,k} = Σᵢ∈b wᵢ(ηᵢ − c_b)ᵏ/k!. The remainder is below 4.2e-17 and 1.2e-16 per unit weight.
  */
object TaylorBins {

  val Width: Double = 0.1
  val Order: Int = 8

  /** σ⁽ᵏ⁾ = Pₖ(σ) for k = 0 to Order + 1, coefficients in ascending powers: P₀(s) = s and
    * Pₖ₊₁(s) = Pₖ′(s)(s − s²).
    */
  val polynomials: Vector[Array[Double]] = Iterator
    .iterate(Array(0.0, 1.0)) { p =>
      val d = Array.tabulate(math.max(p.length - 1, 1))(i =>
        if (i + 1 < p.length) (i + 1) * p(i + 1) else 0.0
      )
      val next = new Array[Double](d.length + 2)
      d.indices.foreach { i =>
        next(i + 1) += d(i)
        next(i + 2) -= d(i)
      }
      next
    }
    .take(Order + 2)
    .toVector

  /** σ⁽ᵏ⁾ at σ = s (Horner). */
  def derivative(k: Int, s: Double): Double = {
    val p = polynomials(k)
    var value = 0.0
    var i = p.length - 1
    while (i >= 0) {
      value = value * s + p(i)
      i -= 1
    }
    value
  }

  /** The bin of η: round(η / width), halves to even. */
  def bin(eta: Double): Long = StrictMath.rint(eta / Width).toLong

  def centre(bin: Long): Double = bin * Width

  /** The moments wᵢ(ηᵢ − c)ᵏ/k!, k = 0 to Order, of one row in bin `b`. */
  def rowMoments(eta: Double, weight: Double, b: Long): Array[Double] = {
    val delta = eta - centre(b)
    val out = new Array[Double](Order + 1)
    var term = weight
    var k = 0
    while (k <= Order) {
      out(k) = term
      term = term * delta / (k + 1)
      k += 1
    }
    out
  }

  /** Σᵢ wᵢσ(γ + ηᵢ) and Σᵢ wᵢσ′(γ + ηᵢ) from the bins (ascending) and their moments. */
  def sums(gamma: Double, bins: Array[Long], moments: Array[Array[Double]]): (Double, Double) = {
    val direct = new NeumaierSum
    val variance = new NeumaierSum
    var b = 0
    while (b < bins.length) {
      val s = 1.0 / (1.0 + StrictMath.exp(-(gamma + centre(bins(b)))))
      var k = 0
      while (k <= Order) {
        direct.add(derivative(k, s) * moments(b)(k))
        variance.add(derivative(k + 1, s) * moments(b)(k))
        k += 1
      }
      b += 1
    }
    (direct.value, variance.value)
  }
}
