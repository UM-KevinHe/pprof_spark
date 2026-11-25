package pprof.spark.numerics

import java.util.SplittableRandom

import munit.FunSuite
import pprof.spark.numerics.kernels.LogisticFE

/** Profile-likelihood Newton (docs/spec/logistic/profile-newton.md §9, D-83) on blocks held in memory: SerBIN's fixed
  * point at a tight tolerance, pass A's sums at the solved intercepts, the bound for providers without events at the
  * default tolerance (X-051), and fewer passes than SerBIN.
  */
class ProfileNewtonSuite extends FunSuite {

  /** One block of providers with low event rates, so some have no events by chance. */
  private def block(sizes: Seq[Int], p: Int, seed: Long): LogisticFE.Rows = {
    val rng = new SplittableRandom(seed)
    val starts = sizes.scanLeft(0)(_ + _).init.toArray
    val total = sizes.sum
    val x = Array.fill(total * p)(math.rint(rng.nextGaussian() * 64) / 64)
    val beta = Array.tabulate(p)(j => if (j % 2 == 0) 0.4 else -0.3)
    val y = new Array[Double](total)
    var r = 0
    sizes.foreach { size =>
      val g = -3.0 + 0.5 * rng.nextGaussian()
      (0 until size).foreach { _ =>
        var eta = g
        (0 until p).foreach(j => eta += x(r * p + j) * beta(j))
        y(r) = if (rng.nextDouble() < LogisticFE.sigmoid(eta)) 1.0 else 0.0
        r += 1
      }
    }
    LogisticFE.Rows(starts, y, Array.fill(total)(1.0), x, p)
  }

  private val p = 3
  private val blocks: Seq[LogisticFE.Rows] =
    (0 until 3).map(b => block((0 until 20).map(k => 20 + (7 * k + 11 * b) % 41), p, 40L + b))

  private def eventsOf(rows: LogisticFE.Rows): Seq[Double] =
    (0 until rows.providers).map(k => (rows.start(k) until rows.end(k)).map(rows.y).sum)

  private val events = blocks.flatMap(eventsOf)
  private val degenerate = events.indices.filter(k => events(k) == 0.0).toSet
  private val start = {
    val yBar = blocks.map(_.y.sum).sum / blocks.map(_.y.length).sum
    Array.fill(events.size)(StrictMath.log(yBar / (1.0 - yBar)))
  }

  test("the data have providers without events") {
    assert(degenerate.size >= 3, s"only ${degenerate.size} providers without events")
  }

  test(
    "at a tight tolerance the profile fit reaches SerBIN's fixed point, bounded providers included"
  ) {
    val options = SerbinOptions(tol = 1e-12)
    val serbin = Serbin.fit(start, new Array[Double](p), new InMemorySerbinPasses(blocks), options)
    val profile = ProfileNewton
      .fit(start, new Array[Double](p), new InMemoryProfilePasses(blocks), options)
      .result
    assert(serbin.converged && profile.converged)
    profile.beta.indices.foreach { j =>
      val d = math.abs(profile.beta(j) - serbin.beta(j))
      assert(d <= 1e-10 + 1e-8 * math.abs(serbin.beta(j)), s"beta $j differs by $d")
    }
    val median = Serbin.median(profile.gamma)
    profile.gamma.indices.foreach { k =>
      val d = math.abs(profile.gamma(k) - serbin.gamma(k))
      assert(d <= 1e-8, s"gamma $k differs by $d")
      if (degenerate(k))
        assert(
          math.abs(profile.gamma(k) - (median - 10.0)) <= 1e-9,
          s"gamma $k: ${profile.gamma(k)}, bound ${median - 10.0}"
        )
    }
  }

  test("a pass gives pass A's sums at the solved intercepts, and interior scores near zero") {
    val beta = Array(0.35, -0.25, 0.3)
    val bounds = blocks.map(_ => 10.0)
    blocks.zip(bounds).foreach { case (rows, bound) =>
      val warm = Array.fill(rows.providers)(-3.0)
      val part = LogisticFE.profile(rows, warm, beta, -3.0, bound)
      val direct = LogisticFE.schur(rows, part.gamma, beta)
      assertEquals(part.schur.packed.toSeq, direct.packed.toSeq)
      assertEquals(part.schur.rhs.toSeq, direct.rhs.toSeq)
      assertEquals(part.schur.loglik, direct.loglik)
      val terms = LogisticFE.terms(rows, part.gamma, beta)
      eventsOf(rows).indices.foreach { k =>
        if (eventsOf(rows)(k) > 0.0)
          assert(
            math.abs(terms.scoreGamma(k)) <= 1e-10 * terms.infoGamma(k),
            s"provider $k: score ${terms.scoreGamma(k)}, information ${terms.infoGamma(k)}"
          )
      }
    }
  }

  test(
    "at the default tolerance, providers without events sit at the bound (X-051), in fewer passes than SerBIN"
  ) {
    val serbinPasses = new InMemorySerbinPasses(blocks)
    val serbin = Serbin.fit(start, new Array[Double](p), serbinPasses, SerbinOptions())
    val passes = new InMemoryProfilePasses(blocks)
    val profile = ProfileNewton.fit(start, new Array[Double](p), passes, SerbinOptions())
    assert(profile.result.converged)
    val median = Serbin.median(profile.result.gamma)
    degenerate.foreach { k =>
      val g = profile.result.gamma(k)
      assert(math.abs(g - (median - 10.0)) <= 1e-12, s"gamma $k: $g, bound ${median - 10.0}")
    }
    // SerBIN makes a pass A and at least one pass B per iteration.
    assert(
      passes.count < 2 * serbin.iterations,
      s"profile ${passes.count} passes, SerBIN ${serbin.iterations} iterations"
    )
    assertEquals(profile.capped, 0)
  }
}
