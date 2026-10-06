package pprof.spark.numerics

import munit.FunSuite

class CholeskySuite extends FunSuite {

  private def dense(packed: Array[Double], p: Int): Array[Double] = {
    val a = new Array[Double](p * p)
    var k = 0
    for (i <- 0 until p; j <- i until p) {
      a(i * p + j) = packed(k)
      a(j * p + i) = packed(k)
      k += 1
    }
    a
  }

  private val a = Array(4.0, 2.0, 0.6, 2.0, 0.5, 3.0)

  test("solve and inverse reproduce the system and the identity") {
    val f = Cholesky.factor(a, 3)
    assert(f.isFullRank)
    val m = dense(a, 3)
    val b = Array(1.0, -2.0, 0.5)
    val x = f.solve(b)
    for (i <- 0 until 3) {
      val r = (0 until 3).map(k => m(i * 3 + k) * x(k)).sum - b(i)
      assert(math.abs(r) <= 1e-14, s"residual $r in row $i")
    }
    val inv = dense(f.inversePacked, 3)
    for (i <- 0 until 3; j <- 0 until 3) {
      val e = (0 until 3).map(k => m(i * 3 + k) * inv(k * 3 + j)).sum - (if (i == j) 1.0 else 0.0)
      assert(
        math.abs(e) <= 1e-14,
        s"A times its inverse differs from the identity by $e at ($i, $j)"
      )
    }
  }

  test("the default tolerance is R's toler.chol, exactly 2^-39") {
    assertEquals(Cholesky.DefaultTolerance, StrictMath.pow(2.0, -39.0))
  }

  test("collinear and constant columns are reported as aliased") {
    val rows = Seq(Array(1.0, 2.0), Array(2.0, -1.0), Array(3.0, 0.5), Array(-1.0, 1.5))
    val x = rows.map(r => Array(r(0), r(1), 2.0 * r(0)))
    val info = for (i <- 0 until 3; j <- i until 3) yield x.map(r => r(i) * r(j)).sum
    assertEquals(Cholesky.factor(info.toArray, 3).aliased, Vector(2))
    assertEquals(Cholesky.factor(Array(1.0, 0.0, 0.0), 2).aliased, Vector(1))
    assertEquals(Cholesky.factor(Array(0.0), 1).aliased, Vector(0))
    intercept[IllegalStateException](
      Cholesky.factor(Array(1.0, 0.0, 0.0), 2).solve(Array(1.0, 1.0))
    )
  }

  test("a pivot below the tolerance is aliased and one above it is not") {
    def pivotMatrix(delta: Double) = Array(1.0, 1.0, 1.0 + delta)
    assertEquals(Cholesky.factor(pivotMatrix(StrictMath.pow(2.0, -40.0)), 2).aliased, Vector(1))
    assert(Cholesky.factor(pivotMatrix(StrictMath.pow(2.0, -38.0)), 2).isFullRank)
  }
}
