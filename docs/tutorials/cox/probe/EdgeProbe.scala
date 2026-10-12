// Edge-case probe: documentation material for docs/tutorials/cox, not compiled by sbt.
package pprof.spark.engine.cox

import pprof.spark.numerics.{Cholesky, Evaluation, Newton, NewtonOptions, NewtonResult}
import pprof.spark.numerics.kernels.Moments

object EdgeProbe {
  def g(d: Double): String = f"$d%.10g"

  def block(rows: Seq[(Long, Double, Boolean, Double, Array[Double])], p: Int): CoxBlock =
    CoxBlockBuilder.build(0, rows.map { case (id, t, e, o, x) => CoxRow(0, 0, id, t, e, 1.0, o, x) }.iterator, p)

  def evaluator(b: CoxBlock, efron: Boolean): Array[Double] => Evaluation = {
    val c = Moments.columnSums(b.x, b.p, 0, b.rowCount).map(_ / b.rowCount)
    beta => {
      val part = CoxKernel.partial(b, beta, c, efron)
      Evaluation(part.value, part.score, part.information)
    }
  }

  def show(label: String, r: NewtonResult): Unit = {
    println(s"[$label] converged=${r.converged} iterations=${r.iterations} halvings=${r.halvings}")
    r.log.foreach(it => println(s"    it ${it.iteration}: beta ${it.beta.map(g).mkString(", ")}  loglik ${g(it.value)}  halvings ${it.halvings}"))
    val p = r.beta.length
    val f = Cholesky.factor(r.evaluation.information, p)
    if (f.isFullRank) {
      val v = f.inversePacked
      val se = (0 until p).map(j => g(math.sqrt(v(Moments.packedIndex(j, j, p)))))
      println(s"    final beta ${r.beta.map(g).mkString(", ")}; se ${se.mkString(", ")}; score ${r.evaluation.gradient.map(g).mkString(", ")}; pivots ${f.pivots.map(g).mkString(", ")}")
    } else println(s"    information singular at the end: aliased ${f.aliased}")
  }

  def attempt(label: String)(body: => NewtonResult): Unit =
    try show(label, body)
    catch { case e: Throwable => println(s"[$label] ${e.getClass.getSimpleName}: ${e.getMessage}") }

  val tiny: Seq[(Long, Double, Boolean, Double)] = Seq(
    (0L, 1.0, true, 0.5), (1L, 1.0, true, -0.25), (2L, 2.0, false, 1.0), (3L, 2.0, true, 0.0),
    (4L, 3.0, true, -1.0), (5L, 3.0, true, 0.75), (6L, 4.0, false, 0.25), (7L, 4.0, true, -0.5))

  def main(args: Array[String]): Unit = {
    val mono = Seq((0L, 1.0, true, 1.0), (1L, 2.0, false, 0.0), (2L, 3.0, true, 1.0), (3L, 4.0, false, 0.0), (4L, 5.0, true, 1.0), (5L, 6.0, false, 0.0))
    val mb = block(mono.map { case (i, t, e, x) => (i, t, e, 0.0, Array(x)) }, 1)
    attempt("monotone likelihood, defaults")(Newton.maximize(evaluator(mb, false), Array(0.0), NewtonOptions()))
    attempt("monotone likelihood, eps 1e-11, 100 iterations")(Newton.maximize(evaluator(mb, false), Array(0.0), NewtonOptions(maxIterations = 100, eps = 1e-11)))
    for (o <- Seq(0.0, 700.0, 710.0, -700.0, -720.0, -745.0, -800.0)) {
      val ob = block(tiny.map { case (i, t, e, x) => (i, t, e, o, Array(x)) }, 1)
      attempt(s"tiny-ties, every offset = $o")(Newton.maximize(evaluator(ob, false), Array(0.0), NewtonOptions()))
    }
    val x2 = Array(0.01, -0.01, 0.0, 0.01, -0.01, 0.0, 0.01, -0.01)
    for (scale <- Seq(1.0, 1e3, 1e6)) {
      val sb = block(tiny.map { case (i, t, e, x) => (i, t, e, 0.0, Array(x * scale, x2(i.toInt))) }, 2)
      attempt(s"two covariates: x1 times $scale, x2 in [-0.01, 0.01]")(Newton.maximize(evaluator(sb, false), Array(0.0, 0.0), NewtonOptions()))
    }
    val tb = block(tiny.map { case (i, t, e, x) => (i, t, e, 0.0, Array(x)) }, 1)
    for (start <- Seq(4.0, 8.0, -8.0))
      attempt(s"tiny-ties Breslow started at beta = $start (not reachable from CoxPH.fit)")(Newton.maximize(evaluator(tb, false), Array(start), NewtonOptions()))
    attempt("tiny-ties Breslow, maxIterations = 1")(Newton.maximize(evaluator(tb, false), Array(0.0), NewtonOptions(maxIterations = 1)))
  }
}
