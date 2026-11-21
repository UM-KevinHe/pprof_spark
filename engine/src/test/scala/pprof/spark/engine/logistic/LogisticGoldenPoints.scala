package pprof.spark.engine.logistic

import org.apache.spark.sql.DataFrame

/** The logistic golden runs' fixed points (D-79, docs/spec/performance/step2-chunks.md §6), as named vectors: the
  * merged co-moments, then pass A and the variance pass at SerBIN's start and at `gamma` and `beta`.
  */
object LogisticGoldenPoints {
  def values(
      df: DataFrame,
      spec: LogisticSpec,
      options: LogisticOptions,
      gamma: Array[Double],
      beta: Array[Double]
  ): Map[String, Vector[Double]] = {
    val (comoments, start, estimates) = LogisticFE.fixedPoints(df, spec, options, gamma, beta)
    def named(prefix: String, f: LogisticFE.FixedPoint): Map[String, Vector[Double]] =
      Map(
        s"${prefix}_schur" -> f.schur.toVector,
        s"${prefix}_rhs" -> f.rhs.toVector,
        s"${prefix}_loglik" -> Vector(f.loglik),
        s"${prefix}_score_term" -> Vector(f.scoreTerm),
        s"${prefix}_variance_schur" -> f.varianceSchur.toVector,
        s"${prefix}_variance_loglik" -> Vector(f.varianceLoglik)
      )
    Map("comoments" -> comoments.toVector) ++ named("at_start", start) ++ named(
      "at_estimates",
      estimates
    )
  }
}
