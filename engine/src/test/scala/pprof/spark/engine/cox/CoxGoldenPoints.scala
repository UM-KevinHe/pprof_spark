package pprof.spark.engine.cox

import org.apache.spark.sql.DataFrame

import pprof.spark.numerics.Evaluation

/** One Cox evaluation at each of several fixed β on one working set, for the golden runs' fixed points (D-79,
  * docs/spec/performance/step2-chunks.md §6).
  */
object CoxGoldenPoints {
  def evaluate(
      df: DataFrame,
      spec: CoxSpec,
      options: CoxOptions,
      betas: Seq[Array[Double]]
  ): Seq[Evaluation] =
    CoxPH.withWorkingSet(df, spec, options)(prepared =>
      betas.map(b => CoxPH.evaluate(prepared, b, options.ties))
    )
}
