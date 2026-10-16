package pprof.spark.engine.logistic

import pprof.spark.engine.layout.GroupKey

/** The providers a fit reports (specification §8a): the fitted ones, with any effect replaced by
  * `withProviders`, and the providers added after fitting, in key order. `fitted(v)` is entry v's
  * index among the fitted providers, or -1 for a provider added after fitting; `added(v)` marks the
  * entries `withProviders` set. Passes over the training data use the fitted providers only.
  */
final class LogisticProviderView(
    val keys: Array[GroupKey],
    val gamma: Array[Double],
    val varGamma: Array[Double],
    val varCaseMix: Array[Double],
    val robustVarCaseMix: Array[Double],
    val robustVarFixedBeta: Array[Double],
    val records: Array[Long],
    val fitted: Array[Int],
    val added: Array[Boolean]
) extends Serializable {
  def size: Int = keys.length
}

object LogisticProviderView {

  def of(fit: LogisticFit): LogisticProviderView = {
    val p = fit.providers
    val a = fit.added
    if (a.size == 0)
      new LogisticProviderView(
        p.keys,
        p.gamma,
        p.varGamma,
        p.varCaseMix,
        p.robustVarCaseMix,
        p.robustVarFixedBeta,
        p.records,
        Array.tabulate(p.size)(identity),
        Array.fill(p.size)(false)
      )
    else {
      val fittedIndex = p.keys.zipWithIndex.toMap
      val addedIndex = a.keys.zipWithIndex.toMap
      val keys = (p.keys ++ a.keys.filterNot(fittedIndex.contains)).sorted(GroupKey.ordering)
      val n = keys.length
      val robust = p.robustVarCaseMix.nonEmpty
      val gamma = new Array[Double](n)
      val varGamma = new Array[Double](n)
      val varCaseMix = new Array[Double](n)
      val robustCaseMix = if (robust) new Array[Double](n) else Array.emptyDoubleArray
      val robustFixedBeta = if (robust) new Array[Double](n) else Array.emptyDoubleArray
      val records = new Array[Long](n)
      val fitted = new Array[Int](n)
      val added = new Array[Boolean](n)
      keys.indices.foreach { v =>
        val k = fittedIndex.getOrElse(keys(v), -1)
        fitted(v) = k
        addedIndex.get(keys(v)) match {
          case Some(i) =>
            val variance = a.seGamma(i) * a.seGamma(i)
            gamma(v) = a.gamma(i)
            varGamma(v) = variance
            varCaseMix(v) = variance
            if (robust) {
              robustCaseMix(v) = variance
              robustFixedBeta(v) = variance
            }
            records(v) = if (k >= 0) p.records(k) else a.sizes(i)
            added(v) = true
          case None =>
            gamma(v) = p.gamma(k)
            varGamma(v) = p.varGamma(k)
            varCaseMix(v) = p.varCaseMix(k)
            if (robust) {
              robustCaseMix(v) = p.robustVarCaseMix(k)
              robustFixedBeta(v) = p.robustVarFixedBeta(k)
            }
            records(v) = p.records(k)
        }
      }
      new LogisticProviderView(
        keys,
        gamma,
        varGamma,
        varCaseMix,
        robustCaseMix,
        robustFixedBeta,
        records,
        fitted,
        added
      )
    }
  }
}
