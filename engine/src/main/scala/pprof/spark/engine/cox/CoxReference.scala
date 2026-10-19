package pprof.spark.engine.cox

import org.apache.spark.sql.{Dataset, Encoders}

import pprof.spark.engine.backend.OrderedReduction
import pprof.spark.numerics.NeumaierSum

/** The point a baseline hazard refers to (Phase 1b specification §2a, issue C1, D-49). */
sealed abstract class BaselineReference(val name: String) extends Product with Serializable

object BaselineReference {

  /** x = 0 and offset 0: pprof_py's raw baseline, which every pprof_spark prediction uses (X-014). */
  case object Raw extends BaselineReference("raw")

  /** x = 0 at the weighted mean offset: pprof_py's `baseline_hazard_` and R's
    * `basehaz(fit, centered = FALSE)`.
    */
  case object MeanOffset extends BaselineReference("mean_offset")

  /** R's covariate means at the weighted mean offset: R's `survfit(fit)` default curve and
    * `basehaz(fit)`.
    */
  case object Means extends BaselineReference("means")

  val all: Seq[BaselineReference] = Seq(Raw, MeanOffset, Means)

  def fromName(name: String): BaselineReference =
    all
      .find(_.name == name)
      .getOrElse(
        throw new IllegalArgumentException(
          s"reference must be ${all.map(_.name).mkString(", ")}, got $name"
        )
      )
}

/** The training data's weighted mean offset and R's covariate means (Phase 1b specification §2a). A
  * covariate whose values all lie in {-1, 0, 1} is not centred and has mean 0 (R's `nocenter`); any
  * other has its case-weighted mean for right-censored data (R's `coxph.fit`) and its unweighted
  * mean for data with entry times (R's `agreg.fit`). R's linear predictor is pprof_spark's minus the
  * sum of each coefficient times its mean.
  */
final case class CoxReferencePoint(
    offsetMean: Double,
    means: Vector[Double],
    centered: Vector[Boolean]
) {

  /** exp of the reference subject's linear predictor: the raw baseline times this is `reference`'s. */
  def factor(reference: BaselineReference, beta: Seq[Double]): Double = reference match {
    case BaselineReference.Raw        => 1.0
    case BaselineReference.MeanOffset => StrictMath.exp(offsetMean)
    case BaselineReference.Means      =>
      var eta = offsetMean
      var k = 0
      while (k < means.length) {
        eta += beta(k) * means(k)
        k += 1
      }
      StrictMath.exp(eta)
  }
}

/** One block's sums for the reference point, in canonical row order. */
final case class CoxReferencePartial(blockId: Int, values: Array[Double])

object CoxReference {

  /** Rows, Σw, Σw·o, Σw·xₖ, Σxₖ (compensated, in row order) and the rows with xₖ outside {-1, 0, 1}. */
  private[cox] def partial(block: CoxBlock): CoxReferencePartial = {
    val p = block.p
    val sums = Array.fill(2 + 2 * p)(new NeumaierSum)
    val outside = new Array[Long](p)
    var r = 0
    while (r < block.rowCount) {
      val w = block.weight(r)
      sums(0).add(w)
      sums(1).add(w * block.offset(r))
      var k = 0
      while (k < p) {
        val v = block.x(r * p + k)
        sums(2 + k).add(w * v)
        sums(2 + p + k).add(v)
        if (v != -1.0 && v != 0.0 && v != 1.0) outside(k) += 1L
        k += 1
      }
      r += 1
    }
    CoxReferencePartial(
      block.blockId,
      Array(block.rowCount.toDouble) ++ sums.map(_.value) ++ outside.map(_.toDouble)
    )
  }

  /** The reference point of a working set, its block sums reduced in block order (DIST-6). */
  def point(blocks: Dataset[CoxBlock], p: Int, entryTimes: Boolean): CoxReferencePoint = {
    val parts =
      blocks.map((b: CoxBlock) => partial(b))(Encoders.product[CoxReferencePartial]).collect()
    val s = OrderedReduction.reduce(parts.toSeq.map(q => q.blockId -> q.values), 3 + 3 * p)
    val rows = s(0)
    val weight = s(1)
    val centered = Vector.tabulate(p)(k => s(3 + 2 * p + k) > 0.0)
    val means = Vector.tabulate(p) { k =>
      if (!centered(k)) 0.0 else if (entryTimes) s(3 + p + k) / rows else s(3 + k) / weight
    }
    CoxReferencePoint(s(2) / weight, means, centered)
  }
}
