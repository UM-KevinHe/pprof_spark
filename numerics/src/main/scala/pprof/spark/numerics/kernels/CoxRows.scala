package pprof.spark.numerics.kernels

/** One block's Cox rows in canonical order (Cox specification §6): each stratum's rows by exit time descending, and
  * `entryOrderAt(k)` the row at position k of the stratum's entry order. Row r's covariates are
  * `xs(r)(xBase(r) + j)`, so a kernel takes a row's array once, whether the rows are stored flat ([[CoxRows]]) or in
  * records ([[ChunkedCoxRows]], docs/spec/performance/step2-chunks.md §3); the arithmetic is the same either way.
  */
trait CoxRowsView {
  def p: Int
  def rows: Int
  def timeAt(r: Int): Double
  def eventAt(r: Int): Boolean
  def weightAt(r: Int): Double
  def offsetAt(r: Int): Double
  def entryAt(r: Int): Double
  def entryOrderAt(k: Int): Int
  def xs(r: Int): Array[Double]
  def xBase(r: Int): Int
}

/** Cox rows stored flat, covariates row-major. */
final case class CoxRows(
    time: Array[Double],
    entry: Array[Double],
    entryOrder: Array[Int],
    event: Array[Boolean],
    weight: Array[Double],
    offset: Array[Double],
    x: Array[Double],
    p: Int
) extends CoxRowsView {
  def rows: Int = time.length
  def timeAt(r: Int): Double = time(r)
  def eventAt(r: Int): Boolean = event(r)
  def weightAt(r: Int): Double = weight(r)
  def offsetAt(r: Int): Double = offset(r)
  def entryAt(r: Int): Double = entry(r)
  def entryOrderAt(k: Int): Int = entryOrder(k)
  def xs(r: Int): Array[Double] = x
  def xBase(r: Int): Int = r * p
}

/** Cox rows in records of `recordRows` rows, the last possibly shorter (step2-chunks.md §3): row r, and position r of
  * the entry order, are in record r / `recordRows` at r mod `recordRows`.
  */
final class ChunkedCoxRows(
    val p: Int,
    val recordRows: Int,
    time: Array[Array[Double]],
    entry: Array[Array[Double]],
    entryOrder: Array[Array[Int]],
    event: Array[Array[Boolean]],
    weight: Array[Array[Double]],
    offset: Array[Array[Double]],
    x: Array[Array[Double]]
) extends CoxRowsView {
  require(recordRows > 0, s"a record holds at least one row, got $recordRows")
  require(
    Seq(entry.length, entryOrder.length, event.length, weight.length, offset.length, x.length)
      .forall(_ == time.length),
    "every record needs all of its rows' columns"
  )
  require(
    time.indices.forall { i =>
      val count = time(i).length
      (if (i + 1 < time.length) count == recordRows else count >= 1 && count <= recordRows) &&
      entry(i).length == count && entryOrder(i).length == count && event(i).length == count &&
      weight(i).length == count && offset(i).length == count && x(i).length == count * p
    },
    s"every record holds $recordRows rows but the last, which holds 1 to $recordRows"
  )
  val rows: Int = time.iterator.map(_.length).sum
  def timeAt(r: Int): Double = time(r / recordRows)(r % recordRows)
  def eventAt(r: Int): Boolean = event(r / recordRows)(r % recordRows)
  def weightAt(r: Int): Double = weight(r / recordRows)(r % recordRows)
  def offsetAt(r: Int): Double = offset(r / recordRows)(r % recordRows)
  def entryAt(r: Int): Double = entry(r / recordRows)(r % recordRows)
  def entryOrderAt(k: Int): Int = entryOrder(k / recordRows)(k % recordRows)
  def xs(r: Int): Array[Double] = x(r / recordRows)
  def xBase(r: Int): Int = (r % recordRows) * p
}
