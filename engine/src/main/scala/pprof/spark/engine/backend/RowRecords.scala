package pprof.spark.engine.backend

import scala.util.DynamicVariable

/** Rows per stored record of a working set (docs/spec/performance/step2-chunks.md §3): at most 256 KiB of
  * covariates, so that no array of a record reaches 512 KiB, fixed in code. The count changes no result, so tests may
  * set it lower with [[rowsOverride]] to cross record boundaries in small data; a working set reads it on the driver
  * when it is built.
  */
private[engine] object RowRecords {

  /** The covariate bytes one record holds at most. */
  val RecordBytes: Int = 256 * 1024

  /** C = max(1, ⌊256 KiB / 8p⌋) rows: 182 at 180 features, 1,310 at 25. */
  def default(p: Int): Int = math.max(1, RecordBytes / (8 * math.max(1, p)))

  /** For tests: a record size in place of the default. */
  val rowsOverride: DynamicVariable[Option[Int]] = new DynamicVariable[Option[Int]](None)

  def rowsPerRecord(p: Int): Int = rowsOverride.value.getOrElse(default(p))
}
