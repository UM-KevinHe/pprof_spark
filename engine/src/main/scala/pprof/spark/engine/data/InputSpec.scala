package pprof.spark.engine.data

/** The input columns of a blocked computation: the minimal data contract of the platform skeleton
  * (§7.1, §7.2). Column names are matched exactly.
  *
  * @param groupCol integral or string column; each group stays whole within one logical block,
  *   like a stratum or a provider
  * @param featureCols numeric columns, converted to doubles
  * @param rowIdCol optional integral column of unique row identifiers, which fixes the canonical
  *   row order (DIST-6); without it, rows are ordered by their values
  */
final case class InputSpec(
    groupCol: String,
    featureCols: Seq[String],
    rowIdCol: Option[String] = None
) {
  require(featureCols.nonEmpty, "at least one feature column is required")
  require(columns.distinct.size == columns.size, "column names must be distinct")

  /** Every input column, group column first. */
  def columns: Seq[String] = groupCol +: (featureCols ++ rowIdCol.toSeq)
}
