package pprof.spark.engine.backend

import org.apache.spark.sql.{Dataset, SparkSession}

/** Names the Spark jobs of each operation of a fit (PROJECT_CONTEXT §6.13, OI-108). The operation's tag, "pprof."
  * and its name, is added to the session for the calling thread while the action runs and removed afterwards, so
  * that a listener can attribute the jobs to the operation rather than to a line of code or a notebook's cell. Tags
  * change no result, and `SparkSession.addTag` serves Spark Connect as it does Classic, in Spark 4.0 and 4.1.
  */
object Operations {

  val Prefix: String = "pprof."

  def tagged[T](spark: SparkSession, operation: String)(body: => T): T = {
    val tag = Prefix + operation
    spark.addTag(tag)
    try body
    finally spark.removeTag(tag)
  }

  /** A Dataset's actions under an operation's tag. */
  implicit final class TaggedActions[T](private val ds: Dataset[T]) extends AnyVal {
    def collectAs(operation: String): Array[T] = tagged(ds.sparkSession, operation)(ds.collect())
    def countAs(operation: String): Long = tagged(ds.sparkSession, operation)(ds.count())
  }
}
