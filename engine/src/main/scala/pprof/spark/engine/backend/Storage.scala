package pprof.spark.engine.backend

import org.apache.spark.sql.Dataset

/** Persisting at a fit's storage level as the Dataset's own session resolves it (PROJECT_CONTEXT
  * §10.3, D-77): every table a fit persists goes through `persistAt`.
  */
object Storage {

  implicit final class PersistAt[T](private val ds: Dataset[T]) extends AnyVal {
    def persistAt(blocks: BlockOptions): Dataset[T] =
      ds.persist(blocks.storageLevelIn(ds.sparkSession))
  }
}
