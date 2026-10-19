package pprof.spark.bench.synthetic

import org.apache.spark.sql.functions.{col, xxhash64}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}

/** Deterministic synthetic data for the scale tests (scale-test plan §4). Rows are generated from `spark.range(n)`
  * in contiguous ranges of identifiers, each value a function of the design and the row's identifier alone
  * ([[Draws]]), so the data are identical for any number of partitions and can be regenerated exactly.
  */
object Synthetic {

  /** The generator's version, in every manifest. It changes whenever a generated value changes. */
  val Version: Int = 1

  /** Rows per Parquet file are chosen for files of about this size before compression (plan §4). */
  val TargetFileBytes: Long = 128L * 1024 * 1024

  /** The streams of [[Draws]]: one per generated quantity, so that no two quantities share a draw. */
  private object Streams {
    val Effect = 1
    val Outcome = 2
    val Trials = 3
    val EventTime = 4
    val Censoring = 5
    val Noise = 6
    def feature(k: Int): Int = 1000 + k
  }

  /** The columns: `id` and `provider`, the model's outcome columns, then the features. */
  def schema(design: Design): StructType = {
    val outcome = design.model match {
      case _: Model.Cox =>
        Seq(StructField("time", DoubleType, false), StructField("event", IntegerType, false))
      case l: Model.Logistic =>
        StructField("y", IntegerType, false) +: (if (l.maxTrials > 1)
                                                   Seq(StructField("n", IntegerType, false))
                                                 else Nil)
      case _: Model.Linear => Seq(StructField("y", DoubleType, false))
    }
    StructType(
      Seq(
        StructField("id", LongType, false),
        StructField("provider", LongType, false)
      ) ++ outcome ++
        design.features.map(StructField(_, DoubleType, false))
    )
  }

  /** Provider j's effect: γⱼ for the logistic and linear models, log λⱼ for Cox. */
  def effect(design: Design, provider: Int): Double = {
    val z = Draws.normal(design.seed, provider.toLong, Streams.Effect)
    design.model match {
      case c: Model.Cox      => StrictMath.log(c.scale) + c.spread * z
      case l: Model.Logistic => l.gammaMean + l.gammaSd * z
      case l: Model.Linear   => l.gammaMean + l.gammaSd * z
    }
  }

  /** Feature k of row `id`: binary with P(1) = 0.3 when k is odd, a standard normal on a 1/64 grid when even. */
  def feature(seed: Long, id: Long, k: Int): Double =
    if (k % 2 == 1) { if (Draws.uniform(seed, id, Streams.feature(k)) < 0.3) 1.0 else 0.0 }
    else StrictMath.rint(64.0 * Draws.normal(seed, id, Streams.feature(k))) / 64.0

  /** Row `id` of provider `provider`, whose effect is `effect`: a function of the design and `id` alone. */
  def row(design: Design, beta: Array[Double], id: Long, provider: Int, effect: Double): Row = {
    val seed = design.seed
    val x = Array.tabulate(design.p)(k => feature(seed, id, k + 1))
    var eta = 0.0
    var k = 0
    while (k < x.length) { eta += beta(k) * x(k); k += 1 }
    val outcome: Seq[Any] = design.model match {
      case c: Model.Cox =>
        val rate = StrictMath.exp(effect + eta)
        val t = StrictMath.pow(
          -StrictMath.log(Draws.uniform(seed, id, Streams.EventTime)) / rate,
          1.0 / c.shape
        )
        val censoring = c.horizon * Draws.uniform(seed, id, Streams.Censoring)
        val end = math.min(t, math.min(censoring, c.horizon))
        Seq(StrictMath.ceil(365.0 * end), if (t <= math.min(censoring, c.horizon)) 1 else 0)
      case l: Model.Logistic =>
        val probability = 1.0 / (1.0 + StrictMath.exp(-(effect + eta)))
        val trials =
          if (l.maxTrials == 1) 1
          else 1 + (Draws.uniform(seed, id, Streams.Trials) * l.maxTrials).toInt
        var successes = 0
        var t = 0
        while (t < trials) {
          if (Draws.uniform(seed, id, Streams.Outcome, t) < probability) successes += 1
          t += 1
        }
        if (l.maxTrials == 1) Seq(successes) else Seq(successes, trials)
      case l: Model.Linear => Seq(effect + eta + l.sigma * Draws.normal(seed, id, Streams.Noise))
    }
    Row.fromSeq(Seq[Any](id, provider.toLong) ++ outcome ++ x.toSeq)
  }

  /** Partitions for files of about [[TargetFileBytes]], at eight bytes a column before compression. */
  def defaultPartitions(design: Design): Int = {
    val bytes = design.n.toDouble * 8.0 * schema(design).length
    math.max(1L, math.ceil(bytes / TargetFileBytes.toDouble).toLong).toInt
  }

  /** The provider starts of a design, computed once per JVM where they are needed (the driver, then each executor),
    * so that generation needs neither a broadcast nor a `SparkContext`.
    */
  private object Starts {
    private val cache =
      new java.util.concurrent.ConcurrentHashMap[(Sizes, Long, Int), Array[Long]]()
    def apply(design: Design): Array[Long] = {
      if (cache.size > 16) cache.clear()
      cache.computeIfAbsent(
        (design.sizes, design.n, design.m),
        _ => Sizes.starts(design.sizes.sizes(design.n, design.m))
      )
    }
  }

  /** The rows, in `partitions` contiguous ranges of identifiers; identical for any number of partitions. */
  def rows(spark: SparkSession, design: Design, partitions: Int): DataFrame = {
    require(partitions >= 1, s"at least one partition is required, got $partitions")
    Starts(design) // validates the design on the driver
    val beta = design.beta.toArray
    spark
      .range(0L, design.n, 1L, partitions)
      .mapPartitions { ids =>
        val s = Starts(design)
        var provider = -1
        var effectOf = 0.0
        ids.map { boxed =>
          val id = boxed.longValue
          if (provider < 0 || id < s(provider) || id >= s(provider + 1)) {
            provider = Sizes.providerOf(s, id)
            effectOf = effect(design, provider)
          }
          row(design, beta, id, provider, effectOf)
        }
      }(Encoders.row(schema(design)))
      .toDF()
  }

  /** The true provider effects: `provider`, its first row `start`, its `size` and its `effect` (as [[effect]]). */
  def providers(spark: SparkSession, design: Design): DataFrame = {
    Starts(design)
    val schema = StructType(
      Seq("provider", "start", "size")
        .map(StructField(_, LongType, false)) :+ StructField("effect", DoubleType, false)
    )
    spark
      .range(0L, design.m.toLong)
      .mapPartitions { ids =>
        val s = Starts(design)
        ids.map { boxed =>
          val j = boxed.intValue
          Row(j.toLong, s(j), s(j + 1) - s(j), effect(design, j))
        }
      }(Encoders.row(schema))
      .toDF()
  }

  /** An order-independent fingerprint of a table: the wrapping sum of every row's `xxhash64` (plan §4). */
  def checksum(df: DataFrame): Long =
    df.select(xxhash64(df.columns.toIndexedSeq.map(col): _*))
      .as(Encoders.scalaLong)
      .mapPartitions(hashes => Iterator(hashes.foldLeft(0L)(_ + _)))(Encoders.scalaLong)
      .collect()
      .foldLeft(0L)(_ + _)

  /** Writes the design's data under `path`: the rows in `data/`, the true provider effects in `providers/` (both
    * Parquet) and the [[Manifest]] in `manifest/` (one line of JSON). Never overwrites: an existing path fails.
    */
  def write(spark: SparkSession, design: Design, path: String, partitions: Int = 0): Manifest = {
    val files = if (partitions > 0) partitions else defaultPartitions(design)
    rows(spark, design, files).write.parquet(s"$path/data")
    providers(spark, design).write.parquet(s"$path/providers")
    val data = spark.read.parquet(s"$path/data")
    val manifest = Manifest(
      design,
      rows = data.count(),
      files = files,
      checksum = checksum(data),
      providersChecksum = checksum(spark.read.parquet(s"$path/providers"))
    )
    spark
      .createDataset(Seq(manifest.json))(Encoders.STRING)
      .coalesce(1)
      .write
      .text(s"$path/manifest")
    manifest
  }

  /** The manifest written under `path`, as JSON. */
  def manifest(spark: SparkSession, path: String): String =
    spark.read.text(s"$path/manifest").head().getString(0)

  /** Whether the data under `path` still match their manifest: the same row count and checksum. */
  def verify(spark: SparkSession, path: String): Boolean = {
    val recorded = Manifest.parse(manifest(spark, path))
    val data = spark.read.parquet(s"$path/data")
    data.count() == recorded.get("rows").asLong && checksum(data) == recorded.get("checksum").asLong
  }
}
