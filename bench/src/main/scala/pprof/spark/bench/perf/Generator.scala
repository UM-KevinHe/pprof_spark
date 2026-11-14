package pprof.spark.bench.perf

import java.util.SplittableRandom

import scala.collection.immutable.ArraySeq

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{AnalysisException, DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import pprof.spark.bench.synthetic.{Manifest, Synthetic}
import pprof.spark.engine.BuildInfo
import pprof.spark.numerics.Normal

/** One point of the performance phase's data (docs/spec/performance/plan.md, step 0).
  *
  * Designs (the feasibility study's §5.1): `simple` has `databricks/02_scale.py`'s distributions, binary features at
  * P = 0.3 and features uniform on [−1, 1] in steps of 1/64 alternately, β alternating ±0.5/√p and provider effects on
  * [−0.5, 0.5]. `correlated` has the study's: correlated indicators from six latent factors, half of them rare, and
  * continuous covariates with offsets up to 65 and standard deviations from 0.1 to 50, β of random sign at 0.5/√p per
  * standard deviation, and provider effects −1 + 0.5·Z. Providers are equal in size and hold consecutive identifiers.
  * Cox outcomes have an exponential baseline (0.2 a year) and uniform censoring over five years, in whole days; an
  * unstratified Cox point has no provider effects.
  */
final case class PerfDesign(
    model: String,
    design: String = "simple",
    n: Long = 5000000L,
    m: Int = 8000,
    p: Int = 180,
    seed: Long = 79L,
    stratified: Boolean = true
) {
  require(model == "cox" || model == "logistic", s"model must be cox or logistic, got $model")
  require(
    design == "simple" || design == "correlated",
    s"design must be simple or correlated, got $design"
  )
  require(
    n >= 1L && m >= 1 && m.toLong <= n && p >= 1,
    s"need 1 <= m <= n and p >= 1, got n = $n, m = $m, p = $p"
  )
  require(design == "simple" || p >= 3, "the correlated design needs at least three features")
  require(stratified || model == "cox", "only Cox data can be unstratified")

  def features: Seq[String] = (1 to p).map(k => s"x$k")

  def name: String =
    s"$model-$design-n$n-m$m-p$p-s$seed" + (if (stratified) "" else "-unstratified")
}

/** The design's fixed quantities, drawn once from the seed in a fixed order, and each row as a pure function of the
  * seed and its identifier.
  */
final class PerfParameters(val design: PerfDesign) extends Serializable {
  import PerfParameters._

  val p: Int = design.p
  private val correlated = design.design == "correlated"

  /** Indicators of each kind (rare, then common) and continuous covariates, in the correlated design. */
  val k: Int = if (correlated) p / 3 else 0
  val kc: Int = if (correlated) p - 2 * k else 0

  /** Loadings of the indicators on the factors, factor-major (Factors × 2k). */
  val load: Array[Double] = new Array[Double](Factors * 2 * k)
  val prevalence: Array[Double] = new Array[Double](2 * k)
  val threshold: Array[Double] = new Array[Double](2 * k)

  /** Weights of the continuous covariates on the first three factors, factor-major (3 × kc). */
  val weights: Array[Double] = new Array[Double](3 * kc)
  val spread: Array[Double] = new Array[Double](kc)
  val offset: Array[Double] = new Array[Double](kc)
  val scale: Array[Double] = new Array[Double](kc)
  val mean: Array[Double] = new Array[Double](p)
  val beta: Array[Double] = new Array[Double](p)

  locally {
    if (!correlated) {
      var j = 0
      while (j < p) {
        beta(j) = (if ((j + 1) % 2 == 0) 1.0 else -1.0) * 0.5 / math.sqrt(p.toDouble)
        mean(j) = if (j % 2 == 0) 0.3 else 0.0
        j += 1
      }
    } else {
      val rng = new SplittableRandom(mix64(design.seed + 0x6a09e667f3bcc909L))
      var i = 0
      while (i < Factors * 2 * k) {
        val a = 0.3 + 0.5 * rng.nextDouble()
        load(i) = if (rng.nextDouble() < 0.35) a else 0.0
        i += 1
      }
      var j = 0
      while (j < 2 * k) {
        prevalence(j) = if (j < k) 0.01 + 0.07 * rng.nextDouble() else 0.1 + 0.4 * rng.nextDouble()
        var s = 1.0
        var f = 0
        while (f < Factors) {
          val l = load(f * 2 * k + j)
          s += l * l
          f += 1
        }
        threshold(j) = Normal.upperQuantile(prevalence(j)) * math.sqrt(s)
        mean(j) = prevalence(j)
        j += 1
      }
      var c = 0
      while (c < kc) {
        var s = 1.0
        var f = 0
        while (f < 3) {
          val w = -0.8 + 1.6 * rng.nextDouble()
          weights(f * kc + c) = w
          s += w * w
          f += 1
        }
        spread(c) = math.sqrt(s)
        offset(c) = 65.0 * rng.nextDouble()
        scale(c) = StrictMath.pow(10.0, -1.0 + 2.7 * rng.nextDouble())
        mean(2 * k + c) = offset(c)
        c += 1
      }
      var q = 0
      while (q < p) {
        val sd =
          if (q < 2 * k) math.sqrt(prevalence(q) * (1.0 - prevalence(q))) else scale(q - 2 * k)
        beta(q) = (if (rng.nextDouble() < 0.5) -1.0 else 1.0) * 0.5 / math.sqrt(p.toDouble) / sd
        q += 1
      }
    }
  }

  /** The true effect of provider `j`. */
  def effect(j: Long): Double =
    if (!design.stratified) 0.0
    else {
      val rng = new SplittableRandom(mix64(mix64(design.seed + 0x2545f4914f6cdd1dL) + j))
      if (correlated) -1.0 + 0.5 * normal(rng)
      else (math.floor(rng.nextDouble() * 201.0) - 100.0) / 200.0
    }

  /** Row `id` of provider `provider`, whose effect is `effect`; `x` is scratch space of length p. */
  def row(id: Long, provider: Long, effect: Double, x: Array[Double]): Row = {
    val rng = new SplittableRandom(mix64(mix64(design.seed) + id))
    var linear = 0.0
    if (!correlated) {
      var j = 0
      while (j < p) {
        val u = rng.nextDouble()
        x(j) =
          if (j % 2 == 0) (if (u < 0.3) 1.0 else 0.0) else (math.floor(u * 129.0) - 64.0) / 64.0
        linear += beta(j) * x(j)
        j += 1
      }
    } else {
      val factor = new Array[Double](Factors)
      var f = 0
      while (f < Factors) {
        factor(f) = normal(rng)
        f += 1
      }
      var j = 0
      while (j < 2 * k) {
        var z = normal(rng)
        f = 0
        while (f < Factors) {
          z += load(f * 2 * k + j) * factor(f)
          f += 1
        }
        x(j) = if (z > threshold(j)) 1.0 else 0.0
        j += 1
      }
      var c = 0
      while (c < kc) {
        var g = normal(rng)
        f = 0
        while (f < 3) {
          g += weights(f * kc + c) * factor(f)
          f += 1
        }
        x(2 * k + c) = offset(c) + scale(c) * g / spread(c)
        c += 1
      }
      j = 0
      while (j < p) {
        linear += beta(j) * (x(j) - mean(j))
        j += 1
      }
    }
    val eta = effect + linear
    val cox = design.model == "cox"
    val values = new Array[Any](p + (if (cox) 4 else 3))
    values(0) = id
    values(1) = provider
    var j = 0
    while (j < p) {
      values(2 + j) = x(j)
      j += 1
    }
    if (cox) {
      val event = -StrictMath.log(1.0 - rng.nextDouble()) / (0.2 * StrictMath.exp(eta))
      val end = math.min(5.0 * rng.nextDouble(), 5.0)
      values(p + 2) = math.max(1.0, math.ceil(365.0 * math.min(event, end)))
      values(p + 3) = if (event <= end) 1 else 0
    } else values(p + 2) = if (rng.nextDouble() < 1.0 / (1.0 + StrictMath.exp(-eta))) 1 else 0
    Row.fromSeq(ArraySeq.unsafeWrapArray(values))
  }
}

object PerfParameters {
  val Factors: Int = 6

  /** SplitMix64's finalizer. */
  def mix64(z0: Long): Long = {
    var z = z0 + 0x9e3779b97f4a7c15L
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
    z ^ (z >>> 31)
  }

  /** A standard normal draw by Box and Muller, with fdlibm's functions so that every JVM draws the same. */
  def normal(rng: SplittableRandom): Double = {
    val u = 1.0 - rng.nextDouble()
    math.sqrt(-2.0 * StrictMath.log(u)) * StrictMath.cos(2.0 * math.Pi * rng.nextDouble())
  }
}

/** The data of a point under a storage path: Parquet rows in `data/` and one line of JSON in `manifest/`. */
object Generator {

  val Version: Int = 1
  val TargetFileBytes: Double = 128.0 * 1024 * 1024

  final case class Prepared(
      data: DataFrame,
      generated: Boolean,
      seconds: Double,
      verified: Option[Boolean],
      checksum: Long
  )

  def schema(d: PerfDesign): StructType = StructType(
    Seq(StructField("id", LongType, false), StructField("provider", LongType, false)) ++
      d.features.map(StructField(_, DoubleType, false)) ++
      (if (d.model == "cox")
         Seq(StructField("time", DoubleType, false), StructField("event", IntegerType, false))
       else Seq(StructField("y", IntegerType, false)))
  )

  /** Provider j holds rows ⌈j·n/m⌉ until ⌈(j + 1)·n/m⌉, as `02_scale.py`'s `(id * m) div n`. */
  def providerOf(d: PerfDesign, id: Long): Long = (id * d.m) / d.n

  /** The rows, in `partitions` ranges of identifiers; identical for any number of partitions. */
  def rows(spark: SparkSession, d: PerfDesign, partitions: Int): DataFrame = {
    require(partitions >= 1, s"at least one partition is required, got $partitions")
    val parameters = new PerfParameters(d)
    spark
      .range(0L, d.n, 1L, partitions)
      .mapPartitions { ids =>
        val x = new Array[Double](d.p)
        var provider = -1L
        var effect = 0.0
        ids.map { boxed =>
          val id = boxed.longValue
          val j = providerOf(d, id)
          if (j != provider) {
            provider = j
            effect = parameters.effect(j)
          }
          parameters.row(id, j, effect, x)
        }
      }(Encoders.row(schema(d)))
      .toDF()
  }

  /** The point's data under `path`, generated and written once; a later call reads them and, with `verify`, checks
    * the row count and checksum against the manifest.
    */
  def prepare(spark: SparkSession, d: PerfDesign, path: String, verify: Boolean): Prepared = {
    val start = System.nanoTime()
    def seconds = (System.nanoTime() - start) / 1e9
    existing(spark, path) match {
      case Some(json) =>
        val manifest = Manifest.parse(json)
        require(
          manifest.get("point").asText() == d.name,
          s"the data under $path are ${manifest.get("point").asText()}, not ${d.name}"
        )
        val data = spark.read.parquet(s"$path/data")
        val checksum = manifest.get("checksum").asLong()
        val verified =
          if (!verify) None
          else
            Some(
              data.count() == manifest.get("rows").asLong() && Synthetic.checksum(data) == checksum
            )
        verified.foreach(ok => require(ok, s"the data under $path no longer match their manifest"))
        Prepared(data, generated = false, seconds, verified, checksum)
      case None =>
        val files =
          math.max(1L, math.ceil(d.n.toDouble * 8.0 * (d.p + 4) / TargetFileBytes).toLong).toInt
        rows(spark, d, files).write.parquet(s"$path/data")
        val data = spark.read.parquet(s"$path/data")
        val count = data.count()
        val checksum = Synthetic.checksum(data)
        val mapper = new ObjectMapper()
        val root = mapper.createObjectNode()
        root
          .put("generator", "pprof_spark.bench.perf")
          .put("version", Version)
          .put("git_sha", BuildInfo.gitSha)
        root
          .put("point", d.name)
          .put("model", d.model)
          .put("design", d.design)
          .put("n", d.n)
          .put("m", d.m)
        root.put("p", d.p).put("seed", d.seed).put("stratified", d.stratified)
        val beta = root.putArray("beta")
        new PerfParameters(d).beta.foreach(b => beta.add(b))
        root.put("rows", count).put("files", files).put("checksum", checksum)
        spark
          .createDataset(Seq(mapper.writeValueAsString(root)))(Encoders.STRING)
          .coalesce(1)
          .write
          .text(s"$path/manifest")
        Prepared(data, generated = true, seconds, Some(count == d.n), checksum)
    }
  }

  private def existing(spark: SparkSession, path: String): Option[String] =
    try Some(spark.read.text(s"$path/manifest").head().getString(0))
    catch { case _: AnalysisException => None }
}
