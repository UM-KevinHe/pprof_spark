package pprof.spark.engine.cox {

  import org.apache.spark.sql.{DataFrame, Encoders}
  import org.apache.spark.storage.StorageLevel
  import pprof.spark.engine.backend.OrderedReduction
  import pprof.spark.numerics.{NeumaierSum, NeumaierVector}
  import pprof.spark.numerics.kernels.Moments

  /** A prototype of the recommended pass on the engine's own blocks: the information as
    * sum_i r_i c_i x_i x_i' - sum_t d_t xbar_t xbar_t' (Breslow, rows without entry times), accumulated with
    * plain sums over 256 rows that are then folded into Neumaier totals; block partials reduced on the
    * executors. For the review only: not the engine's kernel.
    */
  object HybridProbe {
    final class Acc(val p: Int, chunk: Int) {
      val value = new NeumaierSum
      val score = new NeumaierVector(p)
      val a: Array[Array[Double]] = Array.fill(p)(new Array[Double](p))
      val b: Array[Array[Double]] = Array.fill(p)(new Array[Double](p))
      val total: Array[NeumaierVector] = Array.fill(p)(new NeumaierVector(p))
      private var pending = 0
      def tick(): Unit = { pending += 1; if (pending >= chunk) flush() }
      def flush(): Unit = {
        var j = 0
        while (j < p) {
          val aj = a(j); val bj = b(j); val tj = total(j)
          var l = j
          while (l < p) { tj.addAt(l, aj(l) - bj(l)); aj(l) = 0.0; bj(l) = 0.0; l += 1 }
          j += 1
        }
        pending = 0
      }
      def packed: Array[Double] = {
        val out = new Array[Double](Moments.packedLength(p))
        var k = 0; var j = 0
        while (j < p) { var l = j; while (l < p) { out(k) = total(j).valueAt(l); k += 1; l += 1 }; j += 1 }
        out
      }
    }

    def stratum(block: CoxBlock, from: Int, until: Int, beta: Array[Double], center: Array[Double], acc: Acc): Unit = {
      val p = block.p
      val x = block.x
      val n = until - from
      val s0 = new NeumaierSum
      val s1 = new NeumaierVector(p)
      val xc = new Array[Double](p)
      val eventSum = new Array[Double](p)
      val mean = new Array[Double](p)
      val risk = new Array[Double](n)
      val atExit = new Array[Double](n)
      var cum = 0.0
      var r = from
      while (r < until) {
        val t = block.time(r)
        var dn = 0; var dw = 0.0; var etaEvents = 0.0
        java.util.Arrays.fill(eventSum, 0.0)
        while (r < until && block.time(r) == t) {
          val w = block.weight(r)
          if (w > 0.0) {
            var j = 0; var linear = 0.0
            while (j < p) { xc(j) = x(r * p + j) - center(j); linear += xc(j) * beta(j); j += 1 }
            val eta = linear + block.offset(r)
            val rw = w * StrictMath.exp(eta)
            risk(r - from) = rw
            atExit(r - from) = cum
            s0.add(rw)
            j = 0
            while (j < p) { s1.addAt(j, rw * xc(j)); j += 1 }
            if (block.event(r)) {
              dn += 1; dw += w; etaEvents += w * eta
              j = 0
              while (j < p) { eventSum(j) += w * xc(j); j += 1 }
            }
          }
          r += 1
        }
        if (dn > 0) {
          val total0 = s0.value
          acc.value.add(etaEvents - dw * StrictMath.log(total0))
          var j = 0
          while (j < p) { mean(j) = s1.valueAt(j) / total0; acc.score.addAt(j, eventSum(j) - dw * mean(j)); j += 1 }
          j = 0
          while (j < p) {
            val wm = dw * mean(j)
            val row = acc.b(j)
            var l = j
            while (l < p) { row(l) += wm * mean(l); l += 1 }
            j += 1
          }
          acc.tick()
          cum += dw / total0
        }
      }
      r = from
      while (r < until) {
        val cw = risk(r - from) * (cum - atExit(r - from))
        if (cw != 0.0) {
          var j = 0
          while (j < p) { xc(j) = x(r * p + j) - center(j); j += 1 }
          j = 0
          while (j < p) {
            val wx = cw * xc(j)
            val row = acc.a(j)
            var l = j
            while (l < p) { row(l) += wx * xc(l); l += 1 }
            j += 1
          }
          acc.tick()
        }
        r += 1
      }
    }

    def partial(block: CoxBlock, beta: Array[Double], center: Array[Double]): CoxPartial = {
      val acc = new Acc(block.p, 256)
      var g = 0
      while (g < block.groupCount) { stratum(block, block.groupStart(g), block.groupEnd(g), beta, center, acc); g += 1 }
      acc.flush()
      CoxPartial(block.blockId, acc.value.value, acc.score.values, acc.packed)
    }

    def run(df: DataFrame, spec: CoxSpec, options: CoxOptions, beta: Array[Double]): Seq[(String, Double)] =
      CoxPH.withWorkingSet(df, spec, options) { prepared =>
        require(spec.entry.isEmpty, "the prototype covers rows without entry times")
        val out = Seq.newBuilder[(String, Double)]
        def best(label: String)(body: => Unit): Unit = {
          var bst = Long.MaxValue; var t = 0
          while (t < 4) { val t0 = System.nanoTime(); body; val dt = System.nanoTime() - t0; if (t >= 2 && dt < bst) bst = dt; t += 1 }
          out += label -> bst / 1e9
        }
        val blocks = prepared.workingSet.blocks
        val p = spec.features.size
        val q = Moments.packedLength(p)
        val c = prepared.center.clone()
        def reduced(parts: Array[CoxPartial]): (Double, Array[Double], Array[Double]) = {
          val v = new NeumaierSum
          parts.sortBy(_.blockId).foreach(x => v.add(x.value))
          (v.value, OrderedReduction.reduce(parts.toSeq.map(x => x.blockId -> x.score), p),
            OrderedReduction.reduce(parts.toSeq.map(x => x.blockId -> x.information), q))
        }
        val ref = reduced(blocks.map((b: CoxBlock) => CoxKernel.partial(b, beta, c, false))(Encoders.product[CoxPartial]).collect())
        val hyb = reduced(blocks.map((b: CoxBlock) => partial(b, beta, c))(Encoders.product[CoxPartial]).collect())
        def scaled(x: Array[Double], y: Array[Double]): Double = x.indices.iterator.map(i => math.abs(x(i) - y(i))).max / y.iterator.map(math.abs).max
        out += "prototype against the engine: log-likelihood (relative)" -> math.abs(hyb._1 - ref._1) / math.abs(ref._1)
        out += "prototype against the engine: score (scaled)" -> scaled(hyb._2, ref._2)
        out += "prototype against the engine: information (scaled)" -> scaled(hyb._3, ref._3)
        val rdd = blocks.rdd.persist(StorageLevel.MEMORY_ONLY)
        rdd.count()
        best("today: Dataset cache, engine kernel, partials collected") {
          blocks.map((b: CoxBlock) => CoxKernel.partial(b, beta, c, false))(Encoders.product[CoxPartial]).collect(); ()
        }
        best("Dataset cache, prototype kernel, partials collected") {
          blocks.map((b: CoxBlock) => partial(b, beta, c))(Encoders.product[CoxPartial]).collect(); ()
        }
        best("RDD object cache, prototype kernel, treeAggregate") {
          rdd.treeAggregate((0.0, new Array[Double](p), new Array[Double](q)))(
            (acc, b) => {
              val part = partial(b, beta, c)
              var i = 0
              while (i < p) { acc._2(i) += part.score(i); i += 1 }
              i = 0
              while (i < q) { acc._3(i) += part.information(i); i += 1 }
              (acc._1 + part.value, acc._2, acc._3)
            },
            (x, y) => {
              var i = 0
              while (i < p) { x._2(i) += y._2(i); i += 1 }
              i = 0
              while (i < q) { x._3(i) += y._3(i); i += 1 }
              (x._1 + y._1, x._2, x._3)
            },
            depth = 2
          ); ()
        }
        out += "blocks" -> prepared.plan.blockCount.toDouble
        rdd.unpersist()
        out.result()
      }
  }
}

package bench {

  import org.apache.spark.sql.SparkSession
  import pprof.spark.engine.cox.{CoxOptions, CoxSpec, HybridProbe}

  object HybridBench {
    def main(args: Array[String]): Unit = {
      val n = args(0).toLong; val m = args(1).toInt; val p = args(2).toInt
      val spark = SparkSession.builder().master("local[1]").appName("hybrid-probe")
        .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", args(3)).getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      val features = (1 to p).map(k => s"x$k")
      val truth = (1 to p).map(k => (if (k % 2 == 0) 1.0 else -1.0) * 0.5 / math.sqrt(p.toDouble))
      val seed = 79
      def uniform(key: String, tag: String) = s"((pmod(hash($key, $seed, '$tag'), 1000003) + 0.5) / 1000003.0)"
      def feature(k: Int) =
        if (k % 2 == 1) s"CAST(CASE WHEN ${uniform("id", s"x$k")} < 0.3 THEN 1.0 ELSE 0.0 END AS DOUBLE)"
        else s"CAST((pmod(hash(id, $seed, 'x$k'), 129) - 64) / 64.0 AS DOUBLE)"
      val base = spark.range(0L, n, 1L, args(4).toInt).selectExpr("id", s"(id * $m) div $n AS provider")
      val withFeatures = base.selectExpr(Seq("id", "provider") ++ (1 to p).map(k => s"${feature(k)} AS x$k"): _*)
      val linear = truth.zipWithIndex.map { case (b, i) => s"(${b}D) * x${i + 1}" }.mkString(" + ")
      val effect = s"((pmod(hash(provider, $seed, 'gamma'), 201) - 100) / 200.0)"
      val hazard = s"0.2 * exp($effect) * exp($linear)"
      val df = withFeatures.selectExpr("*", s"-ln(${uniform("id", "event_time")}) / ($hazard) AS event_years", s"5.0 * ${uniform("id", "censoring")} AS censor_years")
        .selectExpr(Seq("id", "provider") ++ features ++ Seq(
          "CAST(greatest(1.0, ceil(365.0 * least(event_years, censor_years, 5.0))) AS DOUBLE) AS time",
          "CASE WHEN event_years <= least(censor_years, 5.0) THEN 1 ELSE 0 END AS event"): _*)
      val cached = df.persist()
      cached.count()
      println(s"n=$n m=$m p=$p shufflePartitions=${args(3)} inputPartitions=${args(4)} master=local[1] java=${System.getProperty("java.version")}")
      val rows = HybridProbe.run(cached, CoxSpec("time", "event", features, strata = Some("provider")), CoxOptions(), truth.toArray)
      rows.foreach { case (label, v) => println(if (v < 1e-6 && v > 0) f"  $label%-62s $v%.2e" else f"  $label%-62s $v%9.3f") }
      spark.stop()
    }
  }
}
