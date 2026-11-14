package pprof.spark.bench.perf

import java.lang.management.{ManagementFactory, MemoryType}
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerBlockUpdated,
  SparkListenerEvent,
  SparkListenerExecutorMetricsUpdate,
  SparkListenerJobStart,
  SparkListenerStageCompleted,
  SparkListenerStageSubmitted,
  SparkListenerTaskEnd
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.{
  SparkListenerSQLExecutionEnd,
  SparkListenerSQLExecutionStart
}

/** One SQL execution's totals. */
final class ExecutionRecord(val id: Long, val site: String, val start: Long, val plan: String) {
  var end: Long = -1L
  var tags: Set[String] = Set.empty
  var tasks: Long = 0L
  var runMs: Long = 0L
  var cpuNs: Long = 0L
  var gcMs: Long = 0L
  var resultBytes: Long = 0L
  var shuffleWriteBytes: Long = 0L
  var shuffleReadBytes: Long = 0L
  var spilledBytes: Long = 0L
  var peakTaskMemory: Long = 0L
  var singleTaskStages: Int = 0
  def wallMs: Long = if (end >= start) end - start else 0L
}

/** One call site's totals over a window: the SQL executions with one description (the call site) and tag set. */
final case class SiteSummary(
    site: String,
    tags: String,
    executions: Int,
    wallMs: Long,
    runMs: Long,
    cpuMs: Long,
    gcMs: Long,
    tasks: Long,
    resultBytes: Long,
    maxResultBytes: Long,
    shuffleWriteBytes: Long,
    shuffleReadBytes: Long,
    spilledBytes: Long,
    peakTaskMemory: Long,
    singleTaskStages: Int,
    plan: String
)

/** A window's call sites, the executor time no SQL execution claims, and the memory peaks. */
final case class WindowSummary(
    sites: Vector[SiteSummary],
    unattributedTasks: Long,
    unattributedCpuMs: Long,
    unattributedRunMs: Long,
    driverHeapPeakBytes: Long,
    driverHeapPoolPeakBytes: Long,
    driverMaxHeapBytes: Long,
    executorHeapPeakBytes: Long,
    cachedPeakBytes: Long
)

/** What step 0 of the performance plan records, by SQL execution (plan §4): wall, executor run, CPU and GC time,
  * task results (the bytes sent to the driver), shuffle bytes, spills, peak task memory and single-task stages; the
  * driver's heap peak, by sampling and by its pools; the executors' heap peaks from their heartbeats; and the peak of
  * cached RDD blocks. Stages are attributed when they are submitted, from their own properties, so that the map stages
  * adaptive execution runs as jobs of their own are counted; executor time without an execution is kept apart.
  * Classic Spark only: `bench` code, never engine code (ARCH-2).
  */
final class Recorder private (spark: SparkSession) extends SparkListener {
  private val executions = mutable.LinkedHashMap.empty[Long, ExecutionRecord]
  private val stageExecution = mutable.HashMap.empty[Int, Long]
  private val events = new AtomicLong()
  private var unattributedTasks = 0L
  private var unattributedCpuNs = 0L
  private var unattributedRunMs = 0L
  private val executorHeap = mutable.HashMap.empty[String, Long]
  private val cachedBlocks = mutable.HashMap.empty[(String, String), Long]
  private var cached = 0L
  private var cachedPeak = 0L
  @volatile private var heapPeak = 0L
  @volatile private var running = true
  private val heapPools =
    ManagementFactory.getMemoryPoolMXBeans.asScala.filter(_.getType == MemoryType.HEAP).toVector
  private val sampler = new Thread(
    () =>
      while (running) {
        val used = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed
        if (used > heapPeak) heapPeak = used
        Thread.sleep(50L)
      },
    "pprof-perf-heap"
  )

  private def executionOf(properties: Properties): Option[Long] =
    Option(properties).flatMap(p => Option(p.getProperty("spark.sql.execution.id"))).map(_.toLong)

  override def onOtherEvent(event: SparkListenerEvent): Unit = synchronized {
    events.incrementAndGet()
    event match {
      case e: SparkListenerSQLExecutionStart =>
        executions(e.executionId) =
          new ExecutionRecord(e.executionId, e.description, e.time, e.physicalPlanDescription)
      case e: SparkListenerSQLExecutionEnd => executions.get(e.executionId).foreach(_.end = e.time)
      case _                               =>
    }
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = synchronized {
    events.incrementAndGet()
    for (id <- executionOf(event.properties); x <- executions.get(id)) {
      val tags = Option(event.properties.getProperty("spark.job.tags"))
        .fold(Set.empty[String])(_.split(",").flatMap(tag => Recorder.operationTag(tag.trim)).toSet)
      x.tags = x.tags ++ tags
      event.stageIds.foreach(s => stageExecution.getOrElseUpdate(s, id))
    }
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = synchronized {
    events.incrementAndGet()
    executionOf(event.properties).foreach(id => stageExecution(event.stageInfo.stageId) = id)
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = synchronized {
    events.incrementAndGet()
    if (event.stageInfo.numTasks == 1)
      stageExecution
        .get(event.stageInfo.stageId)
        .flatMap(executions.get)
        .foreach(x => x.singleTaskStages += 1)
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = synchronized {
    events.incrementAndGet()
    Option(event.taskMetrics).foreach { m =>
      stageExecution.get(event.stageId).flatMap(executions.get) match {
        case Some(x) =>
          x.tasks += 1
          x.runMs += m.executorRunTime
          x.cpuNs += m.executorCpuTime
          x.gcMs += m.jvmGCTime
          x.resultBytes += m.resultSize
          x.shuffleWriteBytes += m.shuffleWriteMetrics.bytesWritten
          x.shuffleReadBytes += m.shuffleReadMetrics.totalBytesRead
          x.spilledBytes += m.memoryBytesSpilled + m.diskBytesSpilled
          x.peakTaskMemory = math.max(x.peakTaskMemory, m.peakExecutionMemory)
        case None =>
          unattributedTasks += 1
          unattributedCpuNs += m.executorCpuTime
          unattributedRunMs += m.executorRunTime
      }
    }
  }

  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit =
    synchronized {
      events.incrementAndGet()
      event.executorUpdates.values.foreach { metrics =>
        val heap = metrics.getMetricValue("JVMHeapMemory")
        executorHeap(event.execId) = math.max(executorHeap.getOrElse(event.execId, 0L), heap)
      }
    }

  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = synchronized {
    events.incrementAndGet()
    val info = event.blockUpdatedInfo
    if (info.blockId.isRDD) {
      val key = (info.blockManagerId.executorId, info.blockId.name)
      val size = if (info.storageLevel.isValid) info.memSize + info.diskSize else 0L
      cached += size - cachedBlocks.getOrElse(key, 0L)
      if (size == 0L) cachedBlocks.remove(key) else cachedBlocks(key) = size
      cachedPeak = math.max(cachedPeak, cached)
    }
  }

  /** Waits until no event has arrived for half a second, at most a minute, so that a window is complete. */
  def settle(): Unit = {
    var quiet = 0
    var rounds = 0
    while (quiet < 2 && rounds < 240) {
      val before = events.get()
      Thread.sleep(250L)
      if (events.get() == before) quiet += 1 else quiet = 0
      rounds += 1
    }
  }

  /** Starts a window and resets its peaks; returns the window's first SQL execution identifier. */
  def mark(): Long = {
    settle()
    synchronized {
      executorHeap.clear()
      cachedPeak = cached
      unattributedTasks = 0L
      unattributedCpuNs = 0L
      unattributedRunMs = 0L
      heapPools.foreach(_.resetPeakUsage())
      heapPeak = 0L
      if (executions.isEmpty) 0L else executions.keys.max + 1L
    }
  }

  /** The window that began at execution `from`, by call site, slowest first. */
  def summary(from: Long): WindowSummary = {
    settle()
    synchronized {
      val window = executions.values.filter(_.id >= from).toVector
      val sites = window
        .groupBy(x => (x.site, x.tags.toSeq.sorted.mkString(",")))
        .toVector
        .map { case ((site, tags), xs) =>
          SiteSummary(
            site,
            tags,
            xs.size,
            xs.map(_.wallMs).sum,
            xs.map(_.runMs).sum,
            xs.map(_.cpuNs).sum / 1000000L,
            xs.map(_.gcMs).sum,
            xs.map(_.tasks).sum,
            xs.map(_.resultBytes).sum,
            xs.map(_.resultBytes).max,
            xs.map(_.shuffleWriteBytes).sum,
            xs.map(_.shuffleReadBytes).sum,
            xs.map(_.spilledBytes).sum,
            xs.map(_.peakTaskMemory).max,
            xs.map(_.singleTaskStages).sum,
            xs.head.plan
          )
        }
        .sortBy(s => (-s.wallMs, s.site))
      WindowSummary(
        sites,
        unattributedTasks,
        unattributedCpuNs / 1000000L,
        unattributedRunMs,
        heapPeak,
        heapPools.map(_.getPeakUsage.getUsed).sum,
        Runtime.getRuntime.maxMemory,
        if (executorHeap.isEmpty) 0L else executorHeap.values.max,
        cachedPeak
      )
    }
  }

  def detach(): Unit = {
    running = false
    spark.sparkContext.removeSparkListener(this)
  }
}

object Recorder {

  /** The operation a job tag names: the engine's tags start with "pprof." (from there on, whatever prefix Spark adds
    * to a session's tags), and Spark's own tags (a session's, an SQL execution's, a broadcast's) name none. Dropping
    * Spark's tags lets one call site's executions group together.
    */
  def operationTag(tag: String): Option[String] = {
    val at = tag.indexOf("pprof.")
    if (at < 0) None else Some(tag.substring(at))
  }

  def attach(spark: SparkSession): Recorder = {
    val recorder = new Recorder(spark)
    spark.sparkContext.addSparkListener(recorder)
    recorder.sampler.setDaemon(true)
    recorder.sampler.start()
    recorder
  }
}
