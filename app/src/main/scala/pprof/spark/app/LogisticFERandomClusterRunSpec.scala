package pprof.spark.app

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.logistic.{
  EffectReference,
  LogisticFERandomClusterOptions,
  LogisticFERandomClusterSpec
}

/** Where the job takes β, σ and the start (docs/spec/logistic/fe-random-cluster-job.md §4). */
sealed trait FERandomClusterSourceSpec
final case class FERandomClusterGivenSpec(beta: Seq[Double], sigma: Double, start: Seq[Double])
    extends FERandomClusterSourceSpec
final case class FERandomClusterStagesSpec(stage1: String, stage2: String)
    extends FERandomClusterSourceSpec

final case class FERandomClusterTestRequest(
    method: String,
    reference: EffectReference,
    alternative: String,
    level: Double,
    critical: Option[Double],
    nResample: Int,
    seed: Long
)

final case class FERandomClusterIntervalRequest(
    kinds: Seq[String],
    measures: Seq[String],
    method: String,
    reference: EffectReference,
    alternative: String,
    level: Double
)

final case class FERandomClusterOutputSpec(
    path: String,
    fit: Boolean,
    providers: Boolean,
    clusters: Boolean,
    summary: Option[(Double, Double, String)],
    providerTests: Seq[FERandomClusterTestRequest],
    gammaIntervals: Option[FERandomClusterIntervalRequest],
    measures: Option[(Seq[String], EffectReference)],
    measureIntervals: Option[FERandomClusterIntervalRequest],
    fitted: Boolean
)

final case class LogisticFERandomClusterRunSpec(
    input: InputSpec,
    spec: LogisticFERandomClusterSpec,
    options: LogisticFERandomClusterOptions,
    source: FERandomClusterSourceSpec,
    outputs: FERandomClusterOutputSpec
)

/** Run specifications of the random-cluster job (version 1, `model` `fe-random-cluster`), every problem at once. */
object LogisticFERandomClusterRunSpec {

  private val Jobs = Map(
    "cox" -> "CoxJob",
    "logistic" -> "LogisticJob",
    "three-stage" -> "ThreeStageJob",
    "linear" -> "LinearJob",
    "linear-re" -> "LinearREJob",
    "logistic-re" -> "LogisticREJob"
  )
  private val Methods = Seq("exact", "poibin_exact", "resampling")
  private val Alternatives = Seq("two_sided", "greater", "less")

  private def node(parent: JsonNode, field: String): Option[JsonNode] =
    Option(parent).flatMap(p => Option(p.get(field))).filterNot(_.isNull)

  def parse(json: String): LogisticFERandomClusterRunSpec = {
    val root =
      try new ObjectMapper().readTree(json)
      catch {
        case e: Exception =>
          throw new IllegalArgumentException(
            s"the run specification is not valid JSON: ${e.getMessage}"
          )
      }
    val problems = Vector.newBuilder[String]
    def unknown(parent: JsonNode, where: String, known: Set[String]): Unit =
      Option(parent)
        .filter(_.isObject)
        .foreach(_.fieldNames().asScala.filterNot(known).foreach { k =>
          problems += s"$where.$k does not belong to a fe-random-cluster specification"
        })
    def text(
        parent: JsonNode,
        field: String,
        where: String,
        required: Boolean = false
    ): Option[String] =
      node(parent, field) match {
        case Some(v) if v.isTextual => Some(v.asText())
        case Some(_)                => problems += s"$where.$field must be a string"; None
        case None                   => if (required) problems += s"$where.$field is required"; None
      }
    def number(parent: JsonNode, field: String, where: String): Option[Double] =
      node(parent, field).flatMap { v =>
        if (v.isNumber) Some(v.asDouble())
        else { problems += s"$where.$field must be a number"; None }
      }
    def integer(parent: JsonNode, field: String, where: String): Option[Long] =
      node(parent, field).flatMap { v =>
        if (v.isIntegralNumber) Some(v.asLong())
        else { problems += s"$where.$field must be an integer"; None }
      }
    def flag(parent: JsonNode, field: String, where: String): Boolean = node(parent, field).exists {
      v =>
        if (v.isBoolean) v.asBoolean()
        else { problems += s"$where.$field must be true or false"; false }
    }
    def numbers(parent: JsonNode, field: String, where: String): Seq[Double] =
      node(parent, field) match {
        case Some(v) if v.isArray && v.elements().asScala.forall(_.isNumber) =>
          v.elements().asScala.map(_.asDouble()).toSeq
        case Some(_) => problems += s"$where.$field must be a list of numbers"; Seq.empty
        case None    => problems += s"$where.$field is required"; Seq.empty
      }
    def strings(
        parent: JsonNode,
        field: String,
        where: String,
        allowed: Seq[String],
        default: Seq[String]
    ): Seq[String] =
      node(parent, field) match {
        case Some(v) if v.isArray && v.elements().asScala.forall(_.isTextual) =>
          val found = v.elements().asScala.map(_.asText()).toSeq
          found
            .filterNot(allowed.contains)
            .foreach(x => problems += s"$where.$field: unknown value $x")
          found.filter(allowed.contains)
        case Some(_) => problems += s"$where.$field must be a list of strings"; default
        case None    => default
      }
    def choice(
        parent: JsonNode,
        field: String,
        where: String,
        allowed: Seq[String],
        default: String
    ): String =
      text(parent, field, where).getOrElse(default) match {
        case v if allowed.contains(v) => v
        case v                        =>
          problems += s"$where.$field must be one of ${allowed.mkString(", ")}, got $v"; default
      }
    def level(parent: JsonNode, where: String): Double = {
      val v = number(parent, "level", where).getOrElse(0.95)
      if (!(v > 0.0 && v < 1.0)) problems += s"$where.level must lie in (0, 1), got $v"
      v
    }
    def reference(parent: JsonNode, where: String, default: EffectReference): EffectReference =
      node(parent, "reference") match {
        case None                                             => default
        case Some(v) if v.isNumber                            => EffectReference.Value(v.asDouble())
        case Some(v) if v.isTextual && v.asText() == "median" => EffectReference.Median
        case Some(v) if v.isTextual && v.asText() == "mean"   => EffectReference.Mean
        case Some(v)                                          =>
          problems += s"$where.reference must be median, mean or a number, got ${v.toString}";
          default
      }
    node(root, "version") match {
      case Some(v) if v.isInt && v.asInt() == RunSpec.Version => ()
      case Some(v)                                            =>
        problems += s"version ${v.asText()} is not supported; this release reads version ${RunSpec.Version}"
      case None => problems += "version is required"
    }
    node(root, "model") match {
      case Some(v) if v.isTextual && v.asText() == "fe-random-cluster" => ()
      case Some(v) if v.isTextual && Jobs.contains(v.asText())         =>
        problems += s"model ${v.asText()}: run this specification with pprof.spark.app.${Jobs(v.asText())}"
      case Some(v) => problems += s"model must be fe-random-cluster for this job, got ${v.toString}"
      case None    =>
        problems += "model is required; a specification without one is a Cox specification, for pprof.spark.app.CoxJob"
    }
    unknown(root, "specification", Set("version", "model", "input", "columns", "fit", "outputs"))
    val input = node(root, "input").orNull
    if (input == null) problems += "input is required"
    unknown(input, "input", Set("path", "table", "format", "options"))
    val inputSpec = InputSpec(
      text(input, "path", "input"),
      text(input, "table", "input"),
      text(input, "format", "input").getOrElse("parquet"),
      node(input, "options")
        .map(_.properties().asScala.map(e => e.getKey -> e.getValue.asText()).toMap)
        .getOrElse(Map.empty)
    )
    if (input != null && inputSpec.path.isDefined == inputSpec.table.isDefined)
      problems += "input needs exactly one of path and table"
    val columns = node(root, "columns").orNull
    if (columns == null) problems += "columns is required"
    unknown(
      columns,
      "columns",
      Set("outcome", "features", "provider", "cluster", "observed", "rowId")
    )
    val spec = LogisticFERandomClusterSpec(
      text(columns, "outcome", "columns", required = columns != null).getOrElse(""),
      node(columns, "features")
        .map(_.elements().asScala.map(_.asText()).toSeq)
        .getOrElse(Seq.empty),
      text(columns, "provider", "columns", required = columns != null).getOrElse(""),
      text(columns, "cluster", "columns", required = columns != null).getOrElse(""),
      text(columns, "observed", "columns"),
      text(columns, "rowId", "columns")
    )
    val fit = node(root, "fit").orNull
    unknown(
      fit,
      "fit",
      Set(
        "nNodes",
        "maxIter",
        "tol",
        "bound",
        "boundMode",
        "estimator",
        "convergenceCriterion",
        "path",
        "blocks",
        "given",
        "stages"
      )
    )
    val d = LogisticFERandomClusterOptions()
    val b = BlockOptions()
    val blocks = node(fit, "blocks").orNull
    unknown(
      blocks,
      "fit.blocks",
      Set("targetBlockBytes", "maxGroupsOnDriver", "driverBudgetBytes", "storageLevel")
    )
    val options =
      try
        LogisticFERandomClusterOptions(
          integer(fit, "nNodes", "fit").map(_.toInt).getOrElse(d.nNodes),
          integer(fit, "maxIter", "fit").map(_.toInt).getOrElse(d.maxIter),
          number(fit, "tol", "fit").getOrElse(d.tol),
          number(fit, "bound", "fit").getOrElse(d.bound),
          choice(fit, "boundMode", "fit", Seq("relative", "absolute"), d.boundMode),
          text(fit, "estimator", "fit").getOrElse(d.estimator),
          BlockOptions(
            integer(blocks, "targetBlockBytes", "fit.blocks").getOrElse(b.targetBlockBytes),
            integer(blocks, "maxGroupsOnDriver", "fit.blocks")
              .map(_.toInt)
              .getOrElse(b.maxGroupsOnDriver),
            integer(blocks, "driverBudgetBytes", "fit.blocks").getOrElse(b.driverBudgetBytes),
            text(blocks, "storageLevel", "fit.blocks").getOrElse(b.storageLevel)
          ),
          text(fit, "path", "fit").getOrElse(d.path),
          text(fit, "convergenceCriterion", "fit").getOrElse(d.convergenceCriterion)
        )
      catch {
        case e: IllegalArgumentException =>
          problems += s"fit: ${e.getMessage.stripPrefix("requirement failed: ")}"
          d
      }
    val givenNode = node(fit, "given")
    val stages = node(fit, "stages")
    val source: FERandomClusterSourceSpec = (givenNode, stages) match {
      case (Some(g), None) =>
        unknown(g, "fit.given", Set("beta", "sigma", "start"))
        val sigma = number(g, "sigma", "fit.given")
        if (sigma.isEmpty) problems += "fit.given.sigma is required"
        FERandomClusterGivenSpec(
          numbers(g, "beta", "fit.given"),
          sigma.getOrElse(0.0),
          numbers(g, "start", "fit.given")
        )
      case (None, Some(s)) =>
        unknown(s, "fit.stages", Set("stage1", "stage2"))
        FERandomClusterStagesSpec(
          text(s, "stage1", "fit.stages", required = true).getOrElse(""),
          text(s, "stage2", "fit.stages", required = true).getOrElse("")
        )
      case _ =>
        problems += "fit needs exactly one source: given (beta, sigma, start) or stages (stage1, stage2)"
        FERandomClusterGivenSpec(Seq.empty, 0.0, Seq.empty)
    }
    val outputs = node(root, "outputs").orNull
    if (outputs == null) problems += "outputs is required"
    unknown(
      outputs,
      "outputs",
      Set(
        "path",
        "fit",
        "providers",
        "clusters",
        "summary",
        "providerTests",
        "gammaIntervals",
        "measures",
        "measureIntervals",
        "fitted"
      )
    )
    val summary = node(outputs, "summary").map { n =>
      unknown(n, "outputs.summary", Set("level", "null", "alternative"))
      if (stages.isEmpty || givenNode.nonEmpty)
        problems += "outputs.summary needs the stages source: it is stage 1's Wald table"
      (
        level(n, "outputs.summary"),
        number(n, "null", "outputs.summary").getOrElse(0.0),
        choice(n, "alternative", "outputs.summary", Alternatives, "two_sided")
      )
    }
    val tests = node(outputs, "providerTests").toSeq.flatMap { t =>
      val items = if (t.isArray) t.elements().asScala.toSeq else Seq(t)
      items.zipWithIndex.map { case (item, i) =>
        val where = if (t.isArray) s"outputs.providerTests[$i]" else "outputs.providerTests"
        unknown(
          item,
          where,
          Set("method", "reference", "alternative", "level", "critical", "nResample", "seed")
        )
        val critical = number(item, "critical", where)
        critical
          .filterNot(_ > 0.0)
          .foreach(c => problems += s"$where.critical must be positive, got $c")
        FERandomClusterTestRequest(
          choice(item, "method", where, Methods, "exact"),
          reference(item, where, EffectReference.Median),
          choice(item, "alternative", where, Alternatives, "two_sided"),
          level(item, where),
          critical,
          integer(item, "nResample", where).map(_.toInt).getOrElse(10000),
          integer(item, "seed", where).getOrElse(0L)
        )
      }
    }
    val methods = tests.map(_.method)
    methods
      .diff(methods.distinct)
      .distinct
      .foreach(m => problems += s"outputs.providerTests lists method $m twice")
    def intervalRequest(name: String, kinds: Boolean): Option[FERandomClusterIntervalRequest] =
      node(outputs, name).map { n =>
        val where = s"outputs.$name"
        unknown(
          n,
          where,
          (Set("method", "reference", "alternative", "level") ++ (if (kinds)
                                                                    Set("kinds", "measures")
                                                                  else Set.empty))
        )
        FERandomClusterIntervalRequest(
          if (kinds) strings(n, "kinds", where, Seq("indirect", "direct"), Seq("indirect"))
          else Seq.empty,
          if (kinds) strings(n, "measures", where, Seq("ratio", "rate"), Seq("rate", "ratio"))
          else Seq.empty,
          choice(n, "method", where, Seq("exact", "poibin_exact"), "exact"),
          reference(n, where, EffectReference.Median),
          choice(n, "alternative", where, Alternatives, "two_sided"),
          level(n, where)
        )
      }
    val measures = node(outputs, "measures").map { n =>
      unknown(n, "outputs.measures", Set("kinds", "reference"))
      (
        strings(n, "kinds", "outputs.measures", Seq("indirect", "direct"), Seq("indirect")),
        reference(n, "outputs.measures", EffectReference.Median)
      )
    }
    val outputSpec = FERandomClusterOutputSpec(
      text(outputs, "path", "outputs", required = outputs != null).getOrElse(""),
      flag(outputs, "fit", "outputs"),
      flag(outputs, "providers", "outputs"),
      flag(outputs, "clusters", "outputs"),
      summary,
      tests,
      intervalRequest("gammaIntervals", kinds = false),
      measures,
      intervalRequest("measureIntervals", kinds = true),
      flag(outputs, "fitted", "outputs")
    )
    val found = problems.result()
    if (found.nonEmpty)
      throw new IllegalArgumentException(
        s"the run specification has problems: ${found.mkString("; ")}"
      )
    LogisticFERandomClusterRunSpec(inputSpec, spec, options, source, outputSpec)
  }
}
