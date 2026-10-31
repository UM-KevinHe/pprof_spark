package pprof.spark.app

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.logistic.{
  EffectReference,
  LogisticREInference,
  LogisticREOptions,
  LogisticRESpec
}

/** A provider test of the logistic random-effect job (docs/spec/logistic/random-effect-job.md §3). */
final case class LogisticRETestRequest(
    method: String,
    reference: EffectReference,
    alternative: String,
    level: Double,
    critical: Option[Double],
    nResample: Int,
    seed: Long
)

final case class LogisticREMeasuresRequest(kinds: Seq[String], reference: EffectReference)

final case class LogisticREIntervalsRequest(
    kinds: Seq[String],
    measures: Seq[String],
    reference: EffectReference,
    alternative: String,
    level: Double
)

/** What the job writes under `path` (§4); `predictions` holds `useRe` and `type` when requested. */
final case class LogisticREOutputSpec(
    path: String,
    fit: Boolean,
    providers: Boolean,
    summary: Option[Double],
    providerTests: Seq[LogisticRETestRequest],
    alphaIntervals: Option[Double],
    measures: Option[LogisticREMeasuresRequest],
    measureIntervals: Option[LogisticREIntervalsRequest],
    profile: Option[Double],
    predictions: Option[(Boolean, String)],
    fitted: Boolean
)

final case class LogisticRERunSpec(
    input: InputSpec,
    spec: LogisticRESpec,
    options: LogisticREOptions,
    outputs: LogisticREOutputSpec
)

/** The logistic random-intercept model as JSON (spec 4b-1 §15, spec 4b-3 §3): `model` reads `columns` and `fit` for
  * the Python facade, and `parse` whole run specifications (version 1, `model` `logistic-re`) for `LogisticREJob`;
  * every problem is reported at once.
  */
object LogisticRERunSpec {

  def model(columnsJson: String, fitJson: String): (LogisticRESpec, LogisticREOptions) = {
    val mapper = new ObjectMapper()
    def read(json: String, what: String): JsonNode =
      try mapper.readTree(if (json == null || json.isEmpty) "{}" else json)
      catch {
        case e: Exception =>
          throw new IllegalArgumentException(s"$what is not valid JSON: ${e.getMessage}")
      }
    val problems = Vector.newBuilder[String]
    val result =
      modelFrom(read(columnsJson, "columns"), read(fitJson, "fit"), new Parser(problems, "model"))
    val found = problems.result()
    if (found.nonEmpty)
      throw new IllegalArgumentException(s"the model has problems: ${found.mkString("; ")}")
    result
  }

  def parse(json: String): LogisticRERunSpec = {
    val root =
      try new ObjectMapper().readTree(json)
      catch {
        case e: Exception =>
          throw new IllegalArgumentException(
            s"the run specification is not valid JSON: ${e.getMessage}"
          )
      }
    val problems = Vector.newBuilder[String]
    val p = new Parser(problems, "specification")
    node(root, "version") match {
      case Some(v) if v.isInt && v.asInt() == RunSpec.Version => ()
      case Some(v)                                            =>
        problems += s"version ${v.asText()} is not supported; this release reads version ${RunSpec.Version}"
      case None => problems += "version is required"
    }
    node(root, "model") match {
      case Some(v) if v.isTextual && v.asText() == "logistic-re" => ()
      case Some(v) if v.isTextual && Jobs.contains(v.asText())   =>
        problems += s"model ${v.asText()}: run this specification with pprof.spark.app.${Jobs(v.asText())}"
      case Some(v) => problems += s"model must be logistic-re for this job, got ${v.toString}"
      case None    =>
        problems += "model is required; a specification without one is a Cox specification, for pprof.spark.app.CoxJob"
    }
    p.unknown(root, "specification", Set("version", "model", "input", "columns", "fit", "outputs"))
    val (spec, options) = modelFrom(node(root, "columns").orNull, node(root, "fit").orNull, p)
    if (node(root, "columns").isEmpty) problems += "columns is required"
    val input = node(root, "input").orNull
    if (input == null) problems += "input is required"
    p.unknown(input, "input", Set("path", "table", "format", "options"))
    val inputSpec = InputSpec(
      p.text(input, "path", "input"),
      p.text(input, "table", "input"),
      p.text(input, "format", "input").getOrElse("parquet"),
      node(input, "options")
        .map(o => o.properties().asScala.map(e => e.getKey -> e.getValue.asText()).toMap)
        .getOrElse(Map.empty)
    )
    if (input != null && inputSpec.path.isDefined == inputSpec.table.isDefined)
      problems += "input needs exactly one of path and table"
    val outputs = node(root, "outputs").orNull
    if (outputs == null) problems += "outputs is required"
    p.unknown(
      outputs,
      "outputs",
      Set(
        "path",
        "fit",
        "providers",
        "summary",
        "providerTests",
        "alphaIntervals",
        "measures",
        "measureIntervals",
        "profile",
        "predictions",
        "fitted"
      )
    )
    def levelOf(name: String): Option[Double] = node(outputs, name).map { n =>
      p.unknown(n, s"outputs.$name", Set("level"))
      p.level(n, "level", s"outputs.$name")
    }
    val tests = node(outputs, "providerTests").toSeq.flatMap { t =>
      val items = if (t.isArray) t.elements().asScala.toSeq else Seq(t)
      items.zipWithIndex.map { case (item, i) =>
        val where = if (t.isArray) s"outputs.providerTests[$i]" else "outputs.providerTests"
        p.unknown(
          item,
          where,
          Set("method", "reference", "alternative", "level", "critical", "nResample", "seed")
        )
        if (node(item, "nullModel").isDefined) problems += s"$where.nullModel arrives with OI-87"
        val method = p.text(item, "method", where).getOrElse("wald")
        if (method == "exact")
          problems += s"$where.method exact needs a cluster factor (part 4d, OI-93)"
        else if (!LogisticREInference.Methods.contains(method))
          problems += s"$where.method must be one of ${LogisticREInference.Methods.mkString(", ")}, got $method"
        val critical = p.number(item, "critical", where)
        critical
          .filterNot(_ > 0.0)
          .foreach(c => problems += s"$where.critical must be positive, got $c")
        val nResample = p.integer(item, "nResample", where).map(_.toInt).getOrElse(10000)
        if (nResample <= 0) problems += s"$where.nResample must be positive, got $nResample"
        LogisticRETestRequest(
          method,
          p.reference(item, where, EffectReference.Value(0.0)),
          p.choice(item, "alternative", where, LogisticREInference.Alternatives, "two_sided"),
          p.level(item, "level", where),
          critical,
          nResample,
          p.integer(item, "seed", where).getOrElse(0L)
        )
      }
    }
    val methods = tests.map(_.method)
    methods
      .diff(methods.distinct)
      .distinct
      .foreach(m => problems += s"outputs.providerTests lists method $m twice")
    val measures = node(outputs, "measures").map { m =>
      p.unknown(m, "outputs.measures", Set("kinds", "reference"))
      LogisticREMeasuresRequest(
        p.list(m, "kinds", "outputs.measures", LogisticREInference.Kinds, Seq("indirect")),
        p.reference(m, "outputs.measures", EffectReference.Median)
      )
    }
    val intervals = node(outputs, "measureIntervals").map { m =>
      val where = "outputs.measureIntervals"
      p.unknown(m, where, Set("kinds", "measures", "reference", "alternative", "level"))
      LogisticREIntervalsRequest(
        p.list(m, "kinds", where, LogisticREInference.Kinds, Seq("indirect")),
        p.list(m, "measures", where, LogisticREInference.Measures, Seq("rate", "ratio")),
        p.reference(m, where, EffectReference.Median),
        p.choice(m, "alternative", where, LogisticREInference.Alternatives, "two_sided"),
        p.level(m, "level", where)
      )
    }
    val profile = levelOf("profile")
    if (profile.nonEmpty && !options.nagq1)
      problems += "outputs.profile needs the Laplace fit (fit.stage2 true), as pprof_py's profile_sigma"
    val predictions = node(outputs, "predictions").flatMap {
      case v if v.isBoolean => if (v.asBoolean()) Some((false, "response")) else None
      case v if v.isObject  =>
        p.unknown(v, "outputs.predictions", Set("useRe", "type"))
        Some(
          (
            p.flag(v, "useRe", "outputs.predictions", default = false),
            p.choice(v, "type", "outputs.predictions", Seq("response", "link"), "response")
          )
        )
      case _ =>
        problems += "outputs.predictions must be true, false or an object with useRe and type"
        None
    }
    val outputSpec = LogisticREOutputSpec(
      p.text(outputs, "path", "outputs", required = true).getOrElse(""),
      p.flag(outputs, "fit", "outputs", default = false),
      p.flag(outputs, "providers", "outputs", default = false),
      levelOf("summary"),
      tests,
      levelOf("alphaIntervals"),
      measures,
      intervals,
      profile,
      predictions,
      p.flag(outputs, "fitted", "outputs", default = false)
    )
    val found = problems.result()
    if (found.nonEmpty)
      throw new IllegalArgumentException(
        s"the run specification has problems: ${found.mkString("; ")}"
      )
    LogisticRERunSpec(inputSpec, spec, options, outputSpec)
  }

  /** The other models' jobs, which a logistic-re job names when given their specification. */
  private val Jobs = Map(
    "cox" -> "CoxJob",
    "logistic" -> "LogisticJob",
    "three-stage" -> "ThreeStageJob",
    "linear" -> "LinearJob",
    "linear-re" -> "LinearREJob"
  )

  private def node(parent: JsonNode, field: String): Option[JsonNode] =
    Option(parent).flatMap(p => Option(p.get(field))).filterNot(_.isNull)

  private def modelFrom(
      columns: JsonNode,
      fit: JsonNode,
      p: Parser
  ): (LogisticRESpec, LogisticREOptions) = {
    p.unknown(columns, "columns", Set("outcome", "features", "provider", "offset", "rowId"))
    val spec = LogisticRESpec(
      p.text(columns, "outcome", "columns", required = true).getOrElse(""),
      p.strings(columns, "features", "columns").getOrElse(Seq.empty),
      p.text(columns, "provider", "columns", required = true).getOrElse(""),
      p.text(columns, "offset", "columns"),
      p.text(columns, "rowId", "columns")
    )
    p.unknown(
      fit,
      "fit",
      Set("stage2", "includeIntercept", "sigmaUpper", "maxIterations", "blocks")
    )
    val blocks = node(fit, "blocks").orNull
    p.unknown(
      blocks,
      "fit.blocks",
      Set("targetBlockBytes", "maxGroupsOnDriver", "driverBudgetBytes", "storageLevel")
    )
    val b = BlockOptions()
    val d = LogisticREOptions()
    val stage2 = p.flag(fit, "stage2", "fit", default = true)
    val includeIntercept = p.flag(fit, "includeIntercept", "fit", default = true)
    val sigmaUpper = p.number(fit, "sigmaUpper", "fit").getOrElse(Double.PositiveInfinity)
    val maxIterations =
      p.integer(fit, "maxIterations", "fit").map(_.toInt).getOrElse(d.maxIterations)
    val targetBlockBytes =
      p.integer(blocks, "targetBlockBytes", "fit.blocks").getOrElse(b.targetBlockBytes)
    val maxGroupsOnDriver = p
      .integer(blocks, "maxGroupsOnDriver", "fit.blocks")
      .map(_.toInt)
      .getOrElse(b.maxGroupsOnDriver)
    val driverBudgetBytes =
      p.integer(blocks, "driverBudgetBytes", "fit.blocks").getOrElse(b.driverBudgetBytes)
    val storageLevel = p.text(blocks, "storageLevel", "fit.blocks").getOrElse(b.storageLevel)
    val options =
      try
        LogisticREOptions(
          stage2,
          includeIntercept,
          sigmaUpper,
          maxIterations,
          BlockOptions(targetBlockBytes, maxGroupsOnDriver, driverBudgetBytes, storageLevel)
        )
      catch {
        case e: IllegalArgumentException =>
          p.problem(s"fit: ${e.getMessage.stripPrefix("requirement failed: ")}")
          d.copy(nagq1 = stage2) // keeps `stage2` for the checks that depend on it (the profile)
      }
    (spec, options)
  }

  /** The field readers, each adding its problem rather than failing; `noun` names what fields belong to. */
  private final class Parser(
      problems: scala.collection.mutable.Builder[String, Vector[String]],
      noun: String
  ) {

    def problem(text: String): Unit = problems += text

    def unknown(parent: JsonNode, where: String, known: Set[String]): Unit =
      Option(parent).filter(_.isObject).foreach { p =>
        p.properties()
          .asScala
          .map(_.getKey)
          .filterNot(known)
          .foreach(k => problems += s"$where.$k does not belong to a logistic-re $noun")
      }

    def text(
        parent: JsonNode,
        field: String,
        where: String,
        required: Boolean = false
    ): Option[String] =
      node(parent, field) match {
        case Some(v) if v.isTextual => Some(v.asText())
        case Some(_)                =>
          problems += s"$where.$field must be a string"
          None
        case None =>
          if (required) problems += s"$where.$field is required"
          None
      }

    def number(parent: JsonNode, field: String, where: String): Option[Double] =
      node(parent, field).flatMap { v =>
        if (v.isNumber) Some(v.asDouble())
        else {
          problems += s"$where.$field must be a number"
          None
        }
      }

    def integer(parent: JsonNode, field: String, where: String): Option[Long] =
      node(parent, field).flatMap { v =>
        if (v.isIntegralNumber) Some(v.asLong())
        else {
          problems += s"$where.$field must be an integer"
          None
        }
      }

    def flag(parent: JsonNode, field: String, where: String, default: Boolean): Boolean =
      node(parent, field).fold(default) { v =>
        if (v.isBoolean) v.asBoolean()
        else {
          problems += s"$where.$field must be true or false"
          default
        }
      }

    def strings(parent: JsonNode, field: String, where: String): Option[Seq[String]] =
      node(parent, field).map {
        case v if v.isArray && v.elements().asScala.forall(_.isTextual) =>
          v.elements().asScala.map(_.asText()).toSeq
        case _ =>
          problems += s"$where.$field must be a list of strings"
          Seq.empty
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
          problems += s"$where.$field must be one of ${allowed.mkString(", ")}, got $v"
          default
      }

    def list(
        parent: JsonNode,
        field: String,
        where: String,
        allowed: Seq[String],
        default: Seq[String]
    ): Seq[String] = {
      val found = strings(parent, field, where).getOrElse(default)
      found.filterNot(allowed.contains).foreach(k => problems += s"$where.$field: unknown value $k")
      found.filter(allowed.contains)
    }

    def level(parent: JsonNode, field: String, where: String): Double = {
      val value = number(parent, field, where).getOrElse(0.95)
      if (!(value > 0.0 && value < 1.0)) problems += s"$where.$field must lie in (0, 1), got $value"
      value
    }

    def reference(parent: JsonNode, where: String, default: EffectReference): EffectReference =
      node(parent, "reference") match {
        case None                                           => default
        case Some(v) if v.isNumber                          => EffectReference.Value(v.asDouble())
        case Some(v) if v.isTextual && v.asText == "median" => EffectReference.Median
        case Some(v) if v.isTextual && v.asText == "mean"   => EffectReference.Mean
        case Some(v)                                        =>
          problems += s"$where.reference must be median, mean or a number, got ${v.toString}"
          default
      }
  }
}
