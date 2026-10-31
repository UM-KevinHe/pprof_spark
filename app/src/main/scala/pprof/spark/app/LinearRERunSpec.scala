package pprof.spark.app

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.linear.{LinearREInference, LinearREOptions, LinearRESpec}
import pprof.spark.engine.logistic.EffectReference

/** What the linear random-effect job writes under `path` (docs/spec/linear/random-effect-job.md §4). `predictions`
  * holds `useRe` when predictions are requested.
  */
final case class LinearREOutputSpec(
    path: String,
    fit: Boolean,
    providers: Boolean,
    summary: Option[LinearSummaryRequest],
    fixedEffects: Boolean,
    confInt: Option[Double],
    providerTests: Option[LinearTestsRequest],
    alphaIntervals: Option[Double],
    measures: Option[LinearMeasuresRequest],
    measureIntervals: Option[LinearMeasureIntervalsRequest],
    predictions: Option[Boolean],
    fitted: Boolean
)

final case class LinearRERunSpec(
    input: InputSpec,
    spec: LinearRESpec,
    options: LinearREOptions,
    outputs: LinearREOutputSpec
)

/** Parses linear random-effect run specifications (version 1, `model` `linear-re`,
  * docs/spec/linear/random-effect-job.md §3); every problem is reported at once.
  */
object LinearRERunSpec {

  def parse(json: String): LinearRERunSpec = {
    val root =
      try new ObjectMapper().readTree(json)
      catch {
        case e: Exception =>
          throw new IllegalArgumentException(
            s"the run specification is not valid JSON: ${e.getMessage}"
          )
      }
    val problems = Vector.newBuilder[String]
    val (spec, options) = model(root, problems)
    node(root, "version") match {
      case Some(v) if v.isInt && v.asInt() == RunSpec.Version => ()
      case Some(v)                                            =>
        problems += s"version ${v.asText()} is not supported; this release reads version ${RunSpec.Version}"
      case None => problems += "version is required"
    }
    node(root, "model") match {
      case Some(v) if v.isTextual && v.asText() == "linear-re"   => ()
      case Some(v) if v.isTextual && v.asText() == "logistic-re" =>
        problems += "model logistic-re: run this specification with pprof.spark.app.LogisticREJob"
      case Some(v) if v.isTextual && v.asText() == "linear" =>
        problems += "model linear: run this specification with pprof.spark.app.LinearJob"
      case Some(v) if v.isTextual && v.asText() == "logistic" =>
        problems += "model logistic: run this specification with pprof.spark.app.LogisticJob"
      case Some(v) if v.isTextual && v.asText() == "three-stage" =>
        problems += "model three-stage: run this specification with pprof.spark.app.ThreeStageJob"
      case Some(v) if v.isTextual && v.asText() == "cox" =>
        problems += "model cox: run this specification with pprof.spark.app.CoxJob"
      case Some(v) => problems += s"model must be linear-re for this job, got ${v.toString}"
      case None    =>
        problems += "model is required; a specification without one is a Cox specification, for pprof.spark.app.CoxJob"
    }
    val p = new Parser(problems)
    p.unknown(root, "specification", Set("version", "model", "input", "columns", "fit", "outputs"))
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
        "fixedEffects",
        "confInt",
        "providerTests",
        "alphaIntervals",
        "measures",
        "measureIntervals",
        "predictions",
        "fitted"
      )
    )
    val summary = node(outputs, "summary").map { s =>
      p.unknown(s, "outputs.summary", Set("level", "null", "alternative"))
      LinearSummaryRequest(
        p.level(s, "level", "outputs.summary"),
        p.number(s, "null", "outputs.summary").getOrElse(0.0),
        p.choice(s, "alternative", "outputs.summary", LinearREInference.Alternatives, "two_sided")
      )
    }
    val confInt = node(outputs, "confInt").map { c =>
      p.unknown(c, "outputs.confInt", Set("level"))
      p.level(c, "level", "outputs.confInt")
    }
    val providerTests = node(outputs, "providerTests").map { t =>
      p.unknown(t, "outputs.providerTests", Set("reference", "alternative", "level", "critical"))
      val critical = p.number(t, "critical", "outputs.providerTests")
      critical
        .filterNot(_ > 0.0)
        .foreach(c => problems += s"outputs.providerTests.critical must be positive, got $c")
      LinearTestsRequest(
        p.reference(t, "outputs.providerTests", EffectReference.Value(0.0)),
        p.choice(
          t,
          "alternative",
          "outputs.providerTests",
          LinearREInference.Alternatives,
          "two_sided"
        ),
        p.level(t, "level", "outputs.providerTests"),
        critical
      )
    }
    val alphaIntervals = node(outputs, "alphaIntervals").map { a =>
      p.unknown(a, "outputs.alphaIntervals", Set("level"))
      p.level(a, "level", "outputs.alphaIntervals")
    }
    val measures = node(outputs, "measures").map { m =>
      p.unknown(m, "outputs.measures", Set("kinds", "reference"))
      LinearMeasuresRequest(
        p.kinds(m, "outputs.measures", LinearREInference.Kinds),
        p.reference(m, "outputs.measures", EffectReference.Median)
      )
    }
    val measureIntervals = node(outputs, "measureIntervals").map { m =>
      p.unknown(m, "outputs.measureIntervals", Set("kinds", "reference", "alternative", "level"))
      LinearMeasureIntervalsRequest(
        p.kinds(m, "outputs.measureIntervals", Seq("indirect")),
        p.reference(m, "outputs.measureIntervals", EffectReference.Median),
        p.choice(
          m,
          "alternative",
          "outputs.measureIntervals",
          LinearREInference.Alternatives,
          "two_sided"
        ),
        p.level(m, "level", "outputs.measureIntervals")
      )
    }
    val predictions = node(outputs, "predictions").flatMap {
      case v if v.isBoolean => if (v.asBoolean()) Some(false) else None
      case v if v.isObject  =>
        p.unknown(v, "outputs.predictions", Set("useRe"))
        Some(p.flag(v, "useRe", "outputs.predictions", default = false))
      case _ =>
        problems += "outputs.predictions must be true, false or an object with useRe"
        None
    }
    val outputSpec = LinearREOutputSpec(
      p.text(outputs, "path", "outputs", required = true).getOrElse(""),
      p.flag(outputs, "fit", "outputs", default = false),
      p.flag(outputs, "providers", "outputs", default = false),
      summary,
      p.flag(outputs, "fixedEffects", "outputs", default = false),
      confInt,
      providerTests,
      alphaIntervals,
      measures,
      measureIntervals,
      predictions,
      p.flag(outputs, "fitted", "outputs", default = false)
    )
    val found = problems.result()
    if (found.nonEmpty)
      throw new IllegalArgumentException(
        s"the run specification has problems: ${found.mkString("; ")}"
      )
    LinearRERunSpec(inputSpec, spec, options, outputSpec)
  }

  /** `columns` and `fit` alone, as the Python facade takes them (spec §6); every problem at once. */
  def model(columnsJson: String, fitJson: String): (LinearRESpec, LinearREOptions) = {
    val mapper = new ObjectMapper()
    def read(json: String, what: String): JsonNode =
      try mapper.readTree(if (json == null || json.isEmpty) "{}" else json)
      catch {
        case e: Exception =>
          throw new IllegalArgumentException(s"$what is not valid JSON: ${e.getMessage}")
      }
    val root = mapper.createObjectNode()
    root.set[JsonNode]("columns", read(columnsJson, "columns"))
    root.set[JsonNode]("fit", read(fitJson, "fit"))
    val problems = Vector.newBuilder[String]
    val result = model(root, problems)
    val found = problems.result()
    if (found.nonEmpty)
      throw new IllegalArgumentException(s"the model has problems: ${found.mkString("; ")}")
    result
  }

  private def node(parent: JsonNode, field: String): Option[JsonNode] =
    Option(parent).flatMap(p => Option(p.get(field))).filterNot(_.isNull)

  /** `columns` and `fit` of a specification. */
  private def model(
      root: JsonNode,
      problems: scala.collection.mutable.Builder[String, Vector[String]]
  ): (LinearRESpec, LinearREOptions) = {
    val p = new Parser(problems)
    val columns = node(root, "columns").orNull
    if (columns == null) problems += "columns is required"
    p.unknown(
      columns,
      "columns",
      Set("outcome", "features", "provider", "offset", "weights", "rowId")
    )
    val spec = LinearRESpec(
      p.text(columns, "outcome", "columns", required = true).getOrElse(""),
      p.strings(columns, "features", "columns").getOrElse(Seq.empty),
      p.text(columns, "provider", "columns", required = true).getOrElse(""),
      p.text(columns, "offset", "columns"),
      p.text(columns, "weights", "columns"),
      p.text(columns, "rowId", "columns")
    )
    val fit = node(root, "fit").orNull
    p.unknown(fit, "fit", Set("reml", "includeIntercept", "thetaUpper", "maxEvaluations", "blocks"))
    val blocks = node(fit, "blocks").orNull
    p.unknown(
      blocks,
      "fit.blocks",
      Set("targetBlockBytes", "maxGroupsOnDriver", "driverBudgetBytes", "storageLevel")
    )
    val b = BlockOptions()
    val d = LinearREOptions()
    val options =
      try
        LinearREOptions(
          p.flag(fit, "reml", "fit", default = true),
          p.flag(fit, "includeIntercept", "fit", default = true),
          p.number(fit, "thetaUpper", "fit").getOrElse(Double.PositiveInfinity),
          p.integer(fit, "maxEvaluations", "fit").map(_.toInt).getOrElse(d.maxEvaluations),
          BlockOptions(
            p.integer(blocks, "targetBlockBytes", "fit.blocks").getOrElse(b.targetBlockBytes),
            p.integer(blocks, "maxGroupsOnDriver", "fit.blocks")
              .map(_.toInt)
              .getOrElse(b.maxGroupsOnDriver),
            p.integer(blocks, "driverBudgetBytes", "fit.blocks").getOrElse(b.driverBudgetBytes),
            p.text(blocks, "storageLevel", "fit.blocks").getOrElse(b.storageLevel)
          )
        )
      catch {
        case e: IllegalArgumentException =>
          problems += s"fit: ${e.getMessage.stripPrefix("requirement failed: ")}"
          d
      }
    (spec, options)
  }

  /** The field readers, each adding its problem rather than failing. */
  private final class Parser(problems: scala.collection.mutable.Builder[String, Vector[String]]) {

    def unknown(parent: JsonNode, where: String, known: Set[String]): Unit =
      Option(parent).filter(_.isObject).foreach { p =>
        p.properties()
          .asScala
          .map(_.getKey)
          .filterNot(known)
          .foreach(k => problems += s"$where.$k does not belong to a linear-re specification")
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

    def level(parent: JsonNode, field: String, where: String): Double = {
      val value = number(parent, field, where).getOrElse(0.95)
      if (!(value > 0.0 && value < 1.0)) problems += s"$where.$field must lie in (0, 1), got $value"
      value
    }

    def kinds(parent: JsonNode, where: String, default: Seq[String]): Seq[String] = {
      val found = strings(parent, "kinds", where).getOrElse(default)
      found
        .filterNot(LinearREInference.Kinds.contains)
        .foreach(k => problems += s"$where.kinds: unknown kind $k")
      found.filter(LinearREInference.Kinds.contains)
    }
  }
}
