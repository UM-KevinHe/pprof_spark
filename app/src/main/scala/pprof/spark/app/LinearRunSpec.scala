package pprof.spark.app

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.linear.{
  LinearFE,
  LinearOptions,
  LinearProviderTests,
  LinearSpec,
  LinearStandardization
}
import pprof.spark.engine.logistic.EffectReference

final case class LinearSummaryRequest(level: Double, nullValue: Double, alternative: String)

final case class LinearTestsRequest(
    reference: EffectReference,
    alternative: String,
    level: Double,
    critical: Option[Double]
)

final case class LinearMeasuresRequest(kinds: Seq[String], reference: EffectReference)

final case class LinearMeasureIntervalsRequest(
    kinds: Seq[String],
    reference: EffectReference,
    alternative: String,
    level: Double
)

/** What the linear job writes under `path` (docs/spec/linear/job-and-python.md §3). */
final case class LinearOutputSpec(
    path: String,
    fit: Boolean,
    providers: Boolean,
    summary: Option[LinearSummaryRequest],
    providerTests: Option[LinearTestsRequest],
    effectIntervals: Option[Double],
    measures: Option[LinearMeasuresRequest],
    measureIntervals: Option[LinearMeasureIntervalsRequest],
    predictions: Boolean
)

final case class LinearRunSpec(
    input: InputSpec,
    spec: LinearSpec,
    options: LinearOptions,
    outputs: LinearOutputSpec
)

/** Parses linear run specifications (version 1, `model` `linear`, docs/spec/linear/job-and-python.md §2); every
  * problem is reported at once.
  */
object LinearRunSpec {

  def parse(json: String): LinearRunSpec = {
    val root =
      try new ObjectMapper().readTree(json)
      catch {
        case e: Exception =>
          throw new IllegalArgumentException(
            s"the run specification is not valid JSON: ${e.getMessage}"
          )
      }
    val problems = Vector.newBuilder[String]
    def node(parent: JsonNode, field: String): Option[JsonNode] =
      Option(parent).flatMap(p => Option(p.get(field))).filterNot(_.isNull)
    def unknown(parent: JsonNode, where: String, known: Set[String]): Unit =
      Option(parent).filter(_.isObject).foreach { p =>
        p.fieldNames()
          .asScala
          .filterNot(known)
          .foreach(k => problems += s"$where.$k does not belong to a linear specification")
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
    def flag(parent: JsonNode, field: String, where: String): Boolean = node(parent, field).exists {
      v =>
        if (v.isBoolean) v.asBoolean()
        else {
          problems += s"$where.$field must be true or false"
          false
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
    def reference(parent: JsonNode, where: String): EffectReference =
      node(parent, "reference") match {
        case None                                           => EffectReference.Median
        case Some(v) if v.isNumber                          => EffectReference.Value(v.asDouble())
        case Some(v) if v.isTextual && v.asText == "median" => EffectReference.Median
        case Some(v) if v.isTextual && v.asText == "mean"   => EffectReference.Mean
        case Some(v)                                        =>
          problems += s"$where.reference must be median, mean or a number, got ${v.toString}"
          EffectReference.Median
      }
    def level(parent: JsonNode, field: String, where: String): Double = {
      val value = number(parent, field, where).getOrElse(0.95)
      if (!(value > 0.0 && value < 1.0)) problems += s"$where.$field must lie in (0, 1), got $value"
      value
    }
    def kinds(parent: JsonNode, where: String, default: Seq[String]): Seq[String] = {
      val found = strings(parent, "kinds", where).getOrElse(default)
      found
        .filterNot(LinearStandardization.Kinds.contains)
        .foreach(k => problems += s"$where.kinds: unknown kind $k")
      found.filter(LinearStandardization.Kinds.contains)
    }

    node(root, "version") match {
      case Some(v) if v.isInt && v.asInt() == RunSpec.Version => ()
      case Some(v)                                            =>
        problems += s"version ${v.asText()} is not supported; this release reads version ${RunSpec.Version}"
      case None => problems += "version is required"
    }
    node(root, "model") match {
      case Some(v) if v.isTextual && v.asText() == "linear"    => ()
      case Some(v) if v.isTextual && v.asText() == "linear-re" =>
        problems += "model linear-re: run this specification with pprof.spark.app.LinearREJob"
      case Some(v) if v.isTextual && v.asText() == "logistic-re" =>
        problems += "model logistic-re: run this specification with pprof.spark.app.LogisticREJob"
      case Some(v) if v.isTextual && v.asText() == "logistic" =>
        problems += "model logistic: run this specification with pprof.spark.app.LogisticJob"
      case Some(v) if v.isTextual && v.asText() == "three-stage" =>
        problems += "model three-stage: run this specification with pprof.spark.app.ThreeStageJob"
      case Some(v) if v.isTextual && v.asText() == "cox" =>
        problems += "model cox: run this specification with pprof.spark.app.CoxJob"
      case Some(v) => problems += s"model must be linear for this job, got ${v.toString}"
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
        .map(o => o.properties().asScala.map(e => e.getKey -> e.getValue.asText()).toMap)
        .getOrElse(Map.empty)
    )
    if (input != null && inputSpec.path.isDefined == inputSpec.table.isDefined)
      problems += "input needs exactly one of path and table"
    val columns = node(root, "columns").orNull
    if (columns == null) problems += "columns is required"
    unknown(columns, "columns", Set("outcome", "features", "provider", "rowId"))
    val features = strings(columns, "features", "columns").getOrElse {
      if (columns != null) problems += "columns.features is required"
      Seq.empty
    }
    val spec = LinearSpec(
      text(columns, "outcome", "columns", required = true).getOrElse(""),
      features,
      text(columns, "provider", "columns", required = true).getOrElse(""),
      text(columns, "rowId", "columns")
    )
    val fit = node(root, "fit").orNull
    unknown(fit, "fit", Set("varianceOption", "blocks"))
    val blocks = node(fit, "blocks").orNull
    unknown(
      blocks,
      "fit.blocks",
      Set("targetBlockBytes", "maxGroupsOnDriver", "driverBudgetBytes", "storageLevel")
    )
    val b = BlockOptions()
    val options =
      try
        LinearOptions(
          choice(fit, "varianceOption", "fit", LinearOptions.VarianceOptions, "complete"),
          BlockOptions(
            integer(blocks, "targetBlockBytes", "fit.blocks").getOrElse(b.targetBlockBytes),
            integer(blocks, "maxGroupsOnDriver", "fit.blocks")
              .map(_.toInt)
              .getOrElse(b.maxGroupsOnDriver),
            integer(blocks, "driverBudgetBytes", "fit.blocks").getOrElse(b.driverBudgetBytes),
            text(blocks, "storageLevel", "fit.blocks").getOrElse(b.storageLevel)
          )
        )
      catch {
        case e: IllegalArgumentException =>
          problems += s"fit: ${e.getMessage.stripPrefix("requirement failed: ")}"
          LinearOptions()
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
        "summary",
        "providerTests",
        "effectIntervals",
        "measures",
        "measureIntervals",
        "predictions"
      )
    )
    val summary = node(outputs, "summary").map { s =>
      unknown(s, "outputs.summary", Set("level", "null", "alternative"))
      LinearSummaryRequest(
        level(s, "level", "outputs.summary"),
        number(s, "null", "outputs.summary").getOrElse(0.0),
        choice(s, "alternative", "outputs.summary", LinearFE.Alternatives, "two_sided")
      )
    }
    val providerTests = node(outputs, "providerTests").map { t =>
      unknown(t, "outputs.providerTests", Set("reference", "alternative", "level", "critical"))
      val critical = number(t, "critical", "outputs.providerTests")
      critical
        .filterNot(_ > 0.0)
        .foreach(c => problems += s"outputs.providerTests.critical must be positive, got $c")
      LinearTestsRequest(
        reference(t, "outputs.providerTests"),
        choice(
          t,
          "alternative",
          "outputs.providerTests",
          LinearProviderTests.Alternatives,
          "two_sided"
        ),
        level(t, "level", "outputs.providerTests"),
        critical
      )
    }
    val effectIntervals = node(outputs, "effectIntervals").map { e =>
      unknown(e, "outputs.effectIntervals", Set("level"))
      level(e, "level", "outputs.effectIntervals")
    }
    val measures = node(outputs, "measures").map { m =>
      unknown(m, "outputs.measures", Set("kinds", "reference"))
      LinearMeasuresRequest(
        kinds(m, "outputs.measures", LinearStandardization.Kinds),
        reference(m, "outputs.measures")
      )
    }
    val measureIntervals = node(outputs, "measureIntervals").map { m =>
      unknown(m, "outputs.measureIntervals", Set("kinds", "reference", "alternative", "level"))
      LinearMeasureIntervalsRequest(
        kinds(m, "outputs.measureIntervals", Seq("indirect")),
        reference(m, "outputs.measureIntervals"),
        choice(
          m,
          "alternative",
          "outputs.measureIntervals",
          LinearProviderTests.Alternatives,
          "two_sided"
        ),
        level(m, "level", "outputs.measureIntervals")
      )
    }
    val outputSpec = LinearOutputSpec(
      text(outputs, "path", "outputs", required = true).getOrElse(""),
      flag(outputs, "fit", "outputs"),
      flag(outputs, "providers", "outputs"),
      summary,
      providerTests,
      effectIntervals,
      measures,
      measureIntervals,
      flag(outputs, "predictions", "outputs")
    )
    val found = problems.result()
    if (found.nonEmpty)
      throw new IllegalArgumentException(
        s"the run specification has problems: ${found.mkString("; ")}"
      )
    LinearRunSpec(inputSpec, spec, options, outputSpec)
  }
}
