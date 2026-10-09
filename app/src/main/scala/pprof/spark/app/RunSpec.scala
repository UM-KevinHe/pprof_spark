package pprof.spark.app

import scala.jdk.CollectionConverters._
import scala.util.Try

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.cox.{CoxOptions, CoxSpec, Standardization, TestMethod, Ties}

/** Where the job reads its rows: a path in a Spark data source format, or a table. */
final case class InputSpec(
    path: Option[String],
    table: Option[String],
    format: String,
    options: Map[String, String]
)

final case class MeasuresRequest(provider: String, kinds: Seq[Standardization])

final case class TestsRequest(provider: String, method: TestMethod, level: Double)

/** What the job writes under `path`: each requested result in its own subdirectory. */
final case class OutputSpec(
    path: String,
    fit: Boolean,
    baseline: Boolean,
    residuals: Boolean,
    measures: Option[MeasuresRequest],
    tests: Option[TestsRequest]
)

/** A versioned run specification (Phase 1d specification §5). */
final case class RunSpec(
    input: InputSpec,
    columns: CoxSpec,
    options: CoxOptions,
    outputs: OutputSpec
)

/** Parses run specifications; every problem is reported at once. */
object RunSpec {

  val Version: Int = 1

  def parse(json: String): RunSpec = {
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
        else { problems += s"$where.$field must be a number"; None }
      }
    def flag(parent: JsonNode, field: String, where: String): Boolean =
      node(parent, field).exists { v =>
        if (v.isBoolean) v.asBoolean()
        else { problems += s"$where.$field must be true or false"; false }
      }
    def strings(parent: JsonNode, field: String, where: String): Seq[String] =
      node(parent, field) match {
        case Some(v) if v.isArray && v.elements().asScala.forall(_.isTextual) =>
          v.elements().asScala.map(_.asText()).toSeq
        case Some(_) =>
          problems += s"$where.$field must be a list of strings"
          Seq.empty
        case None =>
          problems += s"$where.$field is required"
          Seq.empty
      }

    node(root, "version") match {
      case Some(v) if v.isInt && v.asInt() == Version => ()
      case Some(v)                                    =>
        problems += s"version ${v.asText()} is not supported; this release reads version $Version"
      case None => problems += "version is required"
    }
    node(root, "model").foreach { v =>
      if (!v.isTextual) problems += "model must be a string"
      else
        v.asText() match {
          case "cox"      => ()
          case "logistic" =>
            problems += "model logistic: run this specification with pprof.spark.app.LogisticJob"
          case "three-stage" =>
            problems += "model three-stage: run this specification with pprof.spark.app.ThreeStageJob"
          case other => problems += s"model must be cox, logistic or three-stage, got $other"
        }
    }

    val input = node(root, "input").orNull
    if (input == null) problems += "input is required"
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
    val time = text(columns, "time", "columns", required = true)
    val event = text(columns, "event", "columns", required = true)
    val features = strings(columns, "features", "columns")

    val fit = node(root, "fit").orNull
    val ties = text(fit, "ties", "fit").map(name =>
      Try(Ties.fromName(name)).toOption.getOrElse {
        problems += s"fit.ties must be breslow or efron, got $name"
        Ties.Breslow
      }
    )
    val blocks = node(fit, "blocks").orNull
    val defaults = CoxOptions()
    val blockOptions = {
      val b = BlockOptions()
      b.copy(
        targetBlockBytes = number(blocks, "targetBlockBytes", "fit.blocks")
          .map(_.toLong)
          .getOrElse(b.targetBlockBytes),
        maxGroupsOnDriver = number(blocks, "maxGroupsOnDriver", "fit.blocks")
          .map(_.toInt)
          .getOrElse(b.maxGroupsOnDriver),
        driverBudgetBytes = number(blocks, "driverBudgetBytes", "fit.blocks")
          .map(_.toLong)
          .getOrElse(b.driverBudgetBytes),
        storageLevel = text(blocks, "storageLevel", "fit.blocks").getOrElse(b.storageLevel)
      )
    }

    val outputs = node(root, "outputs").orNull
    if (outputs == null) problems += "outputs is required"
    val measures = node(outputs, "measures").map { m =>
      val kinds = node(m, "kinds")
        .map(_ => strings(m, "kinds", "outputs.measures"))
        .getOrElse(Seq("indirect"))
      MeasuresRequest(
        text(m, "provider", "outputs.measures", required = true).getOrElse(""),
        kinds.flatMap {
          case "indirect" => Some(Standardization.Indirect)
          case "direct"   => Some(Standardization.Direct)
          case other      =>
            problems += s"outputs.measures.kinds: unknown kind $other; expected indirect or direct"
            None
        }
      )
    }
    val tests = node(outputs, "tests").map { t =>
      val method = text(t, "method", "outputs.tests").getOrElse("midp") match {
        case "midp"  => TestMethod.MidP
        case "exact" => TestMethod.Exact
        case other   =>
          problems += s"outputs.tests.method must be midp or exact, got $other"
          TestMethod.MidP
      }
      val level = number(t, "level", "outputs.tests").getOrElse(0.95)
      if (!(level > 0.0 && level < 1.0))
        problems += s"outputs.tests.level must lie in (0, 1), got $level"
      TestsRequest(
        text(t, "provider", "outputs.tests", required = true).getOrElse(""),
        method,
        level
      )
    }
    val outputSpec = OutputSpec(
      text(outputs, "path", "outputs", required = true).getOrElse(""),
      flag(outputs, "fit", "outputs"),
      flag(outputs, "baseline", "outputs"),
      flag(outputs, "residuals", "outputs"),
      measures,
      tests
    )

    val found = problems.result()
    if (found.nonEmpty)
      throw new IllegalArgumentException(
        s"the run specification has problems: ${found.mkString("; ")}"
      )
    val spec = CoxSpec(
      time.get,
      event.get,
      features,
      text(columns, "strata", "columns"),
      text(columns, "rowId", "columns"),
      text(columns, "weight", "columns"),
      text(columns, "offset", "columns"),
      text(columns, "entry", "columns"),
      text(columns, "cluster", "columns")
    )
    val options = CoxOptions(
      ties = ties.getOrElse(defaults.ties),
      maxIterations =
        number(fit, "maxIterations", "fit").map(_.toInt).getOrElse(defaults.maxIterations),
      eps = number(fit, "eps", "fit").getOrElse(defaults.eps),
      maxHalvings = number(fit, "maxHalvings", "fit").map(_.toInt).getOrElse(defaults.maxHalvings),
      confidenceLevel = number(fit, "confidenceLevel", "fit").getOrElse(defaults.confidenceLevel),
      maxStratumRows =
        number(fit, "maxStratumRows", "fit").map(_.toLong).getOrElse(defaults.maxStratumRows),
      blocks = blockOptions,
      robust = flag(fit, "robust", "fit")
    )
    RunSpec(inputSpec, spec, options, outputSpec)
  }
}
