package pprof.spark.app

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.logistic._

final case class ThreeStageTestRequest(
    method: String,
    reference: EffectReference,
    alternative: String,
    level: Double,
    critical: Option[Double],
    nResample: Int,
    seed: Long
)

final case class ThreeStageMeasuresRequest(kinds: Seq[String], reference: EffectReference)

final case class ThreeStageIntervalRequest(
    option: String,
    kinds: Seq[String],
    measure: Seq[String],
    alternative: String,
    level: Double,
    method: String,
    reference: EffectReference
)

final case class ThreeStageSensitivityRequest(
    level: Double,
    method: String,
    alternative: String,
    testLevel: Double
)

/** What the three-stage job writes under `path` (docs/spec/logistic/three-stage-pipeline.md §5). */
final case class ThreeStageOutputSpec(
    path: String,
    fit: Boolean,
    providers: Boolean,
    clusters: Boolean,
    tests: Seq[ThreeStageTestRequest],
    measures: Option[ThreeStageMeasuresRequest],
    intervals: Seq[ThreeStageIntervalRequest],
    sensitivity: Option[ThreeStageSensitivityRequest],
    fitted: Boolean
)

final case class ThreeStageRunSpec(
    input: InputSpec,
    spec: ThreeStageSpec,
    options: ThreeStageFitOptions,
    outputs: ThreeStageOutputSpec
)

/** Parses three-stage run specifications (version 1, `model` `three-stage`); every problem is reported at once. */
object ThreeStageRunSpec {

  def parse(json: String): ThreeStageRunSpec = {
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
          .foreach(k => problems += s"$where.$k does not belong to a three-stage specification")
      }
    def text(
        parent: JsonNode,
        field: String,
        where: String,
        required: Boolean = false
    ): Option[String] = node(parent, field) match {
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
    def list(field: String): Seq[(JsonNode, String)] =
      node(node(root, "outputs").orNull, field) match {
        case Some(v) if v.isArray =>
          v.elements().asScala.zipWithIndex.map { case (n, i) => n -> s"outputs.$field[$i]" }.toSeq
        case Some(_) =>
          problems += s"outputs.$field must be a list"
          Seq.empty
        case None => Seq.empty
      }

    node(root, "version") match {
      case Some(v) if v.isInt && v.asInt() == RunSpec.Version => ()
      case Some(v)                                            =>
        problems += s"version ${v.asText()} is not supported; this release reads version ${RunSpec.Version}"
      case None => problems += "version is required"
    }
    node(root, "model") match {
      case Some(v) if v.isTextual && v.asText() == "three-stage" => ()
      case Some(v) if v.isTextual && v.asText() == "logistic"    =>
        problems += "model logistic: run this specification with pprof.spark.app.LogisticJob"
      case Some(v) if v.isTextual && v.asText() == "cox" =>
        problems += "model cox: run this specification with pprof.spark.app.CoxJob"
      case Some(v) => problems += s"model must be three-stage for this job, got ${v.toString}"
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
    unknown(columns, "columns", Set("outcome", "features", "provider", "cluster", "rowId"))
    val features = strings(columns, "features", "columns").getOrElse {
      if (columns != null) problems += "columns.features is required"
      Seq.empty
    }
    val spec = ThreeStageSpec(
      text(columns, "outcome", "columns", required = true).getOrElse(""),
      features,
      text(columns, "provider", "columns", required = true).getOrElse(""),
      text(columns, "cluster", "columns", required = true).getOrElse(""),
      text(columns, "rowId", "columns")
    )
    val fit = node(root, "fit").orNull
    unknown(fit, "fit", Set("cutoff", "stage1", "stage2", "stage3"))
    val s1 = node(fit, "stage1").orNull
    unknown(s1, "fit.stage1", Set("tol", "maxIter", "bound", "backtrack"))
    val s2 = node(fit, "stage2").orNull
    unknown(
      s2,
      "fit.stage2",
      Set("pirlsTolerance", "pirlsMaxIterations", "gradientTolerance", "maxIterations")
    )
    val s3 = node(fit, "stage3").orNull
    unknown(s3, "fit.stage3", Set("nNodes", "maxIter", "tol", "bound", "boundMode"))
    val d1 = LogisticOptions(screen = false)
    val d2 = ThreeStageStage2Options()
    val d3 = ThreeStageStage3Options()
    val options =
      try
        ThreeStageFitOptions(
          ThreeStageOptions(
            integer(fit, "cutoff", "fit").getOrElse(10L),
            d1.copy(
              tol = number(s1, "tol", "fit.stage1").getOrElse(d1.tol),
              maxIter = integer(s1, "maxIter", "fit.stage1").map(_.toInt).getOrElse(d1.maxIter),
              bound = number(s1, "bound", "fit.stage1").getOrElse(d1.bound),
              backtrack =
                if (node(s1, "backtrack").isDefined) flag(s1, "backtrack", "fit.stage1")
                else d1.backtrack
            )
          ),
          d2.copy(
            pirlsTolerance =
              number(s2, "pirlsTolerance", "fit.stage2").getOrElse(d2.pirlsTolerance),
            pirlsMaxIterations = integer(s2, "pirlsMaxIterations", "fit.stage2")
              .map(_.toInt)
              .getOrElse(d2.pirlsMaxIterations),
            gradientTolerance =
              number(s2, "gradientTolerance", "fit.stage2").getOrElse(d2.gradientTolerance),
            maxIterations =
              integer(s2, "maxIterations", "fit.stage2").map(_.toInt).getOrElse(d2.maxIterations)
          ),
          d3.copy(
            nNodes = integer(s3, "nNodes", "fit.stage3").map(_.toInt).getOrElse(d3.nNodes),
            maxIter = integer(s3, "maxIter", "fit.stage3").map(_.toInt).getOrElse(d3.maxIter),
            tol = number(s3, "tol", "fit.stage3").getOrElse(d3.tol),
            bound = number(s3, "bound", "fit.stage3").getOrElse(d3.bound),
            boundMode =
              choice(s3, "boundMode", "fit.stage3", Seq("relative", "absolute"), d3.boundMode),
            blocks = BlockOptions()
          )
        )
      catch {
        case e: IllegalArgumentException =>
          problems += s"fit: ${e.getMessage.stripPrefix("requirement failed: ")}"
          ThreeStageFitOptions()
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
        "tests",
        "measures",
        "intervals",
        "sensitivity",
        "fitted"
      )
    )
    val tests = list("tests").map { case (t, where) =>
      unknown(
        t,
        where,
        Set("method", "reference", "alternative", "level", "critical", "nResample", "seed")
      )
      ThreeStageTestRequest(
        choice(t, "method", where, ThreeStagePipeline.Methods, "exact"),
        reference(t, where),
        choice(t, "alternative", where, LogisticProviderTests.Alternatives, "two_sided"),
        level(t, "level", where),
        number(t, "critical", where),
        integer(t, "nResample", where).getOrElse(10000L).toInt,
        integer(t, "seed", where).getOrElse(0L)
      )
    }
    val measures = node(outputs, "measures").map { m =>
      unknown(m, "outputs.measures", Set("kinds", "reference"))
      val kinds = strings(m, "kinds", "outputs.measures").getOrElse(Seq("indirect", "direct"))
      kinds
        .filterNot(Set("indirect", "direct"))
        .foreach(k => problems += s"outputs.measures.kinds: unknown kind $k")
      ThreeStageMeasuresRequest(kinds, reference(m, "outputs.measures"))
    }
    val intervals = list("intervals").map { case (t, where) =>
      unknown(
        t,
        where,
        Set("option", "kinds", "measure", "alternative", "level", "method", "reference")
      )
      ThreeStageIntervalRequest(
        choice(t, "option", where, Seq("gamma", "SM"), "SM"),
        strings(t, "kinds", where).getOrElse(Seq("indirect")),
        strings(t, "measure", where).getOrElse(Seq("rate", "ratio")),
        choice(t, "alternative", where, LogisticProviderTests.Alternatives, "two_sided"),
        level(t, "level", where),
        choice(t, "method", where, Seq("exact", "poibin_exact"), "exact"),
        reference(t, where)
      )
    }
    val sensitivity = node(outputs, "sensitivity").map { s =>
      unknown(s, "outputs.sensitivity", Set("level", "method", "alternative", "testLevel"))
      ThreeStageSensitivityRequest(
        level(s, "level", "outputs.sensitivity"),
        choice(s, "method", "outputs.sensitivity", ThreeStagePipeline.Methods, "exact"),
        choice(
          s,
          "alternative",
          "outputs.sensitivity",
          LogisticProviderTests.Alternatives,
          "two_sided"
        ),
        level(s, "testLevel", "outputs.sensitivity")
      )
    }
    val outputSpec = ThreeStageOutputSpec(
      text(outputs, "path", "outputs", required = true).getOrElse(""),
      flag(outputs, "fit", "outputs"),
      flag(outputs, "providers", "outputs"),
      flag(outputs, "clusters", "outputs"),
      tests,
      measures,
      intervals,
      sensitivity,
      flag(outputs, "fitted", "outputs")
    )
    val found = problems.result()
    if (found.nonEmpty)
      throw new IllegalArgumentException(
        s"the run specification has problems: ${found.mkString("; ")}"
      )
    ThreeStageRunSpec(inputSpec, spec, options, outputSpec)
  }
}
