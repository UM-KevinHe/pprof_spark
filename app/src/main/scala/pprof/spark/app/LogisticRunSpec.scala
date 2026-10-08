package pprof.spark.app

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.logistic.{
  EffectReference,
  LogisticOptions,
  LogisticProviderTests,
  LogisticSpec,
  LogisticStandardization
}

final case class CovariateTestsRequest(methods: Seq[String], robust: Boolean)

final case class ProviderTestsRequest(
    method: String,
    reference: EffectReference,
    alternative: String,
    level: Double,
    critical: Option[Double],
    nResample: Int,
    seed: Long
)

final case class LogisticMeasuresRequest(
    kinds: Seq[String],
    reference: EffectReference,
    extremeTrials: Double,
    method: String
)

final case class MeasureTestRequest(
    measure: String,
    nullValue: Option[Double],
    transform: String,
    reference: EffectReference,
    variance: String,
    indirectVariance: String,
    alternative: String,
    level: Double,
    critical: Option[Double],
    method: String
)

/** What the logistic job writes under `path` (Phase 2e specification §3). */
final case class LogisticOutputSpec(
    path: String,
    fit: Boolean,
    providers: Boolean,
    covariateTests: Option[CovariateTestsRequest],
    providerTests: Option[ProviderTestsRequest],
    measures: Option[LogisticMeasuresRequest],
    measureTests: Seq[MeasureTestRequest],
    predictions: Boolean
)

/** A logistic run specification: version 1 with `model` `logistic` (Phase 2e specification §2). */
final case class LogisticRunSpec(
    input: InputSpec,
    columns: LogisticSpec,
    options: LogisticOptions,
    outputs: LogisticOutputSpec
)

/** Parses logistic run specifications; every problem is reported at once. */
object LogisticRunSpec {

  def parse(json: String): LogisticRunSpec = {
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
          .foreach(key => problems += s"$where.$key does not belong to a logistic specification")
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
    def flag(parent: JsonNode, field: String, where: String): Boolean =
      node(parent, field).exists { v =>
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
    def level(parent: JsonNode, where: String): Double = {
      val value = number(parent, "level", where).getOrElse(0.95)
      if (!(value > 0.0 && value < 1.0)) problems += s"$where.level must lie in (0, 1), got $value"
      value
    }

    node(root, "version") match {
      case Some(v) if v.isInt && v.asInt() == RunSpec.Version => ()
      case Some(v)                                            =>
        problems += s"version ${v.asText()} is not supported; this release reads version ${RunSpec.Version}"
      case None => problems += "version is required"
    }
    node(root, "model") match {
      case Some(v) if v.isTextual && v.asText() == "logistic" => ()
      case Some(v) if v.isTextual && v.asText() == "cox"      =>
        problems += "model cox: run this specification with pprof.spark.app.CoxJob"
      case Some(v) => problems += s"model must be logistic for this job, got ${v.toString}"
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
    unknown(
      columns,
      "columns",
      Set("outcome", "features", "provider", "trials", "rowId", "cluster")
    )
    val outcome = text(columns, "outcome", "columns", required = true)
    val provider = text(columns, "provider", "columns", required = true)
    val features = strings(columns, "features", "columns").getOrElse {
      if (columns != null) problems += "columns.features is required"
      Seq.empty
    }
    if (columns != null && node(columns, "features").isDefined && features.isEmpty)
      problems += "columns.features must name at least one feature"
    val cluster = text(columns, "cluster", "columns")
    val spec = LogisticSpec(
      outcome.getOrElse(""),
      features,
      provider.getOrElse(""),
      text(columns, "trials", "columns"),
      text(columns, "rowId", "columns"),
      cluster
    )

    val fit = node(root, "fit").orNull
    unknown(
      fit,
      "fit",
      Set(
        "tol",
        "maxIter",
        "bound",
        "backtrack",
        "screen",
        "minRecords",
        "confidenceLevel",
        "correlationThreshold",
        "maxProvidersOnDriver",
        "blocks"
      )
    )
    val blocks = node(fit, "blocks").orNull
    unknown(
      blocks,
      "fit.blocks",
      Set("targetBlockBytes", "maxGroupsOnDriver", "driverBudgetBytes", "storageLevel")
    )
    val d = LogisticOptions()
    val b = BlockOptions()
    def booleanOption(field: String, default: Boolean): Boolean =
      if (node(fit, field).isDefined) flag(fit, field, "fit") else default
    val options =
      try
        d.copy(
          tol = number(fit, "tol", "fit").getOrElse(d.tol),
          maxIter = integer(fit, "maxIter", "fit").map(_.toInt).getOrElse(d.maxIter),
          bound = number(fit, "bound", "fit").getOrElse(d.bound),
          backtrack = booleanOption("backtrack", d.backtrack),
          screen = booleanOption("screen", d.screen),
          minRecords = integer(fit, "minRecords", "fit").getOrElse(d.minRecords),
          confidenceLevel = number(fit, "confidenceLevel", "fit").getOrElse(d.confidenceLevel),
          correlationThreshold =
            number(fit, "correlationThreshold", "fit").getOrElse(d.correlationThreshold),
          maxProvidersOnDriver = integer(fit, "maxProvidersOnDriver", "fit")
            .map(_.toInt)
            .getOrElse(d.maxProvidersOnDriver),
          blocks = b.copy(
            targetBlockBytes =
              integer(blocks, "targetBlockBytes", "fit.blocks").getOrElse(b.targetBlockBytes),
            maxGroupsOnDriver = integer(blocks, "maxGroupsOnDriver", "fit.blocks")
              .map(_.toInt)
              .getOrElse(b.maxGroupsOnDriver),
            driverBudgetBytes =
              integer(blocks, "driverBudgetBytes", "fit.blocks").getOrElse(b.driverBudgetBytes),
            storageLevel = text(blocks, "storageLevel", "fit.blocks").getOrElse(b.storageLevel)
          )
        )
      catch {
        case e: IllegalArgumentException =>
          problems += s"fit: ${e.getMessage.stripPrefix("requirement failed: ")}"
          d
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
        "covariateTests",
        "providerTests",
        "measures",
        "measureTests",
        "predictions"
      )
    )
    def needsCluster(what: String): Unit =
      if (cluster.isEmpty) problems += s"$what needs columns.cluster"
    val covariateTests = node(outputs, "covariateTests").map { c =>
      unknown(c, "outputs.covariateTests", Set("methods", "robust"))
      val methods =
        strings(c, "methods", "outputs.covariateTests").getOrElse(Seq("wald", "lr", "score"))
      methods.filterNot(Set("wald", "lr", "score")).foreach { m =>
        problems += s"outputs.covariateTests.methods: unknown method $m; expected wald, lr or score"
      }
      val robust = flag(c, "robust", "outputs.covariateTests")
      if (robust) needsCluster("outputs.covariateTests.robust")
      CovariateTestsRequest(methods, robust)
    }
    val providerTests = node(outputs, "providerTests").map { t =>
      val where = "outputs.providerTests"
      unknown(
        t,
        where,
        Set("method", "reference", "alternative", "level", "critical", "nResample", "seed")
      )
      val nResample = integer(t, "nResample", where).getOrElse(10000L)
      if (nResample <= 0L) problems += s"$where.nResample must be positive"
      ProviderTestsRequest(
        choice(t, "method", where, LogisticProviderTests.Methods, "poibin_exact"),
        reference(t, where),
        choice(t, "alternative", where, LogisticProviderTests.Alternatives, "two_sided"),
        level(t, where),
        number(t, "critical", where),
        nResample.toInt,
        integer(t, "seed", where).getOrElse(0L)
      )
    }
    val measures = node(outputs, "measures").map { m =>
      val where = "outputs.measures"
      unknown(m, where, Set("kinds", "reference", "extremeTrials", "method"))
      val kinds = strings(m, "kinds", where).getOrElse(Seq("indirect", "direct"))
      kinds
        .filterNot(Set("indirect", "direct"))
        .foreach(k => problems += s"$where.kinds: unknown kind $k")
      val extreme = number(m, "extremeTrials", where).getOrElse(0.0)
      if (extreme < 0.0) problems += s"$where.extremeTrials must be non-negative"
      LogisticMeasuresRequest(
        kinds,
        reference(m, where),
        extreme,
        choice(m, "method", where, LogisticStandardization.Methods, "binned")
      )
    }
    val measureTests = node(outputs, "measureTests") match {
      case Some(list) if list.isArray =>
        list
          .elements()
          .asScala
          .zipWithIndex
          .map { case (t, i) =>
            val where = s"outputs.measureTests[$i]"
            unknown(
              t,
              where,
              Set(
                "measure",
                "nullValue",
                "transform",
                "reference",
                "variance",
                "indirectVariance",
                "alternative",
                "level",
                "critical",
                "method"
              )
            )
            val measure = text(t, "measure", where, required = true).getOrElse("direct_rate")
            if (!LogisticStandardization.Measures.contains(measure))
              problems += s"$where.measure: unknown measure $measure"
            val variance =
              choice(t, "variance", where, Seq("model", "robust", "robust_fixed_beta"), "model")
            if (variance != "model") needsCluster(s"$where.variance $variance")
            MeasureTestRequest(
              measure,
              number(t, "nullValue", where),
              choice(t, "transform", where, Seq("auto", "identity", "logit", "log"), "auto"),
              reference(t, where),
              variance,
              choice(t, "indirectVariance", where, Seq("null", "fitted"), "null"),
              choice(t, "alternative", where, LogisticProviderTests.Alternatives, "two_sided"),
              level(t, where),
              number(t, "critical", where),
              choice(t, "method", where, LogisticStandardization.Methods, "binned")
            )
          }
          .toSeq
      case Some(_) =>
        problems += "outputs.measureTests must be a list"
        Seq.empty
      case None => Seq.empty
    }
    val outputSpec = LogisticOutputSpec(
      text(outputs, "path", "outputs", required = true).getOrElse(""),
      flag(outputs, "fit", "outputs"),
      flag(outputs, "providers", "outputs"),
      covariateTests,
      providerTests,
      measures,
      measureTests,
      flag(outputs, "predictions", "outputs")
    )
    val found = problems.result()
    if (found.nonEmpty)
      throw new IllegalArgumentException(
        s"the run specification has problems: ${found.mkString("; ")}"
      )
    LogisticRunSpec(inputSpec, spec, options, outputSpec)
  }
}
