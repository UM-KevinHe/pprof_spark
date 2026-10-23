package pprof.spark.app

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import pprof.spark.engine.backend.BlockOptions
import pprof.spark.engine.logistic.{LogisticREOptions, LogisticRESpec}

/** The column roles and options of the logistic random-intercept model as JSON (spec 4b-1 §5, §9): `columns` with
  * `outcome`, `features`, `provider`, `offset` and `rowId`; `fit` with `stage2` (nAGQ = 1, true by default),
  * `includeIntercept`, `sigmaUpper`, `maxIterations` and `blocks`. The Python facade parses through it, and slice
  * 4b-3's job runner will read whole specifications with it; every problem is reported at once.
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
    val columns = read(columnsJson, "columns")
    val fit = read(fitJson, "fit")
    val problems = Vector.newBuilder[String]
    val p = new Parser(problems)
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
    val blocks = Option(fit.get("blocks")).filterNot(_.isNull).orNull
    p.unknown(
      blocks,
      "fit.blocks",
      Set("targetBlockBytes", "maxGroupsOnDriver", "driverBudgetBytes", "storageLevel")
    )
    val b = BlockOptions()
    val d = LogisticREOptions()
    val options =
      try
        LogisticREOptions(
          p.flag(fit, "stage2", "fit", default = true),
          p.flag(fit, "includeIntercept", "fit", default = true),
          p.number(fit, "sigmaUpper", "fit").getOrElse(Double.PositiveInfinity),
          p.integer(fit, "maxIterations", "fit").map(_.toInt).getOrElse(d.maxIterations),
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
    val found = problems.result()
    if (found.nonEmpty)
      throw new IllegalArgumentException(s"the model has problems: ${found.mkString("; ")}")
    (spec, options)
  }

  private def node(parent: JsonNode, field: String): Option[JsonNode] =
    Option(parent).flatMap(p => Option(p.get(field))).filterNot(_.isNull)

  /** The field readers, each adding its problem rather than failing. */
  private final class Parser(problems: scala.collection.mutable.Builder[String, Vector[String]]) {

    def unknown(parent: JsonNode, where: String, known: Set[String]): Unit =
      Option(parent).filter(_.isObject).foreach { p =>
        p.properties()
          .asScala
          .map(_.getKey)
          .filterNot(known)
          .foreach(k => problems += s"$where.$k does not belong to a logistic-re model")
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
  }
}
