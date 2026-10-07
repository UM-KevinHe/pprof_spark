package pprof.spark.app.python

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.{DataFrame, SparkSession}
import pprof.spark.app.RunSpec
import pprof.spark.engine.BuildInfo
import pprof.spark.engine.cox.{
  CoxFit,
  CoxFitIO,
  CoxMeasures,
  CoxOptions,
  CoxPH,
  CoxPrediction,
  CoxProviderTests,
  CoxSpec,
  Standardization,
  TestMethod
}

/** The facade the Python wrappers call through py4j (ADR-0009, D-33). Arguments are Java types and
  * DataFrames; results are DataFrames, JSON summaries with doubles in hexadecimal (exact), and fits
  * as opaque handles. Every statistic is the engine's; nothing here computes one.
  */
object PythonApi {

  /** The package version; the Python wrappers compare it with their own. */
  def version(): String = BuildInfo.version

  /** Fits a Cox model. Column roles and options use the job runner's format
    * (docs/guide/cox-job.md): the `columns` and `fit` objects of a version-1 run specification;
    * every problem is reported at once.
    */
  def coxFit(df: DataFrame, columnsJson: String, fitJson: String): CoxFit = {
    val (spec, options) = coxModel(columnsJson, fitJson)
    CoxPH.fit(df, spec, options)
  }

  /** The column roles and options of [[coxFit]], through the job runner's parser. */
  def coxModel(columnsJson: String, fitJson: String): (CoxSpec, CoxOptions) = {
    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()
    root.put("version", RunSpec.Version)
    root.putObject("input").put("path", "unused")
    root.set[JsonNode]("columns", objectNode(mapper, columnsJson, "columns"))
    root.set[JsonNode]("fit", objectNode(mapper, fitJson, "fit"))
    root.putObject("outputs").put("path", "unused")
    val run = RunSpec.parse(mapper.writeValueAsString(root))
    (run.columns, run.options)
  }

  /** The fit as JSON: coefficients, covariance, likelihoods, convergence, counts, warnings, the
    * specification and options, software and fingerprint. Doubles are `Double.toHexString` strings.
    */
  def coxSummary(fit: CoxFit): String = {
    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()
    def hex(value: Double): String = java.lang.Double.toHexString(value)
    root.put("kind", "CoxFit")
    root.put("featureStatus", fit.featureStatus)
    val coefficients = root.putArray("coefficients")
    fit.coefficients.foreach { c =>
      val node = coefficients.addObject()
      node.put("feature", c.feature)
      node.put("estimate", hex(c.estimate))
      node.put("standardError", hex(c.standardError))
      node.put("z", hex(c.z))
      node.put("pValue", hex(c.pValue))
      node.put("lower", hex(c.lower))
      node.put("upper", hex(c.upper))
    }
    val covariance = root.putArray("covariance")
    fit.covariance.foreach(v => covariance.add(hex(v)))
    root.put("logLikelihood", hex(fit.logLikelihood))
    root.put("logLikelihoodNull", hex(fit.logLikelihoodNull))
    root.put("iterations", fit.iterations)
    root.put("halvings", fit.halvings)
    root.put("converged", fit.converged)
    root.put("message", fit.message)
    root.put("observations", fit.observations)
    root.put("events", fit.events)
    root.put("strata", fit.strata)
    root.put("strataWithoutEvents", fit.strataWithoutEvents)
    root.put("robust", fit.robust)
    root.put("clusters", fit.clusters)
    root.put("ties", fit.ties)
    root.put("fingerprint", fit.fingerprint)
    val warnings = root.putArray("warnings")
    fit.warnings.foreach(w => warnings.add(w))
    val spec = root.putObject("columns")
    spec.put("time", fit.spec.time)
    spec.put("event", fit.spec.event)
    val features = spec.putArray("features")
    fit.spec.features.foreach(f => features.add(f))
    Seq(
      "strata" -> fit.spec.strata,
      "rowId" -> fit.spec.rowId,
      "weight" -> fit.spec.weight,
      "offset" -> fit.spec.offset,
      "entry" -> fit.spec.entry,
      "cluster" -> fit.spec.cluster
    ).foreach { case (key, value) => value.foreach(v => spec.put(key, v)) }
    val software = root.putObject("software")
    fit.software.toFields.foreach { case (key, value) => software.put(key, value) }
    mapper.writeValueAsString(root)
  }

  def coxBaseline(df: DataFrame, fit: CoxFit): DataFrame = CoxPH.baseline(df, fit)

  def coxResiduals(df: DataFrame, fit: CoxFit): DataFrame = CoxPH.residuals(df, fit)

  def coxLinearPredictor(fit: CoxFit, df: DataFrame): DataFrame =
    CoxPrediction.linearPredictor(fit, df)

  def coxRelativeHazard(fit: CoxFit, df: DataFrame): DataFrame =
    CoxPrediction.relativeHazard(fit, df)

  def coxCumulativeHazard(
      fit: CoxFit,
      baseline: DataFrame,
      df: DataFrame,
      timeCol: String
  ): DataFrame =
    CoxPrediction.cumulativeHazard(fit, baseline, df, timeCol)

  def coxSurvival(fit: CoxFit, baseline: DataFrame, df: DataFrame, timeCol: String): DataFrame =
    CoxPrediction.survival(fit, baseline, df, timeCol)

  /** One standardized measure, `indirect` or `direct` (Phase 1d specification §1). */
  def coxMeasures(df: DataFrame, fit: CoxFit, provider: String, kind: String): DataFrame = {
    val standardization = kind match {
      case "indirect" => Standardization.Indirect
      case "direct"   => Standardization.Direct
      case other      =>
        throw new IllegalArgumentException(s"kind must be indirect or direct, got $other")
    }
    CoxMeasures.standardized(df, fit, provider, Seq(standardization))(standardization)
  }

  /** Provider tests, `midp` or `exact` (Phase 1d specification §2). */
  def coxProviderTests(
      df: DataFrame,
      fit: CoxFit,
      provider: String,
      method: String,
      level: Double
  ): DataFrame = {
    val test = method match {
      case "midp"  => TestMethod.MidP
      case "exact" => TestMethod.Exact
      case other => throw new IllegalArgumentException(s"method must be midp or exact, got $other")
    }
    CoxProviderTests.test(df, fit, provider, test, level)
  }

  /** Saves `fit`, with `baseline` when it is not null (`CoxFitIO`). */
  def coxSave(spark: SparkSession, fit: CoxFit, path: String, baseline: DataFrame): Unit =
    CoxFitIO.save(spark, fit, path, Option(baseline))

  def coxLoad(spark: SparkSession, path: String): CoxFit = CoxFitIO.load(spark, path)

  /** The baseline saved with a fit, or null. */
  def coxLoadBaseline(spark: SparkSession, path: String): DataFrame =
    CoxFitIO.loadBaseline(spark, path).orNull

  private def objectNode(mapper: ObjectMapper, json: String, what: String): ObjectNode = {
    val node =
      try mapper.readTree(if (json == null || json.trim.isEmpty) "{}" else json)
      catch {
        case e: Exception =>
          throw new IllegalArgumentException(s"the $what are not valid JSON: ${e.getMessage}")
      }
    node match {
      case o: ObjectNode => o
      case _             => throw new IllegalArgumentException(s"the $what must be a JSON object")
    }
  }
}
