package pprof.spark.app.python

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.{DataFrame, SparkSession}
import pprof.spark.app.{RunSpec, ThreeStageJob, ThreeStageRunSpec}
import pprof.spark.engine.BuildInfo
import pprof.spark.engine.layout.GroupKey
import pprof.spark.engine.logistic.{
  EffectReference,
  LogisticFE,
  LogisticFit,
  LogisticFitIO,
  LogisticOptions,
  LogisticProviderTests,
  LogisticSpec,
  LogisticStandardization,
  LogisticTest,
  ThreeStage,
  ThreeStageFit,
  ThreeStageFitIO,
  ThreeStageFitOptions,
  ThreeStagePipeline,
  ThreeStageSensitivity,
  ThreeStageSpec
}
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

  /** Fits the logistic fixed-effect model (docs/spec/logistic/). `columnsJson`: `outcome`, `features`,
    * `provider` and optionally `trials`, `rowId`, `cluster`; `fitJson`: any of `tol`, `maxIter`,
    * `bound`, `backtrack`, `screen`, `minRecords`, `confidenceLevel`, `correlationThreshold`,
    * `maxProvidersOnDriver`. Every problem is reported at once.
    */
  def logisticFit(df: DataFrame, columnsJson: String, fitJson: String): LogisticFit = {
    val (spec, options) = logisticModel(columnsJson, fitJson)
    LogisticFE.fit(df, spec, options)
  }

  def logisticModel(columnsJson: String, fitJson: String): (LogisticSpec, LogisticOptions) = {
    val mapper = new ObjectMapper()
    val columns = objectNode(mapper, columnsJson, "columns")
    val fit = objectNode(mapper, fitJson, "fit")
    val problems = Vector.newBuilder[String]
    def unknown(node: ObjectNode, where: String, known: Set[String]): Unit =
      node
        .fieldNames()
        .forEachRemaining(key => if (!known(key)) problems += s"$where.$key is not recognized")
    unknown(
      columns,
      "columns",
      Set("outcome", "features", "provider", "trials", "rowId", "cluster")
    )
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
        "maxProvidersOnDriver"
      )
    )
    def text(key: String, required: Boolean): Option[String] = Option(columns.get(key)) match {
      case Some(v) if v.isTextual => Some(v.asText())
      case Some(_)                =>
        problems += s"columns.$key must be a string"
        None
      case None =>
        if (required) problems += s"columns.$key is required"
        None
    }
    def value[A](
        key: String,
        accepts: JsonNode => Boolean,
        read: JsonNode => A,
        kind: String
    ): Option[A] =
      Option(fit.get(key)).flatMap { v =>
        if (accepts(v)) Some(read(v))
        else {
          problems += s"fit.$key must be $kind"
          None
        }
      }
    val outcome = text("outcome", required = true)
    val provider = text("provider", required = true)
    val features = Option(columns.get("features")) match {
      case Some(v) if v.isArray && v.size > 0 && (0 until v.size).forall(i => v.get(i).isTextual) =>
        (0 until v.size).map(i => v.get(i).asText())
      case Some(_) =>
        problems += "columns.features must be a non-empty array of strings"
        Seq.empty
      case None =>
        problems += "columns.features is required"
        Seq.empty
    }
    val defaults = LogisticOptions()
    val number = (v: JsonNode) => v.isNumber
    val integer = (v: JsonNode) => v.isIntegralNumber
    val boolean = (v: JsonNode) => v.isBoolean
    val options =
      try
        Some(
          defaults.copy(
            tol = value("tol", number, _.asDouble, "a number").getOrElse(defaults.tol),
            maxIter = value("maxIter", integer, _.asInt, "an integer").getOrElse(defaults.maxIter),
            bound = value("bound", number, _.asDouble, "a number").getOrElse(defaults.bound),
            backtrack =
              value("backtrack", boolean, _.asBoolean, "a boolean").getOrElse(defaults.backtrack),
            screen = value("screen", boolean, _.asBoolean, "a boolean").getOrElse(defaults.screen),
            minRecords =
              value("minRecords", integer, _.asLong, "an integer").getOrElse(defaults.minRecords),
            confidenceLevel = value("confidenceLevel", number, _.asDouble, "a number")
              .getOrElse(defaults.confidenceLevel),
            correlationThreshold = value("correlationThreshold", number, _.asDouble, "a number")
              .getOrElse(defaults.correlationThreshold),
            maxProvidersOnDriver = value("maxProvidersOnDriver", integer, _.asInt, "an integer")
              .getOrElse(defaults.maxProvidersOnDriver)
          )
        )
      catch {
        case e: IllegalArgumentException =>
          problems += s"fit: ${e.getMessage.stripPrefix("requirement failed: ")}"
          None
      }
    val found = problems.result()
    if (found.nonEmpty)
      throw new IllegalArgumentException(s"invalid model: ${found.mkString("; ")}")
    (
      LogisticSpec(
        outcome.get,
        features,
        provider.get,
        text("trials", false),
        text("rowId", false),
        text("cluster", false)
      ),
      options.get
    )
  }

  /** The logistic fit as JSON, doubles as `Double.toHexString` strings. */
  def logisticSummary(fit: LogisticFit): String = {
    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()
    def hex(value: Double): String = java.lang.Double.toHexString(value)
    root.put("kind", "LogisticFit")
    root.put("featureStatus", fit.status)
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
    fit.robustCovariance.foreach { v =>
      val robust = root.putArray("robustCovariance")
      v.foreach(x => robust.add(hex(x)))
    }
    root.put("logLikelihood", hex(fit.loglik))
    root.put("aic", hex(fit.aic))
    root.put("bic", hex(fit.bic))
    fit.auc.foreach(a => root.put("auc", hex(a)))
    root.put("iterations", fit.iterations)
    root.put("converged", fit.converged)
    root.put("rows", fit.rows)
    root.put("trials", hex(fit.trials))
    root.put("events", hex(fit.events))
    root.put("providers", fit.providers.size)
    root.put("degenerateProviders", fit.degenerateProviders)
    root.put("clusters", fit.clusters)
    root.put("fingerprint", fit.fingerprint)
    val excluded = root.putArray("excluded")
    fit.excluded.foreach { case (key, records) =>
      val node = excluded.addObject()
      node.put("provider", GroupKey.value(key).toString)
      node.put("records", records)
    }
    val warnings = root.putArray("warnings")
    fit.warnings.foreach(w => warnings.add(w))
    val spec = root.putObject("columns")
    spec.put("outcome", fit.spec.outcome)
    val features = spec.putArray("features")
    fit.spec.features.foreach(f => features.add(f))
    spec.put("provider", fit.spec.provider)
    Seq("trials" -> fit.spec.trials, "rowId" -> fit.spec.rowId, "cluster" -> fit.spec.cluster)
      .foreach { case (key, value) =>
        value.foreach(v => spec.put(key, v))
      }
    val software = root.putObject("software")
    fit.software.toFields.foreach { case (key, value) => software.put(key, value) }
    mapper.writeValueAsString(root)
  }

  def logisticProviders(spark: SparkSession, fit: LogisticFit): DataFrame =
    LogisticFE.providerTable(spark, fit)

  /** Wald tests as JSON (slice 2b): `alternative` is `two_sided`, `less` or `greater`. */
  def logisticWald(
      fit: LogisticFit,
      nullValue: Double,
      alternative: String,
      level: Double,
      robust: Boolean
  ): String =
    testsJson(LogisticFE.waldTests(fit, nullValue, alternative, level, robust))

  /** Likelihood-ratio (`lr`) or score (`score`) tests as JSON; `covariatesJson` an array of names, or empty
    * for all.
    */
  def logisticTests(
      df: DataFrame,
      fit: LogisticFit,
      method: String,
      covariatesJson: String
  ): String = {
    val node = new ObjectMapper().readTree(
      if (covariatesJson == null || covariatesJson.isEmpty) "[]" else covariatesJson
    )
    require(node.isArray, "the covariates must be a JSON array of names")
    testsJson(
      LogisticFE.covariateTests(df, fit, method, (0 until node.size).map(i => node.get(i).asText()))
    )
  }

  /** Provider tests (slice 2c): `reference` is `median`, `mean` or a number; `critical` NaN for the
    * default; `providersJson` a JSON array of provider keys, or empty for all.
    */
  def logisticProviderTests(
      df: DataFrame,
      fit: LogisticFit,
      method: String,
      reference: String,
      alternative: String,
      level: Double,
      critical: Double,
      providersJson: String,
      nResample: Int,
      seed: Long
  ): DataFrame = {
    val effect = reference match {
      case "median" => EffectReference.Median
      case "mean"   => EffectReference.Mean
      case other    =>
        EffectReference.Value(
          other.toDoubleOption.getOrElse(
            throw new IllegalArgumentException("reference must be median, mean or a number")
          )
        )
    }
    val providers =
      if (providersJson == null || providersJson.isEmpty) None
      else {
        val node = new ObjectMapper().readTree(providersJson)
        require(node.isArray, "the providers must be a JSON array")
        if (node.size == 0) None else Some((0 until node.size).map(i => node.get(i).asText()))
      }
    LogisticProviderTests.test(
      df,
      fit,
      method,
      effect,
      alternative,
      level,
      if (critical.isNaN) None else Some(critical),
      providers,
      nResample,
      seed
    )
  }

  private def effectReference(reference: String): EffectReference = reference match {
    case "median" => EffectReference.Median
    case "mean"   => EffectReference.Mean
    case other    =>
      EffectReference.Value(
        other.toDoubleOption.getOrElse(
          throw new IllegalArgumentException("reference must be median, mean or a number")
        )
      )
  }

  private def stringList(json: String, what: String): Option[Seq[String]] =
    if (json == null || json.isEmpty) None
    else {
      val node = new ObjectMapper().readTree(json)
      require(node.isArray, s"the $what must be a JSON array")
      if (node.size == 0) None else Some((0 until node.size).map(i => node.get(i).asText()))
    }

  /** Standardized measures (slice 2d), as pprof_py's `calculate_standardized_measures`: a map from
    * `indirect` and `direct` to their tables.
    */
  def logisticStandardizedMeasures(
      df: DataFrame,
      fit: LogisticFit,
      kindsJson: String,
      reference: String,
      providersJson: String,
      extremeTrials: Double,
      method: String
  ): java.util.Map[String, DataFrame] =
    LogisticStandardization
      .measures(
        df,
        fit,
        stringList(kindsJson, "kinds").getOrElse(Seq("indirect", "direct")),
        effectReference(reference),
        stringList(providersJson, "providers"),
        extremeTrials,
        method
      )
      .asJava

  /** One measure's estimates, standard errors and reference value (pprof_py's `standardized_measure`). */
  def logisticStandardizedMeasure(
      df: DataFrame,
      fit: LogisticFit,
      measure: String,
      reference: String,
      variance: String,
      indirectVariance: String,
      method: String
  ): DataFrame =
    LogisticStandardization.measureTable(
      df.sparkSession,
      fit,
      LogisticStandardization.measure(
        df,
        fit,
        measure,
        effectReference(reference),
        variance,
        indirectVariance,
        method
      )
    )

  /** Tests on a measure (pprof_py's `test_standardized`); `nullValue` and `critical` NaN for the defaults. */
  def logisticTestStandardized(
      df: DataFrame,
      fit: LogisticFit,
      measure: String,
      nullValue: Double,
      transform: String,
      reference: String,
      variance: String,
      indirectVariance: String,
      alternative: String,
      level: Double,
      critical: Double,
      providersJson: String,
      method: String
  ): DataFrame =
    LogisticStandardization.test(
      df,
      fit,
      measure,
      if (nullValue.isNaN) None else Some(nullValue),
      transform,
      effectReference(reference),
      variance,
      indirectVariance,
      alternative,
      level,
      if (critical.isNaN) None else Some(critical),
      stringList(providersJson, "providers"),
      method
    )

  def logisticPredict(df: DataFrame, fit: LogisticFit): DataFrame = LogisticFE.predict(df, fit)

  def logisticSave(spark: SparkSession, fit: LogisticFit, path: String): Unit =
    LogisticFitIO.save(spark, fit, path)

  def logisticLoad(spark: SparkSession, path: String): LogisticFit = LogisticFitIO.load(spark, path)

  /** Fits the three-stage model (docs/spec/logistic/three-stage-pipeline.md §1). Column roles and options use the
    * job runner's format (docs/guide/three-stage-job.md): the `columns` and `fit` objects of a version-1 run
    * specification with `model` `three-stage`; every problem is reported at once.
    */
  def threeStageFit(df: DataFrame, columnsJson: String, fitJson: String): ThreeStageFit = {
    val (spec, options) = threeStageModel(columnsJson, fitJson)
    ThreeStagePipeline.fit(df, spec, options)
  }

  /** The column roles and options of [[threeStageFit]], through the job runner's parser. */
  def threeStageModel(
      columnsJson: String,
      fitJson: String
  ): (ThreeStageSpec, ThreeStageFitOptions) = {
    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()
    root.put("version", RunSpec.Version)
    root.put("model", "three-stage")
    root.putObject("input").put("path", "unused")
    root.set[JsonNode]("columns", objectNode(mapper, columnsJson, "columns"))
    root.set[JsonNode]("fit", objectNode(mapper, fitJson, "fit"))
    root.putObject("outputs").put("path", "unused")
    val run = ThreeStageRunSpec.parse(mapper.writeValueAsString(root))
    (run.spec, run.options)
  }

  /** The three-stage fit as JSON: counts and excluded providers, stage 1's fit (as [[logisticSummary]]), stage 2's
    * and stage 3's parameters and convergence, the column roles and the options in the job runner's format.
    * Doubles are `Double.toHexString` strings.
    */
  def threeStageSummary(fit: ThreeStageFit): String = {
    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()
    def hex(value: Double): String = java.lang.Double.toHexString(value)
    val p = fit.preparation
    root.put("kind", "ThreeStageFit")
    root.put("providers", p.providers)
    root.put("clusters", p.clusters)
    root.put("cells", p.cells)
    root.put("includedCells", p.includedCells)
    val excluded = root.putArray("excluded")
    p.excluded.foreach { case (key, records) =>
      val node = excluded.addObject()
      node.put("provider", GroupKey.value(key).toString)
      node.put("records", records)
    }
    root.set[JsonNode]("stage1", mapper.readTree(logisticSummary(p.stage1)))
    val s2 = fit.stage2
    val stage2 = root.putObject("stage2")
    stage2.put("sigmaProvider", hex(s2.sigmaProvider))
    stage2.put("sigmaCluster", hex(s2.sigmaCluster))
    stage2.put("intercept", hex(s2.intercept))
    stage2.put("deviance", hex(s2.deviance))
    stage2.put("converged", s2.converged)
    stage2.put("iterations", s2.iterations)
    stage2.put("evaluations", s2.evaluations)
    stage2.put("projectedGradient", hex(s2.projectedGradient))
    val s3 = fit.stage3
    val stage3 = root.putObject("stage3")
    stage3.put("sigma", hex(s3.sigma))
    stage3.put("converged", s3.converged)
    stage3.put("stalled", s3.stalled)
    stage3.put("iterations", s3.iterations)
    stage3.put("criterion", hex(s3.criterion))
    stage3.put("logLikelihood", hex(s3.loglik))
    stage3.put("heldAtBound", s3.held.count(identity))
    stage3.put("cells", s3.cells)
    stage3.put("bins", s3.bins)
    val columns = root.putObject("columns")
    columns.put("outcome", fit.spec.outcome)
    val features = columns.putArray("features")
    fit.spec.features.foreach(f => features.add(f))
    columns.put("provider", fit.spec.provider)
    columns.put("cluster", fit.spec.cluster)
    fit.spec.rowId.foreach(r => columns.put("rowId", r))
    val options = root.putObject("fit")
    options.put("cutoff", fit.options.preparation.cutoff)
    val o1 = fit.options.preparation.stage1
    val stage1Options = options.putObject("stage1")
    stage1Options.put("tol", hex(o1.tol))
    stage1Options.put("maxIter", o1.maxIter)
    stage1Options.put("bound", hex(o1.bound))
    stage1Options.put("backtrack", o1.backtrack)
    val o2 = fit.options.stage2
    val stage2Options = options.putObject("stage2")
    stage2Options.put("pirlsTolerance", hex(o2.pirlsTolerance))
    stage2Options.put("pirlsMaxIterations", o2.pirlsMaxIterations)
    stage2Options.put("gradientTolerance", hex(o2.gradientTolerance))
    stage2Options.put("maxIterations", o2.maxIterations)
    val o3 = fit.options.stage3
    val stage3Options = options.putObject("stage3")
    stage3Options.put("nNodes", o3.nNodes)
    stage3Options.put("maxIter", o3.maxIter)
    stage3Options.put("tol", hex(o3.tol))
    stage3Options.put("bound", hex(o3.bound))
    stage3Options.put("boundMode", o3.boundMode)
    mapper.writeValueAsString(root)
  }

  /** Stage 3's effects with stage 2's BLUPs, as the job runner writes them (`providers`). */
  def threeStageProviders(spark: SparkSession, fit: ThreeStageFit): DataFrame =
    ThreeStageJob.providersTable(spark, fit)

  /** The clusters' posterior moments with stage 2's BLUPs, as the job runner writes them (`clusters`). */
  def threeStageClusters(spark: SparkSession, fit: ThreeStageFit): DataFrame =
    ThreeStageJob.clustersTable(spark, fit)

  /** Stage 3's provider tests (§2): `exact`, `poibin_exact` or `resampling`; `critical` is NaN for none. */
  def threeStageTest(
      fit: ThreeStageFit,
      method: String,
      reference: String,
      alternative: String,
      level: Double,
      critical: Double,
      providersJson: String,
      nResample: Int,
      seed: Long
  ): DataFrame =
    ThreeStagePipeline.test(
      fit.preparation.data,
      fit.spec,
      fit.stage3,
      method,
      effectReference(reference),
      alternative,
      level,
      if (critical.isNaN) None else Some(critical),
      stringList(providersJson, "providers"),
      nResample = nResample,
      seed = seed
    )

  /** Indirect and direct standardized measures (§3), by kind. */
  def threeStageMeasures(
      fit: ThreeStageFit,
      kindsJson: String,
      reference: String
  ): java.util.Map[String, DataFrame] =
    ThreeStagePipeline
      .measures(
        fit.preparation.data,
        fit.cells,
        fit.spec,
        fit.stage3,
        stringList(kindsJson, "kinds").getOrElse(Seq("indirect", "direct")),
        effectReference(reference)
      )
      .asJava

  /** Intervals by test inversion (§3): `gamma` for the effects, `SM` for the standardized measures. */
  def threeStageIntervals(
      fit: ThreeStageFit,
      option: String,
      kindsJson: String,
      measureJson: String,
      alternative: String,
      level: Double,
      method: String,
      reference: String
  ): java.util.Map[String, DataFrame] =
    ThreeStagePipeline
      .intervals(
        fit.preparation.data,
        fit.cells,
        fit.spec,
        fit.stage3,
        option,
        stringList(kindsJson, "kinds").getOrElse(Seq("indirect")),
        stringList(measureJson, "measure").getOrElse(Seq("rate", "ratio")),
        alternative,
        level,
        method,
        effectReference(reference)
      )
      .asJava

  /** σ_c's sensitivity (§4) with the fit's stage 2 and stage 3 options. */
  def threeStageSensitivity(
      fit: ThreeStageFit,
      level: Double,
      method: String,
      alternative: String,
      testLevel: Double
  ): ThreeStageSensitivity =
    ThreeStagePipeline.sigmaSensitivity(
      fit.preparation.data,
      fit.cells,
      fit.spec,
      fit.stage2,
      level,
      method,
      alternative,
      testLevel,
      fit.options.stage2,
      fit.options.stage3
    )

  /** σ_c's profile interval and estimate as JSON, doubles as `Double.toHexString` strings. */
  def threeStageSensitivitySummary(result: ThreeStageSensitivity): String = {
    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()
    root.put("lower", java.lang.Double.toHexString(result.lower))
    root.put("estimate", java.lang.Double.toHexString(result.estimate))
    root.put("upper", java.lang.Double.toHexString(result.upper))
    mapper.writeValueAsString(root)
  }

  def threeStageSensitivityFlags(result: ThreeStageSensitivity): DataFrame = result.flags

  def threeStageSensitivityTests(result: ThreeStageSensitivity): java.util.Map[String, DataFrame] =
    result.tests.asJava

  /** The prepared training records with stage 3's fitted probability `fitted`. */
  def threeStageFitted(fit: ThreeStageFit): DataFrame =
    ThreeStage.fitted(fit.preparation.data, fit.spec, fit.stage3)

  def threeStageSave(spark: SparkSession, fit: ThreeStageFit, path: String): Unit =
    ThreeStageFitIO.save(spark, fit, path)

  /** Loads a saved fit and re-attaches it to its training data `df` (the records are not saved, §12); data whose
    * stage 1 fingerprint differs are refused.
    */
  def threeStageLoad(spark: SparkSession, path: String, df: DataFrame): ThreeStageFit =
    ThreeStageFitIO.attach(df, ThreeStageFitIO.load(spark, path))

  private def testsJson(tests: Seq[LogisticTest]): String = {
    val mapper = new ObjectMapper()
    val array = mapper.createArrayNode()
    tests.foreach { t =>
      val node = array.addObject()
      node.put("feature", t.feature)
      node.put("estimate", java.lang.Double.toHexString(t.estimate))
      node.put("standardError", java.lang.Double.toHexString(t.standardError))
      node.put("statistic", java.lang.Double.toHexString(t.statistic))
      node.put("pValue", java.lang.Double.toHexString(t.pValue))
      node.put("lower", java.lang.Double.toHexString(t.lower))
      node.put("upper", java.lang.Double.toHexString(t.upper))
      node.put("method", t.method)
    }
    mapper.writeValueAsString(array)
  }

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
