"""Cox proportional hazards through the JVM engine (docs/spec/cox/, docs/guide/cox-job.md)."""
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame

from ._jvm import api


def _float(value: str) -> float:
    """Doubles arrive as Java hexadecimal strings, which parse back bit for bit."""
    return float.fromhex(value)


@dataclass(frozen=True)
class Coefficient:
    feature: str
    estimate: float
    standard_error: float
    z: float
    p_value: float
    lower: float
    upper: float


class CoxPH:
    """Column roles and fit options, as the `columns` and `fit` objects of the job runner's run
    specification; options left as None take the engine's defaults."""

    def __init__(self, time: str, event: str, features: Sequence[str], *, strata: Optional[str] = None,
                 row_id: Optional[str] = None, weight: Optional[str] = None, offset: Optional[str] = None,
                 entry: Optional[str] = None, cluster: Optional[str] = None, ties: Optional[str] = None,
                 robust: Optional[bool] = None, max_iterations: Optional[int] = None,
                 eps: Optional[float] = None, max_halvings: Optional[int] = None,
                 confidence_level: Optional[float] = None, max_stratum_rows: Optional[int] = None,
                 blocks: Optional[dict] = None):
        columns = {"time": time, "event": event, "features": list(features), "strata": strata,
                   "rowId": row_id, "weight": weight, "offset": offset, "entry": entry, "cluster": cluster}
        fit = {"ties": ties, "robust": robust, "maxIterations": max_iterations, "eps": eps,
               "maxHalvings": max_halvings, "confidenceLevel": confidence_level,
               "maxStratumRows": max_stratum_rows, "blocks": blocks}
        self.columns = {k: v for k, v in columns.items() if v is not None}
        self.options = {k: v for k, v in fit.items() if v is not None}

    def fit(self, df: DataFrame) -> "CoxPHModel":
        facade = api(df.sparkSession)
        handle = facade.coxFit(df._jdf, json.dumps(self.columns), json.dumps(self.options))
        return CoxPHModel(df.sparkSession, handle)


class CoxPHModel:
    """A fitted Cox model: a handle to the JVM's `CoxFit` and its summary."""

    def __init__(self, spark, handle):
        self._spark = spark
        self._handle = handle
        summary = json.loads(api(spark).coxSummary(handle))
        self.summary = summary
        self.coefficients: List[Coefficient] = [
            Coefficient(c["feature"], _float(c["estimate"]), _float(c["standardError"]), _float(c["z"]),
                        _float(c["pValue"]), _float(c["lower"]), _float(c["upper"]))
            for c in summary["coefficients"]
        ]
        self.coef_ = [c.estimate for c in self.coefficients]
        packed = [_float(v) for v in summary["covariance"]]
        p = len(self.coefficients)
        self.covariance = [[0.0] * p for _ in range(p)]
        index = 0
        for i in range(p):
            for j in range(i, p):
                self.covariance[i][j] = self.covariance[j][i] = packed[index]
                index += 1
        self.log_likelihood = _float(summary["logLikelihood"])
        self.log_likelihood_null = _float(summary["logLikelihoodNull"])
        for key, name in (("iterations", "iterations"), ("halvings", "halvings"), ("converged", "converged"),
                          ("message", "message"), ("observations", "observations"), ("events", "events"),
                          ("strata", "strata"), ("ties", "ties"), ("robust", "robust"), ("warnings", "warnings"),
                          ("fingerprint", "fingerprint"), ("featureStatus", "feature_status"),
                          ("software", "software"), ("columns", "columns")):
            setattr(self, name, summary[key])

    def _frame(self, jdf) -> DataFrame:
        return DataFrame(jdf, self._spark)

    def baseline(self, df: DataFrame) -> DataFrame:
        """The per-stratum baseline hazard from the training data `df` (Phase 1b §2)."""
        return self._frame(api(self._spark).coxBaseline(df._jdf, self._handle))

    def linear_predictor(self, df: DataFrame) -> DataFrame:
        return self._frame(api(self._spark).coxLinearPredictor(self._handle, df._jdf))

    def relative_hazard(self, df: DataFrame) -> DataFrame:
        return self._frame(api(self._spark).coxRelativeHazard(self._handle, df._jdf))

    def cumulative_hazard(self, df: DataFrame, baseline: DataFrame, time_col: str) -> DataFrame:
        return self._frame(api(self._spark).coxCumulativeHazard(self._handle, baseline._jdf, df._jdf, time_col))

    def survival(self, df: DataFrame, baseline: DataFrame, time_col: str) -> DataFrame:
        return self._frame(api(self._spark).coxSurvival(self._handle, baseline._jdf, df._jdf, time_col))

    def residuals(self, df: DataFrame) -> DataFrame:
        """Martingale, score and dfbeta residuals, keyed by the row identifier (Phase 1c)."""
        return self._frame(api(self._spark).coxResiduals(df._jdf, self._handle))

    def standardized_measures(self, df: DataFrame, provider: str,
                              kinds: Sequence[str] = ("indirect",)) -> Dict[str, DataFrame]:
        """Indirect and direct standardized measures by provider (Phase 1d §1)."""
        facade = api(self._spark)
        return {kind: self._frame(facade.coxMeasures(df._jdf, self._handle, provider, kind)) for kind in kinds}

    def provider_tests(self, df: DataFrame, provider: str, method: str = "midp", level: float = 0.95) -> DataFrame:
        """Mid-p or exact Poisson tests, flags and limits by provider (Phase 1d §2)."""
        return self._frame(api(self._spark).coxProviderTests(df._jdf, self._handle, provider, method, float(level)))

    def save(self, path: str, baseline: Optional[DataFrame] = None) -> None:
        """Saves the fit, and the baseline when given, under a path that must not exist (`CoxFitIO`)."""
        api(self._spark).coxSave(self._spark._jsparkSession, self._handle, path,
                                 None if baseline is None else baseline._jdf)

    @classmethod
    def load(cls, spark, path: str) -> "CoxPHModel":
        return cls(spark, api(spark).coxLoad(spark._jsparkSession, path))

    @staticmethod
    def load_baseline(spark, path: str) -> Optional[DataFrame]:
        jdf = api(spark).coxLoadBaseline(spark._jsparkSession, path)
        return None if jdf is None else DataFrame(jdf, spark)
