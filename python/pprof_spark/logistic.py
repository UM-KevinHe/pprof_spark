"""The logistic fixed-effect provider model through the JVM engine (docs/spec/logistic/)."""
import json
from dataclasses import dataclass
from typing import List, Optional, Sequence

from pyspark.sql import DataFrame

from ._jvm import api
from .cox import Coefficient, _float


@dataclass(frozen=True)
class Test:
    """One covariate's test: `method` is `wald`, `wald-robust`, `lr` or `score`; intervals are Wald-based."""
    feature: str
    estimate: float
    standard_error: float
    statistic: float
    p_value: float
    lower: float
    upper: float
    method: str


def _tests(text: str) -> List[Test]:
    return [Test(t["feature"], _float(t["estimate"]), _float(t["standardError"]), _float(t["statistic"]),
                 _float(t["pValue"]), _float(t["lower"]), _float(t["upper"]), t["method"]) for t in json.loads(text)]


def _square(packed: List[float], p: int) -> List[List[float]]:
    out = [[0.0] * p for _ in range(p)]
    index = 0
    for i in range(p):
        for j in range(i, p):
            out[i][j] = out[j][i] = packed[index]
            index += 1
    return out


class LogisticFixedEffect:
    """Column roles and options of the logistic fixed-effect model; options left as None take the engine's
    defaults (pprof_py v0.7.0's)."""

    def __init__(self, outcome: str, features: Sequence[str], provider: str, *, trials: Optional[str] = None,
                 row_id: Optional[str] = None, cluster: Optional[str] = None, tol: Optional[float] = None,
                 max_iter: Optional[int] = None, bound: Optional[float] = None, backtrack: Optional[bool] = None,
                 screen: Optional[bool] = None, min_records: Optional[int] = None,
                 confidence_level: Optional[float] = None, correlation_threshold: Optional[float] = None,
                 max_providers_on_driver: Optional[int] = None):
        columns = {"outcome": outcome, "features": list(features), "provider": provider, "trials": trials,
                   "rowId": row_id, "cluster": cluster}
        options = {"tol": tol, "maxIter": max_iter, "bound": bound, "backtrack": backtrack, "screen": screen,
                   "minRecords": min_records, "confidenceLevel": confidence_level,
                   "correlationThreshold": correlation_threshold, "maxProvidersOnDriver": max_providers_on_driver}
        self.columns = {k: v for k, v in columns.items() if v is not None}
        self.options = {k: v for k, v in options.items() if v is not None}

    def fit(self, df: DataFrame) -> "LogisticFixedEffectModel":
        facade = api(df.sparkSession)
        handle = facade.logisticFit(df._jdf, json.dumps(self.columns), json.dumps(self.options))
        return LogisticFixedEffectModel(df.sparkSession, handle)


class LogisticFixedEffectModel:
    """A fitted logistic fixed-effect model: a handle to the JVM's `LogisticFit` and its summary."""

    def __init__(self, spark, handle):
        self._spark = spark
        self._handle = handle
        s = json.loads(api(spark).logisticSummary(handle))
        self.summary = s
        self.coefficients: List[Coefficient] = [
            Coefficient(c["feature"], _float(c["estimate"]), _float(c["standardError"]), _float(c["z"]),
                        _float(c["pValue"]), _float(c["lower"]), _float(c["upper"]))
            for c in s["coefficients"]
        ]
        self.coef_ = [c.estimate for c in self.coefficients]
        p = len(self.coefficients)
        self.covariance = _square([_float(v) for v in s["covariance"]], p)
        self.robust_covariance = (_square([_float(v) for v in s["robustCovariance"]], p)
                                  if "robustCovariance" in s else None)
        self.log_likelihood = _float(s["logLikelihood"])
        self.aic = _float(s["aic"])
        self.bic = _float(s["bic"])
        self.auc = _float(s["auc"]) if "auc" in s else None
        self.trials = _float(s["trials"])
        self.events = _float(s["events"])
        self.excluded = [(e["provider"], e["records"]) for e in s["excluded"]]
        for key, name in (("iterations", "iterations"), ("converged", "converged"), ("rows", "rows"),
                          ("providers", "n_providers"), ("degenerateProviders", "degenerate_providers"),
                          ("clusters", "clusters"), ("fingerprint", "fingerprint"), ("warnings", "warnings"),
                          ("columns", "columns"), ("software", "software"), ("featureStatus", "feature_status")):
            setattr(self, name, s[key])

    def _frame(self, jdf) -> DataFrame:
        return DataFrame(jdf, self._spark)

    def providers(self) -> DataFrame:
        """One row per fitted provider: effect, variances (robust ones with clusters), counts, flags."""
        return self._frame(api(self._spark).logisticProviders(self._spark._jsparkSession, self._handle))

    def wald_tests(self, null: float = 0.0, alternative: str = "two_sided", level: float = 0.95,
                   robust: bool = False) -> List[Test]:
        return _tests(api(self._spark).logisticWald(self._handle, float(null), alternative, float(level), robust))

    def covariate_tests(self, df: DataFrame, method: str = "lr",
                        covariates: Optional[Sequence[str]] = None) -> List[Test]:
        """Likelihood-ratio (`lr`) or score (`score`) tests by refits on the training data `df`."""
        names = json.dumps(list(covariates) if covariates else [])
        return _tests(api(self._spark).logisticTests(df._jdf, self._handle, method, names))

    def provider_tests(self, df: DataFrame, method: str = "poibin_exact", reference="median",
                       alternative: str = "two_sided", level: float = 0.95, critical: Optional[float] = None,
                       providers: Optional[Sequence] = None, n_resample: int = 10000, seed: int = 0) -> DataFrame:
        """Provider tests (slice 2c): `poibin_exact`, `score`, `wald` or `bootstrap_exact` against the
        reference effect (`median`, `mean` or a number); one row per provider with pprof_py's
        `PROVIDER_TEST_COLUMNS` plus observed, expected, trials and records."""
        names = json.dumps([str(p) for p in providers]) if providers else ""
        return self._frame(api(self._spark).logisticProviderTests(
            df._jdf, self._handle, method, str(reference), alternative, float(level),
            float("nan") if critical is None else float(critical), names, int(n_resample), int(seed)))

    def standardized_measures(self, df: DataFrame, kinds: Sequence[str] = ("indirect", "direct"),
                              reference="median", providers: Optional[Sequence] = None, extreme_trials: float = 0.0,
                              method: str = "binned") -> dict:
        """Indirect and direct standardized ratios and rates (rates in percent), as pprof_py's
        `calculate_standardized_measures`; `method` is `binned` (X-026) or `exact`."""
        names = json.dumps([str(p) for p in providers]) if providers else ""
        tables = api(self._spark).logisticStandardizedMeasures(
            df._jdf, self._handle, json.dumps(list(kinds)), str(reference), names, float(extreme_trials), method)
        return {kind: self._frame(tables.get(kind)) for kind in kinds}

    def standardized_measure(self, df: DataFrame, measure: str = "direct_rate", reference="median",
                             variance: str = "model", indirect_variance: str = "null",
                             method: str = "binned") -> DataFrame:
        """One measure per provider: estimate, se and the reference value (pprof_py's `standardized_measure`)."""
        return self._frame(api(self._spark).logisticStandardizedMeasure(
            df._jdf, self._handle, measure, str(reference), variance, indirect_variance, method))

    def test_standardized(self, df: DataFrame, measure: str = "direct_rate", null_value: Optional[float] = None,
                          transform: str = "auto", reference="median", variance: str = "model",
                          indirect_variance: str = "null", alternative: str = "two_sided", level: float = 0.95,
                          critical: Optional[float] = None, providers: Optional[Sequence] = None,
                          method: str = "binned") -> DataFrame:
        """Tests on a standardized measure (pprof_py's `test_standardized`, theoretical null)."""
        names = json.dumps([str(p) for p in providers]) if providers else ""
        nan = float("nan")
        return self._frame(api(self._spark).logisticTestStandardized(
            df._jdf, self._handle, measure, nan if null_value is None else float(null_value), transform,
            str(reference), variance, indirect_variance, alternative, float(level),
            nan if critical is None else float(critical), names, method))

    def predict(self, df: DataFrame) -> DataFrame:
        """`df` with `linear_predictor` and `probability`; rows of providers not in the fit raise."""
        return self._frame(api(self._spark).logisticPredict(df._jdf, self._handle))

    def save(self, path: str) -> None:
        api(self._spark).logisticSave(self._spark._jsparkSession, self._handle, path)

    @classmethod
    def load(cls, spark, path: str) -> "LogisticFixedEffectModel":
        return cls(spark, api(spark).logisticLoad(spark._jsparkSession, path))
