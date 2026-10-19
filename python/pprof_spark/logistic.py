"""The logistic fixed-effect provider model through the JVM engine (docs/spec/logistic/)."""
import json
import math
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
                 max_providers_on_driver: Optional[int] = None, blocks: Optional[dict] = None):
        columns = {"outcome": outcome, "features": list(features), "provider": provider, "trials": trials,
                   "rowId": row_id, "cluster": cluster}
        options = {"tol": tol, "maxIter": max_iter, "bound": bound, "backtrack": backtrack, "screen": screen,
                   "minRecords": min_records, "confidenceLevel": confidence_level,
                   "correlationThreshold": correlation_threshold, "maxProvidersOnDriver": max_providers_on_driver,
                   "blocks": blocks}
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

    def add_providers(self, provider_ids, gamma, se_gamma=0.01, group_sizes=None) -> "LogisticFixedEffectModel":
        """Providers added after fitting, as pprof_py's `add_providers` (logistic specification §8a): each takes the
        effect `gamma` and the standard error `se_gamma` (a number, or one per provider). A provider not in the fit is
        added with `group_sizes` records (0 when None), which weight the mean reference; a fitted one is replaced and
        keeps its records and data-based statistics. Like pprof_py, this changes the model and returns it.
        Conventionally, zero-event providers take gamma -17 and all-event ones +17, with se_gamma 0.01."""
        ids = list(provider_ids)
        if not ids:
            return self
        gammas = [float(g) for g in (list(gamma) if hasattr(gamma, "__len__") else [gamma])]
        if len(gammas) == 1 and len(ids) > 1:
            gammas = gammas * len(ids)
        ses = [float(v) for v in (list(se_gamma) if hasattr(se_gamma, "__len__") else [se_gamma])]
        if len(ses) == 1 and len(ids) > 1:
            ses = ses * len(ids)
        sizes = [0] * len(ids) if group_sizes is None else [int(v) for v in group_sizes]
        keys = [v if isinstance(v, str) else int(v) for v in ids]
        self._handle = api(self._spark).logisticWithProviders(
            self._handle, json.dumps(keys), json.dumps(gammas), json.dumps(ses), json.dumps(sizes))
        return self

    def test_standardized(self, df: DataFrame, measure: str = "direct_rate", null_value="reference",
                          transform: str = "auto", reference="median", variance: str = "model",
                          indirect_variance: str = "null", alternative: str = "two_sided", level: float = 0.95,
                          critical: Optional[float] = None, providers: Optional[Sequence] = None,
                          method: str = "binned", null_model=None, interval: str = "inversion",
                          bounds="auto") -> DataFrame:
        """Tests on a standardized measure (pprof_py's `test_standardized`; standardization specification §2a).

        null_value: "reference" (or None; the measure at the reference provider effect), "mean" or "median" of the
        providers' finite estimates, a number, or a callable given those estimates as a list.
        null_model: None or TheoreticalNull() for N(0, 1); FixedNull(mean, sd), pprof_py's FixedNull, or a dict
        with mean and sd for N(mean, sd²). z is calibrated to (z - mean) / sd before p-values, flags and intervals.
        interval: "inversion" or "scale_only". bounds: "auto", a (lower, upper) pair, or None (no clipping).
        Indirect measures use `indirect_variance`; `variance` other than "model" is refused for them, as in
        pprof_py."""
        mode, value = _null_value(self, df, null_value, measure, reference, variance, indirect_variance, method)
        mean, sd = _null_model(null_model)
        bounds_mode, lower, upper = _bounds(bounds)
        names = json.dumps([str(p) for p in providers]) if providers else ""
        nan = float("nan")
        return self._frame(api(self._spark).logisticTestStandardizedWith(
            df._jdf, self._handle, measure, mode, value, transform, str(reference), variance, indirect_variance,
            alternative, float(level), nan if critical is None else float(critical), names, method,
            mean, sd, interval, bounds_mode, lower, upper))

    def predict(self, df: DataFrame) -> DataFrame:
        """`df` with `linear_predictor` and `probability`; rows of providers not in the fit raise."""
        return self._frame(api(self._spark).logisticPredict(df._jdf, self._handle))

    def save(self, path: str) -> None:
        api(self._spark).logisticSave(self._spark._jsparkSession, self._handle, path)

    @classmethod
    def load(cls, spark, path: str) -> "LogisticFixedEffectModel":
        return cls(spark, api(spark).logisticLoad(spark._jsparkSession, path))


def _null_value(model, df, null_value, measure, reference, variance, indirect_variance, method):
    """The engine's null-value mode and number (standardization specification §2a)."""
    nan = float("nan")
    if null_value is None or (isinstance(null_value, str) and null_value == "reference"):
        return "reference", nan
    if isinstance(null_value, str):
        if null_value in ("mean", "median"):
            return null_value, nan
        raise ValueError("null_value must be 'reference', 'mean', 'median', a number, or a callable.")
    if callable(null_value):
        rows = model.standardized_measure(df, measure, reference=reference, variance=variance,
                                          indirect_variance=indirect_variance, method=method).collect()
        finite = [r["estimate"] for r in rows if r["estimate"] is not None and math.isfinite(r["estimate"])]
        return "value", float(null_value(finite))
    return "value", float(null_value)


def _null_model(null_model):
    """The null's mean and sd; sd NaN for the theoretical null (standardization specification §2a)."""
    nan = float("nan")
    if null_model is None or type(null_model).__name__ == "TheoreticalNull":
        return 0.0, nan
    if isinstance(null_model, dict):
        mean, sd = null_model.get("mean", 0.0), null_model.get("sd", 1.0)
    elif hasattr(null_model, "mean") and hasattr(null_model, "sd") and not callable(null_model.mean):
        mean, sd = null_model.mean, null_model.sd
    else:
        raise TypeError("null_model must be None, TheoreticalNull(), FixedNull(mean, sd) or a dict with mean and "
                        "sd; empirical nulls are not implemented yet (OI-78)")
    mean, sd = float(mean), float(sd)
    if not (math.isfinite(mean) and math.isfinite(sd) and sd > 0):
        raise ValueError("FixedNull needs a finite mean and a positive finite sd.")
    return mean, sd


def _bounds(bounds):
    """The engine's bounds mode and limits (standardization specification §2a)."""
    nan = float("nan")
    if isinstance(bounds, str):
        if bounds != "auto":
            raise ValueError("bounds must be 'auto', a (lower, upper) pair, or None.")
        return "auto", nan, nan
    if bounds is None:
        return "none", nan, nan
    lower, upper = bounds
    return "fixed", float(lower), float(upper)
