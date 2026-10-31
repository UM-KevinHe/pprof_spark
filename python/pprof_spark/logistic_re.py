"""The logistic random-intercept provider model through the JVM engine (docs/spec/logistic/random-effect-estimation.md,
slice 4b-1)."""
import json
import math
import warnings
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame

from ._jvm import api
from .cox import _float


@dataclass(frozen=True)
class LogisticRETest:
    """One fixed effect's Wald test (pprof_py's `summary` row)."""
    feature: str
    estimate: float
    standard_error: float
    statistic: float
    p_value: float
    lower: float
    upper: float


@dataclass(frozen=True)
class LogisticRECoefficient:
    feature: str
    estimate: float
    standard_error: float


# pprof_py's settings that pprof_spark's searches do not use (spec 4b-1 §9, §15), at pprof_py's defaults.
_UNUSED = {"max_iter_pirls": 100, "tol_pirls": 1e-8, "tol_outer": 1e-7, "optimizer_stage1": "bobyqa",
           "optimizer_stage2": "nelder-mead"}


class LogisticRandomEffect:
    """Column roles and options of the logistic random-intercept model, with pprof_py's `LogisticRandomEffectModel`
    arguments: its `fit` roles as `features`, `offset` and `include_intercept`, and its constructor's `stage2` (nAGQ = 1
    when true, the default; nAGQ = 0 otherwise), `sigma_upper` and `max_iter_outer`, which caps the quasi-Newton's
    iterations. `max_iter_pirls`, `tol_pirls`, `tol_outer`, `optimizer_stage1` and `optimizer_stage2` are accepted but
    not used, since pprof_spark's searches run to their own precision; a value other than pprof_py's default gives a
    warning. A fractional outcome counts as one observation with a Bernoulli-like likelihood, as in pprof_py (spec
    §14.3). `cluster_vars` waits for part 4d (OI-93); `blocks` is the job runner's."""

    def __init__(self, outcome: str, features: Sequence[str], provider: str, *, offset: Optional[str] = None,
                 row_id: Optional[str] = None, include_intercept: bool = True, stage2: bool = True,
                 sigma_upper: float = math.inf, max_iter_outer: int = 200, max_iter_pirls: int = 100,
                 tol_pirls: float = 1e-8, tol_outer: float = 1e-7, optimizer_stage1: str = "bobyqa",
                 optimizer_stage2: str = "nelder-mead", verbose: bool = False,
                 cluster_vars: Optional[Sequence[str]] = None, blocks: Optional[dict] = None):
        if cluster_vars:
            raise NotImplementedError("cluster_vars (further random intercepts) arrive with part 4d (OI-93)")
        if not (sigma_upper > 0):
            raise ValueError("sigma_upper must be positive or math.inf")
        given = {"max_iter_pirls": max_iter_pirls, "tol_pirls": tol_pirls, "tol_outer": tol_outer,
                 "optimizer_stage1": str(optimizer_stage1).lower(), "optimizer_stage2": str(optimizer_stage2).lower()}
        for name, value in given.items():
            if value != _UNUSED[name]:
                warnings.warn(f"{name}={value!r} is not used: pprof_spark's searches run to their own precision "
                              "(docs/spec/logistic/random-effect-estimation.md §9)", UserWarning, stacklevel=2)
        columns = {"outcome": outcome, "features": list(features), "provider": provider, "offset": offset,
                   "rowId": row_id}
        options = {"stage2": bool(stage2), "includeIntercept": bool(include_intercept),
                   "sigmaUpper": None if math.isinf(sigma_upper) else float(sigma_upper),
                   "maxIterations": int(max_iter_outer), "blocks": blocks}
        self.columns = {k: v for k, v in columns.items() if v is not None}
        self.options = {k: v for k, v in options.items() if v is not None}

    def fit(self, df: DataFrame) -> "LogisticRandomEffectModel":
        handle = api(df.sparkSession).logisticREFit(df._jdf, json.dumps(self.columns), json.dumps(self.options))
        return LogisticRandomEffectModel(df.sparkSession, handle)


class LogisticRandomEffectModel:
    """A fitted logistic random-intercept model: a handle to the JVM's `LogisticREFit` and its summary, with pprof_py's
    method names (docs/spec/logistic/random-effect-inference.md for the inference). Methods that read rows take the
    fit's training data `df`, checked against the fit (API-3). `save` and `load` keep a fit
    (docs/spec/logistic/random-effect-job.md)."""

    def __init__(self, spark, handle):
        self._spark = spark
        self._handle = handle
        s = json.loads(api(spark).logisticRESummary(handle))
        self.summary_json = s
        self.coefficients: List[LogisticRECoefficient] = [
            LogisticRECoefficient(c["feature"], _float(c["estimate"]), _float(c["standardError"]))
            for c in s["coefficients"]]
        self.covariance = [_float(v) for v in s["covariance"]]
        for key, name in (("sigma", "sigma"), ("deviance", "deviance"), ("loglik", "loglik"), ("aic", "aic"),
                          ("bic", "bic"), ("pwrss", "pwrss"), ("logDetH", "log_det_h"), ("ussq", "ussq")):
            setattr(self, name, _float(s[key]))
        self.criterion = s["criterion"]
        self.rows = s["rows"]
        self.n_providers = s["providers"]
        self.search = dict(s["search"])
        self.converged = bool(s["search"]["converged"])
        self.warnings = list(s["warnings"])
        self._offset = s["columns"].get("offset")

    def _frame(self, jdf) -> DataFrame:
        return DataFrame(jdf, self._spark)

    def providers(self) -> DataFrame:
        """One row per provider: `records`, `weight` (Wⱼ at the mode), `blup`, and its conditional `variance` and `se`."""
        return self._frame(api(self._spark).logisticREProviders(self._handle))

    def get_random_effects(self) -> DataFrame:
        """The BLUPs, one row per provider (pprof_py returns them as a Series)."""
        return self.providers().select(self.summary_json["columns"]["provider"], "blup")

    def get_sigma(self) -> float:
        return self.sigma

    def standard_errors(self) -> Dict[str, float]:
        return {c.feature: c.standard_error for c in self.coefficients}

    def predict(self, df: DataFrame, use_re: bool = False, type: str = "response",
                offset_var: Optional[str] = None) -> DataFrame:
        """`df` with `prediction` on the response scale (`type="link"` for the linear predictor): the fixed effects,
        the offset when `offset_var` names the fit's offset column (pprof_py leaves it out otherwise), and with
        `use_re` the BLUPs of providers in the fit and 0 for others, under any key type (X-044)."""
        if offset_var is not None and offset_var != self._offset:
            raise ValueError(f"offset_var must name the fit's offset column ({self._offset!r}), got {offset_var!r}")
        if type not in ("response", "link"):
            raise ValueError("type must be 'response' or 'link'")
        return self._frame(api(self._spark).logisticREPredict(df._jdf, self._handle, bool(use_re),
                                                              offset_var is not None, type))

    def _names(self, values) -> str:
        return json.dumps([str(v) for v in values]) if values else ""

    def _keep(self, frame: DataFrame, providers) -> DataFrame:
        if not providers:
            return frame
        key = self.summary_json["columns"]["provider"]
        return frame.filter(frame[key].cast("string").isin([str(p) for p in providers]))

    def summary(self, level: float = 0.95) -> List[LogisticRETest]:
        """pprof_py's `summary`: Wald z for the fixed effects on the normal (spec 4b-2 §2)."""
        rows = json.loads(api(self._spark).logisticRECovariateTests(self._handle, float(level)))
        return [LogisticRETest(r["feature"], _float(r["estimate"]), _float(r["standardError"]), _float(r["statistic"]),
                               _float(r["pValue"]), _float(r["lower"]), _float(r["upper"])) for r in rows]

    def test(self, df: Optional[DataFrame] = None, providers=None, test_method: str = "wald", reference=0.0,
             null_model=None, alternative: str = "two_sided", level: float = 0.95, critical: Optional[float] = None,
             interval: str = "inversion", n_resample: int = 10000, seed: int = 0) -> DataFrame:
        """pprof_py's `test` (spec 4b-2 §3): `wald` (the default), `poibin_exact` and `resampling`, the latter two on
        the training data `df`; slice 2c's table, one row per provider. `exact` needs a cluster factor (part 4d),
        `null_model` waits for OI-87, and only `interval="inversion"` is supported."""
        if null_model is not None:
            raise NotImplementedError("null_model arrives with OI-87")
        if test_method == "exact":
            raise NotImplementedError("test_method='exact' needs a cluster factor besides the provider (part 4d, OI-93)")
        if interval != "inversion":
            raise ValueError("only interval='inversion' is supported")
        if test_method != "wald" and df is None:
            raise ValueError(f"test_method={test_method!r} needs the fit's training data (API-3)")
        jdf = None if df is None else df._jdf
        return self._frame(api(self._spark).logisticRETest(
            jdf, self._handle, test_method, str(reference), alternative, float(level),
            float("nan") if critical is None else float(critical), self._names(providers), int(n_resample), int(seed)))

    def calculate_confidence_intervals(self, df: Optional[DataFrame] = None, providers=None, level: float = 0.95,
                                       option: str = "SM", stdz="indirect", reference="median",
                                       measure=("rate", "ratio"), alternative: str = "two_sided") -> Dict[str, DataFrame]:
        """pprof_py's `calculate_confidence_intervals` (spec 4b-2 §4), keyed as pprof_py: `alpha_ci` for option
        `alpha` (two-sided); `indirect_ratio`, `indirect_rate`, `direct_ratio` and `direct_rate` for option `SM`,
        which needs the training data `df`. One-sided indirect intervals are one-sided (X-045), and `mean` is the
        size-weighted mean of the BLUPs (X-046)."""
        if option == "alpha":
            if alternative != "two_sided":
                raise ValueError("option 'alpha' supports only two-sided intervals")
            return {"alpha_ci": self._keep(self._frame(
                api(self._spark).logisticREAlphaIntervals(self._handle, float(level))), providers)}
        if option != "SM":
            raise ValueError("option must be 'alpha' or 'SM'")
        if df is None:
            raise ValueError("option 'SM' needs the fit's training data (API-3)")
        kinds = [stdz] if isinstance(stdz, str) else list(stdz)
        measures = [measure] if isinstance(measure, str) else list(measure)
        tables = api(self._spark).logisticREIntervals(df._jdf, self._handle, json.dumps(kinds), json.dumps(measures),
                                                      str(reference), alternative, float(level))
        out = {}
        for key in sorted(tables.keySet()):
            frame = self._frame(tables.get(key)).withColumnRenamed("value", key)
            out[key] = self._keep(frame, providers)
        return out

    def calculate_standardized_measures(self, df: DataFrame, providers=None, stdz="indirect",
                                        reference="median") -> Dict[str, DataFrame]:
        """pprof_py's `calculate_standardized_measures` (spec 4b-2 §5) on the training data `df`: `indirect`
        (ratio, rate, observed, expected) and `direct` tables."""
        kinds = [stdz] if isinstance(stdz, str) else list(stdz)
        tables = api(self._spark).logisticREMeasures(df._jdf, self._handle, json.dumps(kinds), str(reference))
        return {key: self._keep(self._frame(tables.get(key)), providers) for key in sorted(tables.keySet())}

    def profile_sigma(self, df: DataFrame, var: Optional[str] = None, level: float = 0.95) -> Dict[str, float]:
        """pprof_py's `profile_sigma` (spec 4b-2 §6): σ̂ with its profile-likelihood limits, on the training data
        `df`; the Laplace fit only."""
        provider = self.summary_json["columns"]["provider"]
        if var is not None and var != provider:
            raise ValueError(f"var must name the provider column ({provider!r}) until part 4d")
        r = json.loads(api(self._spark).logisticREProfile(df._jdf, self._handle, float(level)))
        return {"sigma": _float(r["sigma"]), "lower": _float(r["lower"]), "upper": _float(r["upper"])}

    def save(self, path: str) -> None:
        """Saves the fit (`LogisticREFitIO`, format version 1); the path must not exist."""
        api(self._spark).logisticRESave(self._spark._jsparkSession, self._handle, path)

    @classmethod
    def load(cls, spark, path: str) -> "LogisticRandomEffectModel":
        """Loads a fit saved by `save` or by `LogisticREJob`, bit for bit."""
        return cls(spark, api(spark).logisticRELoad(spark._jsparkSession, path))

    def fitted_values(self, df: DataFrame) -> DataFrame:
        """The training rows with `xbeta`, `eta`, `fitted` (μ̂ with the BLUPs), `residual`, `pearson` and
        `deviance_residual`."""
        return self._frame(api(self._spark).logisticREFitted(df._jdf, self._handle))
