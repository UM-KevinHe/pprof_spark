"""The linear random-intercept provider model through the JVM engine (docs/spec/linear/random-effect-estimation.md,
slice 4a-1)."""
import json
import math
import warnings
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame

from ._jvm import api
from .cox import _float
from .linear import LinearCoefficient, LinearTest

@dataclass(frozen=True)
class LinearREFixedEffect:
    """One row of pprof_py's `fixed_effects_table`: t = β̂/se with max(n − p, 1) degrees of freedom."""
    feature: str
    estimate: float
    standard_error: float
    statistic: float
    degrees_of_freedom: float
    p_value: float


# pprof_py's optimizer settings, which pprof_spark's search does not use (spec 4a-1 §10), at pprof_py's defaults.
_UNUSED = {"tol_outer": 1e-8, "max_iter_inner": 100, "optimizer": "powell"}


class LinearRandomEffect:
    """Column roles and options of the linear random-intercept model, with pprof_py's `LinearRandomEffectModel`
    arguments: its `fit` roles as `features`, `offset`, `weights` and `include_intercept`, and its constructor's
    `reml`, `theta_upper` and `max_iter_outer`, which caps the search's evaluations. `tol_outer`, `max_iter_inner` and
    `optimizer` are accepted but not used, since pprof_spark's search runs to its own precision; a value other than
    pprof_py's default gives a warning. `cluster_vars` waits for part 4d (OI-93); `blocks` is the job runner's."""

    def __init__(self, outcome: str, features: Sequence[str], provider: str, *, offset: Optional[str] = None,
                 weights: Optional[str] = None, row_id: Optional[str] = None, include_intercept: bool = True,
                 reml: bool = True, theta_upper: float = math.inf, max_iter_outer: int = 200,
                 tol_outer: float = 1e-8, max_iter_inner: int = 100, optimizer: str = "powell",
                 cluster_vars: Optional[Sequence[str]] = None, blocks: Optional[dict] = None):
        if cluster_vars:
            raise NotImplementedError("cluster_vars (further random intercepts) arrive with part 4d (OI-93)")
        optimizer = str(optimizer).lower()
        if optimizer not in ("powell", "nelder-mead"):
            raise ValueError("optimizer must be 'powell' or 'nelder-mead'")
        for name, value in (("tol_outer", tol_outer), ("max_iter_inner", max_iter_inner), ("optimizer", optimizer)):
            if value != _UNUSED[name]:
                warnings.warn(f"{name}={value!r} is not used: pprof_spark's search runs to its own precision "
                              "(docs/spec/linear/random-effect-estimation.md §10)", UserWarning, stacklevel=2)
        columns = {"outcome": outcome, "features": list(features), "provider": provider, "offset": offset,
                   "weights": weights, "rowId": row_id}
        options = {"reml": bool(reml), "includeIntercept": bool(include_intercept),
                   "thetaUpper": None if math.isinf(theta_upper) else float(theta_upper),
                   "maxEvaluations": int(max_iter_outer), "blocks": blocks}
        self.columns = {k: v for k, v in columns.items() if v is not None}
        self.options = {k: v for k, v in options.items() if v is not None}

    def fit(self, df: DataFrame) -> "LinearRandomEffectModel":
        handle = api(df.sparkSession).linearREFit(df._jdf, json.dumps(self.columns), json.dumps(self.options))
        return LinearRandomEffectModel(df.sparkSession, handle)


class LinearRandomEffectModel:
    """A fitted linear random-intercept model: a handle to the JVM's `LinearREFit` and its summary. The method names
    follow pprof_py's `LinearRandomEffectModel` (docs/spec/linear/random-effect-inference.md for the inference);
    saving and loading arrive with slice 4a-3."""

    def __init__(self, spark, handle):
        self._spark = spark
        self._handle = handle
        s = json.loads(api(spark).linearRESummary(handle))
        self.summary_json = s
        self.coefficients: List[LinearCoefficient] = [
            LinearCoefficient(c["feature"], _float(c["estimate"]), _float(c["standardError"])) for c in s["coefficients"]]
        self.covariance = [_float(v) for v in s["covariance"]]
        for key, name in (("sigma", "sigma"), ("theta", "theta"), ("sigmaAlpha", "sigma_alpha"),
                          ("deviance", "deviance"), ("loglik", "loglik"), ("aic", "aic"), ("bic", "bic"),
                          ("pwrss", "pwrss"), ("logDetH", "log_det_h"), ("ussq", "ussq")):
            setattr(self, name, _float(s[key]))
        self.criterion = s["criterion"]
        self.rows = s["rows"]
        self.n_providers = s["providers"]
        self.search = dict(s["search"])
        self.warnings = list(s["warnings"])
        self._offset = s["columns"].get("offset")

    def _frame(self, jdf) -> DataFrame:
        return DataFrame(jdf, self._spark)

    def providers(self) -> DataFrame:
        """One row per provider: `records`, `weight`, `blup`, and the BLUP's conditional `variance` and `se` (X-037)."""
        return self._frame(api(self._spark).linearREProviders(self._handle))

    def get_random_effects(self) -> DataFrame:
        """The BLUPs, one row per provider (pprof_py returns them as a Series)."""
        return self.providers().select(self.summary_json["columns"]["provider"], "blup")

    def get_sigma(self) -> float:
        return self.sigma

    def residual_standard_error(self) -> float:
        return self.sigma

    def get_random_effect_sd(self) -> float:
        return self.sigma_alpha

    def standard_errors(self) -> Dict[str, float]:
        return {c.feature: c.standard_error for c in self.coefficients}

    def predict(self, df: DataFrame, use_re: bool = False, offset_var: Optional[str] = None) -> DataFrame:
        """`df` with `prediction`: xᵀβ̂, plus the offset when `offset_var` names the fit's offset column (pprof_py
        leaves it out otherwise), plus with `use_re` the BLUPs of providers in the fit and 0 for others."""
        if offset_var is not None and offset_var != self._offset:
            raise ValueError(f"offset_var must name the fit's offset column ({self._offset!r}), got {offset_var!r}")
        return self._frame(api(self._spark).linearREPredict(df._jdf, self._handle, bool(use_re),
                                                            offset_var is not None))

    @staticmethod
    def _names(providers) -> str:
        return json.dumps([str(p) for p in providers]) if providers else ""

    def _frames(self, tables) -> Dict[str, DataFrame]:
        return {key: self._frame(tables.get(key)) for key in sorted(tables.keySet())}

    def summary(self, level: float = 0.95, null: float = 0.0, alternative: str = "two_sided",
                covariates: Optional[Sequence[str]] = None) -> List[LinearTest]:
        """pprof_py's `summary`: t tests and intervals for the covariates on n − p − m degrees of freedom, with
        p-values computed from the tails directly and not rounded (X-041)."""
        text = api(self._spark).linearRECovariateTests(self._handle, float(level), float(null), alternative)
        tests = [LinearTest(t["feature"], _float(t["estimate"]), _float(t["standardError"]), _float(t["statistic"]),
                            _float(t["pValue"]), _float(t["lower"]), _float(t["upper"])) for t in json.loads(text)]
        return [t for t in tests if covariates is None or t.feature in covariates]

    def fixed_effects_table(self) -> List[LinearREFixedEffect]:
        """pprof_py's `fixed_effects_table`, one entry per fixed effect."""
        return [LinearREFixedEffect(e["feature"], _float(e["estimate"]), _float(e["standardError"]),
                                    _float(e["statistic"]), _float(e["degreesOfFreedom"]), _float(e["pValue"]))
                for e in json.loads(api(self._spark).linearREFixedEffectsTable(self._handle))]

    def conf_int(self, level: float = 0.95) -> Dict[str, Tuple[float, float]]:
        """pprof_py's `conf_int`: each fixed effect's (lower, upper) with max(n − p, 1) degrees of freedom."""
        return {e["feature"]: (_float(e["lower"]), _float(e["upper"]))
                for e in json.loads(api(self._spark).linearREConfInt(self._handle, float(level)))}

    def test(self, reference=0.0, alternative: str = "two_sided", level: float = 0.95,
             critical: Optional[float] = None, providers: Optional[Sequence] = None) -> DataFrame:
        """pprof_py's `test`: z = (b̂ⱼ − γ₀)/seⱼ against the normal, one row per provider with the provider-test
        columns; γ₀ is 0 by default, or "median", "mean" or a number."""
        return self._frame(api(self._spark).linearRETest(
            self._handle, str(reference), alternative, float(level),
            float("nan") if critical is None else float(critical), self._names(providers)))

    def confidence_intervals(self, df: Optional[DataFrame] = None, option: str = "SM",
                             kinds: Sequence[str] = ("indirect",), reference="median", alternative: str = "two_sided",
                             level: float = 0.95, providers: Optional[Sequence] = None) -> Dict[str, DataFrame]:
        """pprof_py's `calculate_confidence_intervals`, keyed as it is (`alpha_ci`, `indirect_ci`, `direct_ci`):
        option `alpha` needs only the fit and is two-sided; option `SM` needs the fit's training data `df` (API-3)."""
        names = self._names(providers)
        if option == "alpha":
            if alternative != "two_sided":
                raise ValueError("option 'alpha' supports only two-sided intervals")
            return {"alpha_ci": self._frame(api(self._spark).linearREAlphaIntervals(self._handle, float(level), names))}
        if option != "SM":
            raise ValueError("option must be 'alpha' or 'SM'")
        if df is None:
            raise ValueError("option 'SM' needs the fit's training data (API-3)")
        tables = api(self._spark).linearREIntervals(df._jdf, self._handle, json.dumps(list(kinds)), str(reference),
                                                     alternative, float(level), names)
        return {f"{kind}_ci": table for kind, table in self._frames(tables).items()}

    def standardized_measures(self, df: DataFrame, kinds: Sequence[str] = ("indirect", "direct"),
                              reference="median", providers: Optional[Sequence] = None) -> Dict[str, DataFrame]:
        """pprof_py's `calculate_standardized_measures` on the fit's training data `df` (API-3)."""
        return self._frames(api(self._spark).linearREMeasures(df._jdf, self._handle, json.dumps(list(kinds)),
                                                              str(reference), self._names(providers)))

    def fitted_values(self, df: DataFrame) -> DataFrame:
        """The training rows `df` with `xbeta`, `fitted` (with the BLUPs) and `residual`."""
        return self._frame(api(self._spark).linearREFitted(df._jdf, self._handle))
