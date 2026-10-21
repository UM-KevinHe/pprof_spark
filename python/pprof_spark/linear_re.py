"""The linear random-intercept provider model through the JVM engine (docs/spec/linear/random-effect-estimation.md,
slice 4a-1)."""
import json
import math
import warnings
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame

from ._jvm import api
from .cox import _float
from .linear import LinearCoefficient

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
    follow pprof_py's `LinearRandomEffectModel`; tests, intervals and measures arrive with slice 4a-2, saving and
    loading with 4a-3."""

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

    def fitted_values(self, df: DataFrame) -> DataFrame:
        """The training rows `df` with `xbeta`, `fitted` (with the BLUPs) and `residual`."""
        return self._frame(api(self._spark).linearREFitted(df._jdf, self._handle))
