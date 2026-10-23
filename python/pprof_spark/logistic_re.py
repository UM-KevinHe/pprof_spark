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
    method names where they apply. Inference (`summary`, tests, intervals, measures) arrives with slice 4b-2."""

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

    def fitted_values(self, df: DataFrame) -> DataFrame:
        """The training rows with `xbeta`, `eta`, `fitted` (μ̂ with the BLUPs), `residual`, `pearson` and
        `deviance_residual`."""
        return self._frame(api(self._spark).logisticREFitted(df._jdf, self._handle))
