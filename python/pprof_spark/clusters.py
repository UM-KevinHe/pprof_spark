"""The random-effect models with cluster factors, pprof_py's `cluster_vars` (docs/spec/random-effects/cluster-terms-job.md
§3; slices 4d-1 and 4d-2): `LinearRandomEffectClusterModel` and `LogisticRandomEffectClusterModel`, returned by
`LinearRandomEffect(..., cluster_vars=[...]).fit(df)` and `LogisticRandomEffect(..., cluster_vars=[...]).fit(df)`."""
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame

from ._jvm import api
from .cox import _float


@dataclass(frozen=True)
class ClusterCoefficient:
    feature: str
    estimate: float
    standard_error: float


@dataclass(frozen=True)
class ClusterTest:
    """One fixed effect's test: the estimate, its standard error, the statistic, the p-value and the interval."""
    feature: str
    estimate: float
    standard_error: float
    statistic: float
    p_value: float
    lower: float
    upper: float


def _names(values) -> str:
    return json.dumps([str(v) for v in values]) if values else ""


def _coefficients(summary) -> List[ClusterCoefficient]:
    return [ClusterCoefficient(c["feature"], _float(c["estimate"]), _float(c["standardError"])) for c in summary["coefficients"]]


class LinearRandomEffectClusterModel:
    """A fitted linear model with cluster factors (slice 4d-1), with pprof_py's names. Like pprof_py's, it gives estimates
    only: `test`, `calculate_standardized_measures` and `calculate_confidence_intervals` need a fit without cluster
    factors and raise."""

    def __init__(self, spark, handle):
        self._spark = spark
        self._handle = handle
        s = json.loads(api(spark).linearREClusterSummary(handle))
        self.factors: List[str] = list(s["factors"])
        self.theta: Dict[str, float] = dict(zip(self.factors, (_float(v) for v in s["theta"])))
        self.sigma: float = _float(s["sigma"])
        self.random_effect_sd: Dict[str, float] = {f: self.sigma * t for f, t in self.theta.items()}
        self.coefficients: List[ClusterCoefficient] = _coefficients(s)
        self.criterion: str = s["criterion"]
        self.rows: int = s["rows"]
        self.deviance, self.loglik, self.aic, self.bic = (_float(v) for v in s["statistics"])
        self.converged: bool = bool(s["converged"])
        self.boundary: List[str] = list(s["boundary"])

    def _frame(self, jdf) -> DataFrame:
        return DataFrame(jdf, self._spark)

    def get_random_effects(self, var: Optional[str] = None) -> DataFrame:
        """A factor's BLUP table (key, `records`, `weight`, `blup`); the provider's by default."""
        return self._frame(api(self._spark).linearREClusterEffects(self._handle, var or self.factors[0]))

    def get_random_effect_sd(self, var: Optional[str] = None) -> float:
        return self.random_effect_sd[var or self.factors[0]]

    def summary(self, level: float = 0.95, null: float = 0.0, alternative: str = "two_sided") -> List[ClusterTest]:
        """pprof_py's `summary`: t on n − p − (providers) degrees of freedom (X-050)."""
        rows = json.loads(api(self._spark).linearREClusterTests(self._handle, float(level), float(null), alternative))
        return [ClusterTest(r["feature"], *(_float(v) for v in r["values"])) for r in rows]

    def fixed_effects_table(self) -> List[Tuple[str, float, float, float, float, float]]:
        """pprof_py's `fixed_effects_table`: (feature, estimate, standard error, t, degrees of freedom, p)."""
        rows = json.loads(api(self._spark).linearREClusterFixedEffects(self._handle))
        return [(r["feature"], *(_float(v) for v in r["values"])) for r in rows]

    def conf_int(self, level: float = 0.95) -> List[Tuple[str, float, float]]:
        rows = json.loads(api(self._spark).linearREClusterConfInt(self._handle, float(level)))
        return [(r["feature"], *(_float(v) for v in r["values"])) for r in rows]

    def fitted_values(self, df: DataFrame) -> DataFrame:
        return self._frame(api(self._spark).linearREClusterFitted(df._jdf, self._handle))

    def predict(self, df: DataFrame, use_re: bool = False, re_vars: Optional[Sequence[str]] = None) -> DataFrame:
        """pprof_py's `predict(X, use_re, re_vars)`: `prediction` with the BLUPs of `re_vars`, or of every factor when
        `use_re` alone is set; unseen levels contribute 0."""
        factors = list(re_vars) if re_vars is not None else (self.factors if use_re else [])
        return self._frame(api(self._spark).linearREClusterPredict(df._jdf, self._handle, _names(factors)))

    def _refuse(self, *args, **kwargs):
        raise ValueError("Provider profiling with LinearRandomEffectModel needs a fit without cluster_vars, as pprof_py's")

    test = calculate_standardized_measures = calculate_confidence_intervals = _refuse

    def save(self, path: str) -> None:
        api(self._spark).linearREClusterSave(self._spark._jsparkSession, self._handle, path)

    @classmethod
    def load(cls, spark, path: str) -> "LinearRandomEffectClusterModel":
        return cls(spark, api(spark).linearREClusterLoad(spark._jsparkSession, path))


class LogisticRandomEffectClusterModel:
    """A fitted logistic model with cluster factors (slice 4d-2), with pprof_py's names. The fit holds its rows on the
    driver, so the inference methods take no data. A model loaded without its data gives its tables and `summary`;
    its other methods raise until it is loaded with the training data."""

    def __init__(self, spark, handle, saved=None):
        self._spark = spark
        self._handle = handle
        self._saved = saved
        facade = api(spark)
        s = json.loads(facade.logisticREClusterSummary(handle) if handle is not None else facade.logisticREClusterSavedSummary(saved))
        self.factors: List[str] = list(s["factors"])
        self.sigma: Dict[str, float] = dict(zip(self.factors, (_float(v) for v in s["sigma"])))
        self.coefficients: List[ClusterCoefficient] = _coefficients(s)
        self.deviance, self.loglik, self.aic, self.bic = (_float(v) for v in s["statistics"])
        self.converged: bool = bool(s["converged"])
        self.boundary: List[str] = list(s["boundary"])

    def _frame(self, jdf) -> DataFrame:
        return DataFrame(jdf, self._spark)

    def _frames(self, tables) -> Dict[str, DataFrame]:
        return {key: self._frame(tables.get(key)) for key in sorted(tables.keySet())}

    def _fit(self):
        if self._handle is None:
            raise RuntimeError("This fit was loaded without its training data; LogisticRandomEffectClusterModel.load(spark, "
                               "path, df) re-attaches them, checked against the fit's rows and fingerprint.")
        return self._handle

    def get_random_effects(self, var: Optional[str] = None) -> DataFrame:
        """A factor's table (key, `records`, `blup`, `variance`, `se`); the provider's by default."""
        name = var or self.factors[0]
        facade = api(self._spark)
        return self._frame(facade.logisticREClusterEffects(self._handle, name) if self._handle is not None
                           else facade.logisticREClusterSavedEffects(self._saved, name))

    def summary(self, level: float = 0.95) -> List[ClusterTest]:
        """pprof_py's `summary`: Wald z for the fixed effects."""
        rows = json.loads(api(self._spark).logisticREClusterCovariateTests(self._fit(), float(level)))
        return [ClusterTest(r["feature"], *(_float(v) for v in r["values"])) for r in rows]

    def test(self, providers=None, test_method: str = "wald", reference=0.0, null_model=None, alternative: str = "two_sided",
             level: float = 0.95, critical: Optional[float] = None, n_resample: int = 10000, seed: int = 0) -> DataFrame:
        """pprof_py's `test` with cluster factors: `wald`, `poibin_exact`, `resampling`, and `exact` with one cluster
        factor; `reference` 0 by default, as pprof_py's."""
        if null_model is not None:
            raise NotImplementedError("null_model arrives with OI-87")
        table = self._frame(api(self._spark).logisticREClusterTest(
            self._fit(), test_method, str(reference), alternative, float(level),
            float("nan") if critical is None else float(critical), int(n_resample), int(seed)))
        if providers:
            table = table.filter(table[table.columns[0]].cast("string").isin([str(p) for p in providers]))
        return table

    def calculate_standardized_measures(self, providers=None, stdz="indirect", reference="median") -> Dict[str, DataFrame]:
        kinds = [stdz] if isinstance(stdz, str) else list(stdz)
        tables = self._frames(api(self._spark).logisticREClusterMeasures(self._fit(), json.dumps(kinds), str(reference)))
        if providers:
            wanted = [str(p) for p in providers]
            tables = {k: t.filter(t[t.columns[0]].cast("string").isin(wanted)) for k, t in tables.items()}
        return tables

    def calculate_confidence_intervals(self, providers=None, level: float = 0.95, option: str = "SM", stdz="indirect",
                                       reference="median", measure=("rate", "ratio"),
                                       alternative: str = "two_sided") -> Dict[str, DataFrame]:
        """Option `alpha` (`alpha_ci`, two-sided) or `SM` (`indirect_ratio`, ...)."""
        if option == "alpha":
            if alternative != "two_sided":
                raise ValueError("option 'alpha' supports only two-sided intervals")
            tables = {"alpha_ci": self._frame(api(self._spark).logisticREClusterAlphaIntervals(self._fit(), float(level)))}
        elif option == "SM":
            kinds = [stdz] if isinstance(stdz, str) else list(stdz)
            measures = [measure] if isinstance(measure, str) else list(measure)
            tables = self._frames(api(self._spark).logisticREClusterIntervals(
                self._fit(), json.dumps(kinds), json.dumps(measures), str(reference), alternative, float(level)))
            tables = {k: t.withColumnRenamed("value", k) for k, t in tables.items()}
        else:
            raise ValueError("option must be 'alpha' or 'SM'")
        if providers:
            wanted = [str(p) for p in providers]
            tables = {k: t.filter(t[t.columns[0]].cast("string").isin(wanted)) for k, t in tables.items()}
        return tables

    def profile_sigma(self, var: Optional[str] = None, level: float = 0.95) -> Dict[str, Dict[str, float]]:
        """pprof_py's `profile_sigma(var)`: each named factor's σ̂ with its profile-likelihood limits."""
        names = self.factors if var is None else ([var] if isinstance(var, str) else list(var))
        out = {}
        for name in names:
            r = json.loads(api(self._spark).logisticREClusterProfile(self._fit(), name, float(level)))
            out[name] = {"sigma": _float(r["sigma"]), "lower": _float(r["lower"]), "upper": _float(r["upper"])}
        return out

    def fitted_values(self, df: DataFrame) -> DataFrame:
        return self._frame(api(self._spark).logisticREClusterFitted(df._jdf, self._fit()))

    def predict(self, df: DataFrame, use_re: bool = False, re_vars: Optional[Sequence[str]] = None,
                type: str = "response", with_offset: bool = True) -> DataFrame:
        factors = list(re_vars) if re_vars is not None else (self.factors if use_re else [])
        return self._frame(api(self._spark).logisticREClusterPredict(df._jdf, self._fit(), _names(factors),
                                                                    bool(with_offset), type))

    def save(self, path: str) -> None:
        api(self._spark).logisticREClusterSave(self._spark._jsparkSession, self._fit(), path)

    @classmethod
    def load(cls, spark, path: str, df: Optional[DataFrame] = None) -> "LogisticRandomEffectClusterModel":
        facade = api(spark)
        saved = facade.logisticREClusterLoad(spark._jsparkSession, path)
        if df is not None:
            return cls(spark, facade.logisticREClusterAttach(df._jdf, saved))
        return cls(spark, None, saved)
