"""Fixed provider effects with random cluster effects through the JVM engine (docs/spec/logistic/fe-random-cluster.md,
slice 4c-1): pprof_py's `LogisticFERandomClusterModel`, over the three-stage model's stage 3."""
import json
import warnings
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame

from ._jvm import api
from .cox import _float
from .logistic_re import LogisticRETest


class LogisticFERandomCluster:
    """Column roles and options with pprof_py's `LogisticFERandomClusterModel` names: `n_nodes`, `max_iter`, `tol`,
    `bound`, `bound_mode`, `estimator` (`marginal`, the default, or `he2013`, He et al.'s iteration, slice 4c-2) and
    `convergence_criterion` (`max_delta_gamma` or `relative`, for `he2013`). `observed` is pprof_py's `obs_var`, the
    outcome the tests count."""

    def __init__(self, outcome: str, features: Sequence[str], provider: str, cluster: str, *,
                 observed: Optional[str] = None, row_id: Optional[str] = None, n_nodes: int = 20,
                 max_iter: int = 10000, tol: float = 1e-5, bound: float = 10.0, bound_mode: str = "relative",
                 estimator: str = "marginal", convergence_criterion: str = "max_delta_gamma"):
        if estimator not in ("marginal", "he2013"):
            raise ValueError("estimator must be 'marginal' or 'he2013'")
        if convergence_criterion not in ("max_delta_gamma", "relative"):
            raise ValueError("convergence_criterion must be 'max_delta_gamma' or 'relative'")
        if estimator == "marginal" and convergence_criterion != "max_delta_gamma":
            warnings.warn("convergence_criterion applies to estimator='he2013' only", UserWarning, stacklevel=2)
        columns = {"outcome": outcome, "features": list(features), "provider": provider, "cluster": cluster,
                   "observed": observed, "rowId": row_id}
        self.columns = {k: v for k, v in columns.items() if v is not None}
        self.options = {"nNodes": int(n_nodes), "maxIter": int(max_iter), "tol": float(tol), "bound": float(bound),
                        "boundMode": bound_mode, "estimator": estimator, "convergenceCriterion": convergence_criterion}

    def fit(self, df: DataFrame, *, stage1=None, stage2=None, beta=None, sigma: Optional[float] = None,
            gamma_init=None) -> "LogisticFERandomClusterModel":
        """Fits with β, σ and the start given (`beta`, `sigma`, `gamma_init`, one per provider in key order), or from
        `stage1`, a fitted `LogisticFixedEffectModel`, and `stage2`, a fitted `ThreeStageModel` whose stage 2 gives σ
        and the start (pprof_py takes a two-factor `LogisticRandomEffectModel`, which arrives with part 4d)."""
        given = [name for name, value in (("beta", beta), ("sigma", sigma), ("gamma_init", gamma_init)) if value is not None]
        facade = api(df.sparkSession)
        if stage1 is not None or stage2 is not None:
            if stage1 is None or stage2 is None:
                raise ValueError("Pass both stage1 and stage2, or beta, sigma and gamma_init instead.")
            if given:
                raise ValueError(f"Pass either the fitted stages or {given}, not both.")
            handle = facade.feRandomClusterFitStages(df._jdf, json.dumps(self.columns), json.dumps(self.options),
                                                     stage1._handle, stage2._handle)
            return LogisticFERandomClusterModel(df.sparkSession, handle, stage1)
        missing = [name for name in ("beta", "sigma", "gamma_init") if name not in given]
        if missing:
            raise ValueError(f"Pass stage1 and stage2, or all of beta, sigma and gamma_init (missing: {missing}).")
        handle = facade.feRandomClusterFit(df._jdf, json.dumps(self.columns), json.dumps(self.options),
                                           json.dumps([float(b) for b in beta]), float(sigma),
                                           json.dumps([float(g) for g in gamma_init]))
        return LogisticFERandomClusterModel(df.sparkSession, handle, None)


class LogisticFERandomClusterModel:
    """A fitted random-cluster model: a handle to the JVM's fit, which keeps its records, with pprof_py's method
    names. The methods take no data: the fit holds its records, as pprof_py's model does."""

    def __init__(self, spark, handle, stage1, saved=None):
        self._spark = spark
        self._handle = handle
        self._stage1 = stage1
        self._saved = saved
        s = json.loads(api(spark).feRandomClusterSummary(handle) if handle is not None
                       else api(spark).feRandomClusterSavedSummary(saved))
        self.beta = [_float(b) for b in s["beta"]]
        self.sigma = _float(s["sigma"])
        self.loglik = _float(s["logLikelihood"])
        self.convergence = _float(s["criterion"])
        self.iterations = s["iterations"]
        self.converged = bool(s["converged"])
        self.n_providers = s["providers"]
        self.n_clusters = s["clusters"]
        self.estimator = s["estimator"]
        self.warnings = list(s["warnings"])

    def _frame(self, jdf) -> DataFrame:
        return DataFrame(jdf, self._spark)

    def _records(self):
        if self._handle is None:
            raise RuntimeError("This fit was loaded without its training data; LogisticFERandomClusterModel.load(spark, "
                               "path, df) re-attaches them, checked against the fit's rows and fingerprint.")
        return self._handle

    def save(self, path: str) -> None:
        """Saves the fit (`LogisticFERandomClusterFitIO`, format version 1) without its records; the path must not
        exist. A fit loaded without its data cannot be saved again."""
        api(self._spark).feRandomClusterSave(self._spark._jsparkSession, self._records(), path)

    @classmethod
    def load(cls, spark, path: str, df: Optional[DataFrame] = None) -> "LogisticFERandomClusterModel":
        """Loads a fit saved by `save` or by `LogisticFERandomClusterJob`. With `df`, its training data, the records
        are re-attached and every method works; without it, the tables, scalars and `summary(stage1)` work."""
        facade = api(spark)
        saved = facade.feRandomClusterLoad(spark._jsparkSession, path)
        if df is not None:
            return cls(spark, facade.feRandomClusterAttach(df._jdf, saved), None)
        return cls(spark, None, None, saved)

    def _frames(self, tables) -> Dict[str, DataFrame]:
        return {key: self._frame(tables.get(key)) for key in sorted(tables.keySet())}

    def providers(self) -> DataFrame:
        """One row per provider: its effect `gamma` and `held_at_bound`."""
        if self._handle is None:
            return self._frame(api(self._spark).feRandomClusterSavedProviders(self._spark._jsparkSession, self._saved))
        return self._frame(api(self._spark).feRandomClusterProviders(self._spark._jsparkSession, self._handle))

    def clusters(self) -> DataFrame:
        """One row per cluster: the posterior mean and variance of its effect."""
        if self._handle is None:
            return self._frame(api(self._spark).feRandomClusterSavedClusters(self._spark._jsparkSession, self._saved))
        return self._frame(api(self._spark).feRandomClusterClusters(self._spark._jsparkSession, self._handle))

    def summary(self, stage1=None, level: float = 0.95, null: float = 0.0,
                alternative: str = "two_sided") -> List[LogisticRETest]:
        """pprof_py's `summary`: stage 1's Wald tests of β, which must be the β this model used."""
        stage1 = stage1 if stage1 is not None else self._stage1
        handle = None if stage1 is None else stage1._handle
        facade = api(self._spark)
        rows = json.loads(facade.feRandomClusterCovariateTests(self._handle, handle, float(level), float(null), alternative)
                          if self._handle is not None else
                          facade.feRandomClusterSavedCovariateTests(self._saved, handle, float(level), float(null),
                                                                    alternative))
        return [LogisticRETest(r["feature"], _float(r["estimate"]), _float(r["standardError"]), _float(r["statistic"]),
                               _float(r["pValue"]), _float(r["lower"]), _float(r["upper"])) for r in rows]

    def test(self, providers=None, *, test_method: str = "exact", reference="median", null_model=None,
             alternative: str = "two_sided", level: float = 0.95, critical: Optional[float] = None,
             n_resample: int = 10000, seed: int = 0) -> DataFrame:
        """pprof_py's `test`: `exact` (the default), `poibin_exact` or `resampling`, in slice 2c's table."""
        if null_model is not None:
            raise NotImplementedError("null_model arrives with OI-87")
        names = json.dumps([str(p) for p in providers]) if providers else ""
        return self._frame(api(self._spark).feRandomClusterTest(
            self._records(), test_method, str(reference), alternative, float(level),
            float("nan") if critical is None else float(critical), names, int(n_resample), int(seed)))

    def calculate_confidence_intervals(self, providers=None, level: float = 0.95, option: str = "SM", stdz="indirect",
                                       reference="median", measure=("rate", "ratio"), alternative: str = "two_sided",
                                       test_method: str = "exact", null_model=None) -> Dict[str, DataFrame]:
        """pprof_py's `calculate_confidence_intervals`: option `gamma` (`gamma_ci`) or `SM` (`indirect_ratio`, ...)."""
        if null_model is not None:
            raise NotImplementedError("null_model arrives with OI-87")
        kinds = [stdz] if isinstance(stdz, str) else list(stdz)
        measures = [measure] if isinstance(measure, str) else list(measure)
        tables = self._frames(api(self._spark).feRandomClusterIntervals(
            self._records(), option, json.dumps(kinds), json.dumps(measures), alternative, float(level), test_method,
            str(reference)))
        if providers:
            wanted = [str(p) for p in providers]
            key = tables[next(iter(tables))].columns[0]
            tables = {k: t.filter(t[key].cast("string").isin(wanted)) for k, t in tables.items()}
        return tables

    def calculate_standardized_measures(self, providers=None, stdz="indirect", reference="median") -> Dict[str, DataFrame]:
        """pprof_py's `calculate_standardized_measures`: `indirect` and `direct` tables."""
        kinds = [stdz] if isinstance(stdz, str) else list(stdz)
        tables = self._frames(api(self._spark).feRandomClusterMeasures(self._records(), json.dumps(kinds), str(reference)))
        if providers:
            wanted = [str(p) for p in providers]
            key = tables[next(iter(tables))].columns[0]
            tables = {k: t.filter(t[key].cast("string").isin(wanted)) for k, t in tables.items()}
        return tables

    def fitted_values(self) -> DataFrame:
        """The records with `fitted`: expit(γ̂ + E[α | y] + xᵀβ)."""
        return self._frame(api(self._spark).feRandomClusterFitted(self._records()))
