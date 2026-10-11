"""The linear fixed-effect provider model through the JVM engine (docs/spec/linear/, slice 3c)."""
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame

from ._jvm import api
from .cox import _float


@dataclass(frozen=True)
class LinearCoefficient:
    feature: str
    estimate: float
    standard_error: float


@dataclass(frozen=True)
class LinearTest:
    """One covariate's t test and interval on n − m − p degrees of freedom (`summary`)."""
    feature: str
    estimate: float
    standard_error: float
    statistic: float
    p_value: float
    lower: float
    upper: float


class LinearFixedEffect:
    """Column roles and options of the linear fixed-effect model (pprof_py's `LinearFixedEffectModel`). The options
    follow the job runner's `fit` object (docs/guide/linear-job.md): `variance_option` (`complete`, the default, or
    `simplified`) and `blocks`; options left out take the engine's defaults."""

    def __init__(self, outcome: str, features: Sequence[str], provider: str, *, row_id: Optional[str] = None,
                 variance_option: Optional[str] = None, blocks: Optional[dict] = None):
        columns = {"outcome": outcome, "features": list(features), "provider": provider, "rowId": row_id}
        options = {"varianceOption": variance_option, "blocks": blocks}
        self.columns = {k: v for k, v in columns.items() if v is not None}
        self.options = {k: v for k, v in options.items() if v is not None}

    def fit(self, df: DataFrame) -> "LinearFixedEffectModel":
        handle = api(df.sparkSession).linearFit(df._jdf, json.dumps(self.columns), json.dumps(self.options))
        return LinearFixedEffectModel(df.sparkSession, handle)


class LinearFixedEffectModel:
    """A fitted linear fixed-effect model: a handle to the JVM's `LinearFit` and its summary. Tests and effect
    intervals need only the fit; standardized measures and their intervals need its training data (API-3)."""

    def __init__(self, spark, handle):
        self._spark = spark
        self._handle = handle
        s = json.loads(api(spark).linearSummary(handle))
        self.summary_json = s
        self.coefficients: List[LinearCoefficient] = [
            LinearCoefficient(c["feature"], _float(c["estimate"]), _float(c["standardError"])) for c in s["coefficients"]]
        for key in ("sigma", "rss", "r2", "loglik", "aic", "bic", "condition"):
            setattr(self, key, _float(s[key]))
        self.degrees_of_freedom = s["degreesOfFreedom"]
        self.n_providers = s["providers"]
        self.rows = s["rows"]
        self.warnings = list(s["warnings"])

    def _frame(self, jdf) -> DataFrame:
        return DataFrame(jdf, self._spark)

    def _frames(self, tables) -> Dict[str, DataFrame]:
        return {key: self._frame(tables.get(key)) for key in sorted(tables.keySet())}

    @staticmethod
    def _names(providers) -> str:
        return json.dumps([str(p) for p in providers]) if providers else ""

    def providers(self) -> DataFrame:
        """One row per provider: `records`, `gamma`, its `variance` under the fit's option, and `se`."""
        return self._frame(api(self._spark).linearProviders(self._handle))

    def summary(self, level: float = 0.95, null: float = 0.0, alternative: str = "two_sided") -> List[LinearTest]:
        """pprof_py's `summary`: t tests and intervals for the covariates."""
        text = api(self._spark).linearCovariateTests(self._handle, float(level), float(null), alternative)
        return [LinearTest(t["feature"], _float(t["estimate"]), _float(t["standardError"]), _float(t["statistic"]),
                           _float(t["pValue"]), _float(t["lower"]), _float(t["upper"])) for t in json.loads(text)]

    def test(self, reference="median", alternative: str = "two_sided", level: float = 0.95,
             critical: Optional[float] = None, providers: Optional[Sequence] = None) -> DataFrame:
        """pprof_py's `test`: one row per provider with its provider-test columns."""
        return self._frame(api(self._spark).linearTest(
            self._handle, str(reference), alternative, float(level),
            float("nan") if critical is None else float(critical), self._names(providers)))

    def confidence_intervals(self, df: Optional[DataFrame] = None, option: str = "SM",
                             kinds: Sequence[str] = ("indirect",), reference="median", alternative: str = "two_sided",
                             level: float = 0.95, providers: Optional[Sequence] = None) -> Dict[str, DataFrame]:
        """pprof_py's `calculate_confidence_intervals`, keyed as it is (`gamma_ci`, `indirect_ci`, `direct_ci`): option
        `gamma` needs only the fit and is two-sided; option `SM` needs the fit's training data `df` (API-3)."""
        names = self._names(providers)
        if option == "gamma":
            if alternative != "two_sided":
                raise ValueError("option 'gamma' supports only two-sided intervals")
            return {"gamma_ci": self._frame(api(self._spark).linearGammaIntervals(self._handle, float(level), names))}
        if option != "SM":
            raise ValueError("option must be 'gamma' or 'SM'")
        if df is None:
            raise ValueError("option 'SM' needs the fit's training data (API-3)")
        tables = api(self._spark).linearIntervals(df._jdf, self._handle, json.dumps(list(kinds)), str(reference),
                                                   alternative, float(level), names)
        return {f"{kind}_ci": table for kind, table in self._frames(tables).items()}

    def standardized_measures(self, df: DataFrame, kinds: Sequence[str] = ("indirect", "direct"),
                              reference="median", providers: Optional[Sequence] = None) -> Dict[str, DataFrame]:
        """pprof_py's `calculate_standardized_measures` on the fit's training data `df` (API-3)."""
        return self._frames(api(self._spark).linearMeasures(df._jdf, self._handle, json.dumps(list(kinds)),
                                                            str(reference), self._names(providers)))

    def predict(self, df: DataFrame) -> DataFrame:
        """`df` with `prediction` = γ̂ⱼ + xᵀβ̂; rows of providers not in the fit fail."""
        return self._frame(api(self._spark).linearPredict(df._jdf, self._handle))

    def score(self, df: DataFrame) -> float:
        """R² on `df` (pprof_py's `score`, for any provider key type)."""
        return _float(api(self._spark).linearScore(df._jdf, self._handle))

    def save(self, path: str) -> None:
        api(self._spark).linearSave(self._spark._jsparkSession, self._handle, path)

    @classmethod
    def load(cls, spark, path: str) -> "LinearFixedEffectModel":
        return cls(spark, api(spark).linearLoad(spark._jsparkSession, path))
