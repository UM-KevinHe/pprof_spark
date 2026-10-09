"""The three-stage SRR model through the JVM engine (docs/spec/logistic/three-stage-pipeline.md, slice 2f)."""
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame

from ._jvm import api
from .cox import Coefficient, _float


@dataclass(frozen=True)
class SigmaSensitivity:
    """σ_c's profile interval with stage 3 refitted at its ends (pprof_py's `sigma_sensitivity`): `flags` has each
    provider's flag at `lower`, `estimate` and `upper` and whether they agree (`stable`); `tests` has stage 3's
    tests at each."""
    lower: float
    estimate: float
    upper: float
    flags: DataFrame
    tests: Dict[str, DataFrame]


class ThreeStage:
    """Column roles and options of the three-stage SRR model (pprof_py's `LogisticThreeStageModel`). The options
    follow the job runner's `fit` object (docs/guide/three-stage-job.md): `cutoff`, and `stage1`, `stage2` and
    `stage3` as dictionaries of its keys; options left out take the engine's defaults (pprof_py v0.7.0's)."""

    def __init__(self, outcome: str, features: Sequence[str], provider: str, cluster: str, *,
                 row_id: Optional[str] = None, cutoff: Optional[int] = None, stage1: Optional[dict] = None,
                 stage2: Optional[dict] = None, stage3: Optional[dict] = None):
        columns = {"outcome": outcome, "features": list(features), "provider": provider, "cluster": cluster,
                   "rowId": row_id}
        options = {"cutoff": cutoff, "stage1": stage1, "stage2": stage2, "stage3": stage3}
        self.columns = {k: v for k, v in columns.items() if v is not None}
        self.options = {k: v for k, v in options.items() if v is not None}

    def fit(self, df: DataFrame) -> "ThreeStageModel":
        facade = api(df.sparkSession)
        handle = facade.threeStageFit(df._jdf, json.dumps(self.columns), json.dumps(self.options))
        return ThreeStageModel(df.sparkSession, handle)


class ThreeStageModel:
    """A fitted three-stage model: a handle to the JVM's `ThreeStageFit`, which keeps the prepared training
    records, and its summary."""

    def __init__(self, spark, handle):
        self._spark = spark
        self._handle = handle
        s = json.loads(api(spark).threeStageSummary(handle))
        self.summary = s
        stage2, stage3 = s["stage2"], s["stage3"]
        self.sigma = _float(stage2["sigmaCluster"])
        self.sigma_provider = _float(stage2["sigmaProvider"])
        self.intercept = _float(stage2["intercept"])
        self.stage2_converged = stage2["converged"]
        self.stage3_converged = stage3["converged"]
        self.log_likelihood = _float(stage3["logLikelihood"])
        self.stage1_coefficients: List[Coefficient] = [
            Coefficient(c["feature"], _float(c["estimate"]), _float(c["standardError"]), _float(c["z"]),
                        _float(c["pValue"]), _float(c["lower"]), _float(c["upper"]))
            for c in s["stage1"]["coefficients"]
        ]
        self.excluded = [(e["provider"], e["records"]) for e in s["excluded"]]
        for key, name in (("providers", "n_providers"), ("clusters", "n_clusters"), ("cells", "n_cells"),
                          ("includedCells", "included_cells"), ("columns", "columns")):
            setattr(self, name, s[key])

    def _frame(self, jdf) -> DataFrame:
        return DataFrame(jdf, self._spark)

    def _frames(self, tables) -> Dict[str, DataFrame]:
        return {key: self._frame(tables.get(key)) for key in sorted(tables.keySet())}

    def providers(self) -> DataFrame:
        """One row per provider: stage 3's effect `gamma`, stage 2's BLUP, stage 3's start and `held_at_bound`."""
        return self._frame(api(self._spark).threeStageProviders(self._spark._jsparkSession, self._handle))

    def clusters(self) -> DataFrame:
        """One row per cluster: the posterior mean and variance of its effect, and stage 2's BLUP."""
        return self._frame(api(self._spark).threeStageClusters(self._spark._jsparkSession, self._handle))

    def test(self, method: str = "exact", reference="median", alternative: str = "two_sided", level: float = 0.95,
             critical: Optional[float] = None, providers: Optional[Sequence] = None, n_resample: int = 10000,
             seed: int = 0) -> DataFrame:
        """Stage 3's provider tests, `exact`, `poibin_exact` or `resampling`, against the reference effect
        (`median`, `mean` or a number); one row per provider."""
        names = json.dumps([str(p) for p in providers]) if providers else ""
        return self._frame(api(self._spark).threeStageTest(
            self._handle, method, str(reference), alternative, float(level),
            float("nan") if critical is None else float(critical), names, int(n_resample), int(seed)))

    def standardized_measures(self, kinds: Sequence[str] = ("indirect", "direct"),
                              reference="median") -> Dict[str, DataFrame]:
        """Indirect and direct standardized ratios and rates (rates in percent), by kind."""
        return self._frames(api(self._spark).threeStageMeasures(self._handle, json.dumps(list(kinds)),
                                                                str(reference)))

    def confidence_intervals(self, option: str = "SM", kinds: Sequence[str] = ("indirect",),
                             measure: Sequence[str] = ("rate", "ratio"), alternative: str = "two_sided",
                             level: float = 0.95, method: str = "exact", reference="median") -> Dict[str, DataFrame]:
        """Intervals by test inversion, for the effects (`option="gamma"`) or the standardized measures (`SM`)."""
        return self._frames(api(self._spark).threeStageIntervals(
            self._handle, option, json.dumps(list(kinds)), json.dumps(list(measure)), alternative, float(level),
            method, str(reference)))

    def sigma_sensitivity(self, level: float = 0.95, method: str = "exact", alternative: str = "two_sided",
                          test_level: float = 0.95) -> SigmaSensitivity:
        """σ_c's profile interval at `level`, stage 3 refitted at both ends, and each provider's flags."""
        facade = api(self._spark)
        result = facade.threeStageSensitivity(self._handle, float(level), method, alternative, float(test_level))
        sigma = json.loads(facade.threeStageSensitivitySummary(result))
        return SigmaSensitivity(_float(sigma["lower"]), _float(sigma["estimate"]), _float(sigma["upper"]),
                                self._frame(facade.threeStageSensitivityFlags(result)),
                                self._frames(facade.threeStageSensitivityTests(result)))

    def fitted(self) -> DataFrame:
        """The prepared training records with stage 3's fitted probability `fitted`."""
        return self._frame(api(self._spark).threeStageFitted(self._handle))

    def save(self, path: str) -> None:
        api(self._spark).threeStageSave(self._spark._jsparkSession, self._handle, path)

    @classmethod
    def load(cls, spark, path: str, df: DataFrame) -> "ThreeStageModel":
        """Loads a saved fit and re-attaches it to its training data `df`, since the records are not saved; data
        whose stage 1 fingerprint differs are refused."""
        return cls(spark, api(spark).threeStageLoad(spark._jsparkSession, path, df._jdf))
