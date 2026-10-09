"""The Python wrappers carry the engine's three-stage results unchanged (ADR-0009, slice 2f)."""
import json

import pytest
from pyspark.errors import IllegalArgumentException

from pprof_spark import SigmaSensitivity, ThreeStage, ThreeStageModel

from conftest import FIXTURES

CASE = FIXTURES / "three-stage" / "ts-golden"
T_OPT = (2e-4, 1e-6)  # D-42: estimates limited by pprof_py's optimizer


def inference():
    return json.loads((CASE / "pprof_py.json").read_text())["inference"]


def within(actual, expected, rtol=T_OPT[0], atol=T_OPT[1]):
    actual, expected = list(actual), [float.fromhex(v) for v in expected]
    scale = max(abs(e) for e in expected)
    return len(actual) == len(expected) and all(
        abs(a - e) <= atol + rtol * max(abs(e), scale) for a, e in zip(actual, expected))


def column(table, name, key="fac"):
    return [row[name] for row in table.orderBy(key).collect()]


def rows(table, key="fac"):
    return [repr(row) for row in table.orderBy(key).collect()]


@pytest.fixture(scope="module")
def data(spark):
    return spark.read.csv(str(CASE / "input.csv"), header=True, inferSchema=True)


@pytest.fixture(scope="module")
def model(data):
    features = json.loads((CASE / "case.json").read_text())["features"]
    return ThreeStage("Y", features, "fac", "hosp", row_id="rid").fit(data)


def test_fit_matches_pprof_py(model):
    inf = inference()
    assert model.stage2_converged and model.stage3_converged and model.columns["cluster"] == "hosp"
    assert len(model.stage1_coefficients) == 5 and model.summary["fit"]["cutoff"] == 10
    assert within(column(model.providers(), "gamma"), inf["gamma"])
    assert within(column(model.clusters(), "alpha_mean", "hosp"), inf["alpha_mean"])
    assert within([model.sigma], [inf["sensitivity"]["sigma"][1]])


def test_tests_measures_and_intervals(model):
    inf = inference()
    exact = model.test()
    assert {"flag", "z_raw"} <= set(exact.columns)
    assert within(column(exact, "z_raw"), inf["tests"]["exact_two_sided"]["z"])
    some = model.test(alternative="greater", providers=inf["providers"][:2])
    assert some.count() == 2 and {row["flag"] for row in some.collect()} <= {0, 1}
    measures = model.standardized_measures()
    assert sorted(measures) == ["direct", "indirect"]
    observed = inf["measures"]["median"]["indirect"]["observed"]
    assert column(measures["indirect"], "observed") == [float.fromhex(v) for v in observed]
    assert sorted(model.confidence_intervals()) == ["indirect_rate", "indirect_ratio"]
    (effects,) = model.confidence_intervals(option="gamma").values()
    assert all(r["gamma_lower"] <= r["gamma"] <= r["gamma_upper"] for r in effects.collect())
    with pytest.raises(IllegalArgumentException, match="method must be one of"):
        model.test(method="bootstrap")


def test_sigma_sensitivity_matches_pprof_py(model):
    inf = inference()["sensitivity"]
    result = model.sigma_sensitivity()
    assert isinstance(result, SigmaSensitivity) and result.estimate == model.sigma
    assert within([result.lower, result.estimate, result.upper], inf["sigma"])
    for at in ("lower", "estimate", "upper"):
        assert column(result.flags, at) == inf["flags"][at]
    assert column(result.flags, "stable") == inf["stable"]
    assert sorted(result.tests) == ["estimate", "lower", "upper"] and result.tests["upper"].count() == 40


def test_fitted_save_and_load(model, data, spark, tmp_path):
    fitted = model.fitted()
    assert "fitted" in fitted.columns and fitted.filter("fitted <= 0 or fitted >= 1").count() == 0
    path = str(tmp_path / "three-stage")
    model.save(path)
    loaded = ThreeStageModel.load(spark, path, data)
    assert loaded.summary == model.summary
    assert rows(loaded.providers()) == rows(model.providers())
    assert rows(loaded.test()) == rows(model.test())
    with pytest.raises(IllegalArgumentException):
        ThreeStageModel.load(spark, path, data.withColumn("Y", 1 - data["Y"]))


def test_invalid_options_are_reported(data):
    with pytest.raises(IllegalArgumentException, match="fit.stage2.speed"):
        ThreeStage("Y", ["age"], "fac", "hosp", stage2={"speed": 1}).fit(data)
