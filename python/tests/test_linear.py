"""The Python wrappers carry the engine's linear results unchanged (ADR-0009, slice 3c)."""
import json

import pytest
from pyspark.errors import IllegalArgumentException

from pprof_spark import LinearFixedEffect, LinearFixedEffectModel

from conftest import FIXTURES

CASE = FIXTURES / "linear" / "lin-base"
FEATURES = ["x1", "x2", "x3"]
T_COEF, T_FN, T_TEST, T_MEAS = (1e-8, 1e-10), (1e-12, 0.0), (1e-8, 0.0), (1e-8, 0.0)


def fixture(name):
    return json.loads((CASE / name).read_text())


def within(actual, expected, tolerance):
    rtol, atol = tolerance
    actual, expected = list(actual), [float.fromhex(v) for v in expected]
    scale = max(abs(e) for e in expected)
    return len(actual) == len(expected) and all(
        abs(a - e) <= atol + rtol * max(abs(e), scale) for a, e in zip(actual, expected))


def column(table, name, key="provider"):
    return [row[name] for row in table.orderBy(key).collect()]


@pytest.fixture(scope="module")
def data(spark):
    return spark.read.csv(str(CASE / "input.csv"), header=True, inferSchema=True)


@pytest.fixture(scope="module")
def model(data):
    return LinearFixedEffect("y", FEATURES, "provider", row_id="id").fit(data)


def test_fit_and_summary_match_pprof_py(model):
    py = fixture("pprof_py.json")
    c = py["complete"]
    assert within([x.estimate for x in model.coefficients], c["beta"], T_COEF)
    assert within(column(model.providers(), "gamma"), c["gamma"], T_COEF)
    assert within(column(model.providers(), "variance"), c["var_gamma"], (1e-7, 0.0))
    assert within([model.sigma, model.aic, model.bic], c["sigma"] + c["aic"] + c["bic"], T_FN)
    assert model.degrees_of_freedom == c["df"] and model.n_providers == len(c["providers"])
    tests = model.summary()
    assert within([t.statistic for t in tests], py["summary"]["two_sided"]["stat"], T_TEST)
    assert within([t.lower for t in tests], py["summary"]["two_sided"]["ci_lower"], T_COEF)


def test_provider_tests_intervals_and_measures(model, data):
    r = fixture("inference_r.json")
    tests = model.test()
    assert within(column(tests, "z_raw"), r["test_two_sided_z"], T_TEST)
    assert column(tests, "flag") == [int(float.fromhex(v)) for v in r["test_two_sided_flag"]]
    gamma = model.confidence_intervals(option="gamma")["gamma_ci"]
    assert within(column(gamma, "lower"), r["gamma_lower"], T_COEF)
    measures = model.standardized_measures(data)
    assert sorted(measures) == ["direct", "indirect"]
    assert within(column(measures["indirect"], "indirect_difference"), r["measures_median_indirect_difference"], T_MEAS)
    intervals = model.confidence_intervals(data, kinds=("indirect", "direct"))
    assert sorted(intervals) == ["direct_ci", "indirect_ci"]
    assert within(column(intervals["indirect_ci"], "lower"), r["sm_two_sided_indirect_lower"], T_MEAS)
    with pytest.raises(ValueError, match="training data"):
        model.confidence_intervals(option="SM")
    with pytest.raises(IllegalArgumentException, match="API-3"):
        model.standardized_measures(data.withColumn("y", data["y"] + 1.0))


def test_predict_score_save_and_load(model, data, spark, tmp_path):
    py = fixture("pprof_py.json")
    assert within(column(model.predict(data.filter(data["id"] < 100)), "prediction", "id"), py["predict"], T_MEAS)
    assert within([model.score(data)], py["score"], T_FN)
    path = str(tmp_path / "linear")
    model.save(path)
    loaded = LinearFixedEffectModel.load(spark, path)
    assert loaded.summary_json == model.summary_json
    assert [repr(row) for row in loaded.providers().orderBy("provider").collect()] == \
        [repr(row) for row in model.providers().orderBy("provider").collect()]


def test_invalid_options_are_reported(data):
    with pytest.raises(IllegalArgumentException, match="fit.varianceOption"):
        LinearFixedEffect("y", FEATURES, "provider", variance_option="full").fit(data)
