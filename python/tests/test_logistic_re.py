"""The logistic random-intercept wrapper (slice 4b-1) against fixtures/logistic-re (D-58, D-59)."""
import json
import warnings

import pytest

from pprof_spark import LogisticRandomEffect

from conftest import FIXTURES

T_EST, T_OPT = (1e-6, 1e-9), (2e-4, 1e-6)
FEATURES = ["x1", "x2"]


def fixture(case, name):
    return json.loads((FIXTURES / "logistic-re" / case / name).read_text())


def within(actual, expected, tolerance):
    rtol, atol = tolerance
    actual, expected = list(actual), [float.fromhex(v) for v in expected]
    scale = max(abs(e) for e in expected)
    return len(actual) == len(expected) and all(
        abs(a - e) <= atol + rtol * max(abs(e), scale) for a, e in zip(actual, expected))


def column(table, name, key="provider"):
    return [row[name] for row in table.orderBy(key).collect()]


def first_hundred(frame, name):
    return [row[name] for row in frame.filter("id < 100").orderBy("id").collect()]


@pytest.fixture(scope="module")
def offset_data(spark):
    return spark.read.csv(str(FIXTURES / "logistic-re" / "lgre-offset" / "input.csv"), header=True, inferSchema=True)


@pytest.fixture(scope="module")
def offset_model(offset_data):
    return LogisticRandomEffect("y", FEATURES, "provider", offset="o", row_id="id").fit(offset_data)


def test_estimates_match_pprof_py_at_t_est_and_lme4_at_t_opt(offset_model):
    py = fixture("lgre-offset", "pprof_py.json")["nagq1"]["tight"]
    r = fixture("lgre-offset", "r_lme4.json")["nagq1"]["tight"]
    beta = [c.estimate for c in offset_model.coefficients]
    assert [c.feature for c in offset_model.coefficients] == ["(Intercept)"] + FEATURES
    assert within([offset_model.sigma], py["sigma"], T_EST) and within(beta, py["beta"], T_EST)
    assert within(column(offset_model.providers(), "blup"), py["blups"], T_EST)
    assert within(column(offset_model.providers(), "variance"), py["condvar"], T_EST)
    assert within(offset_model.covariance, py["var_beta"], T_EST)
    assert within([offset_model.deviance, offset_model.aic, offset_model.bic],
                  py["deviance"] + py["aic"] + py["bic"], T_EST)
    assert within([offset_model.sigma], r["sigma"], T_OPT) and within(beta, r["beta"], T_OPT)
    assert offset_model.converged and offset_model.criterion == "Laplace" and offset_model.n_providers == 40


def test_predictions_with_blups_under_integer_keys_and_residuals(offset_model, offset_data):
    r = fixture("lgre-offset", "r_lme4.json")["nagq1"]["tight"]
    cases = ((offset_model.predict(offset_data, use_re=True, offset_var="o"), "predict_re"),  # X-044
             (offset_model.predict(offset_data, offset_var="o"), "predict_fixed"),
             (offset_model.predict(offset_data, use_re=True, type="link", offset_var="o"), "predict_link_re"))
    for frame, key in cases:
        assert within(first_hundred(frame, "prediction"), r[key], T_OPT), key
    rows = offset_model.fitted_values(offset_data)
    for key, name in (("fitted", "fitted"), ("pearson", "pearson"), ("deviance_residuals", "deviance_residual")):
        assert within(first_hundred(rows, name), r[key], T_OPT), key
    with pytest.raises(ValueError, match="offset_var"):
        offset_model.predict(offset_data, offset_var="x1")


def test_unused_settings_warn_and_options_map():
    with pytest.warns(UserWarning, match="tol_outer"):
        LogisticRandomEffect("y", FEATURES, "provider", tol_outer=1e-12)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        LogisticRandomEffect("y", FEATURES, "provider")
    with pytest.raises(NotImplementedError, match="OI-93"):
        LogisticRandomEffect("y", FEATURES, "provider", cluster_vars=["c"])
    model = LogisticRandomEffect("y", FEATURES, "provider", stage2=False, sigma_upper=3.0)
    assert model.options == {"stage2": False, "includeIntercept": True, "sigmaUpper": 3.0, "maxIterations": 200}
