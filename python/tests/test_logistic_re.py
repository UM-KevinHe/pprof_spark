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


def inference_fixture():
    return fixture("lgre-offset", "inference_pprof_py.json")


def test_summary_tests_and_alpha_end_to_end_at_t_est(offset_model, offset_data):
    inf = inference_fixture()
    s = offset_model.summary(level=0.9)
    assert within([t.statistic for t in s], inf["summary"]["z"], T_EST)
    assert within([t.lower for t in s], inf["summary"]["lower"], T_EST)
    wald = offset_model.test()
    assert within(column(wald, "z_raw"), inf["test"]["wald_zero"]["z_raw"], T_EST)
    assert within(column(wald, "ci_lower"), inf["test"]["wald_zero"]["ci_lower"], T_EST)
    exact = offset_model.test(offset_data, test_method="poibin_exact")
    p = [float.fromhex(v) for v in inf["test"]["poibin_zero"]["p_value"]]
    clear = [i for i, v in enumerate(p) if v >= 1e-6]  # X-024: pprof_py's FFT tails are exact to about 1e-16 absolute
    z = column(exact, "z_raw")
    assert within([z[i] for i in clear], [inf["test"]["poibin_zero"]["z_raw"][i] for i in clear], T_EST)
    alpha = offset_model.calculate_confidence_intervals(option="alpha")["alpha_ci"]
    assert within(column(alpha, "alpha_lower"), inf["alpha_ci"]["lower"], T_EST)


def test_measures_and_sm_intervals_end_to_end_at_t_est(offset_model, offset_data):
    inf = inference_fixture()
    measures = offset_model.calculate_standardized_measures(offset_data, stdz=["indirect", "direct"])
    for kind in ("indirect", "direct"):
        for q in (f"{kind}_ratio", f"{kind}_rate", "expected"):
            assert within(column(measures[kind], q), inf["measures_median"][kind][q], T_EST), (kind, q)
    sm = offset_model.calculate_confidence_intervals(offset_data, stdz=["indirect", "direct"], measure=["ratio"])
    for key in ("indirect_ratio", "direct_ratio"):
        assert within(column(sm[key], key), inf["sm_two_sided"][key]["value"], T_EST), key
        for side in ("lower", "upper"):
            assert within(column(sm[key], side), inf["sm_two_sided"][key][side], T_EST), (key, side)


def test_profile_sigma_and_refusals(offset_model, offset_data):
    inf = inference_fixture()
    profile = offset_model.profile_sigma(offset_data)
    assert within([profile["lower"], profile["upper"]],
                  inf["profile_sigma"]["lower"] + inf["profile_sigma"]["upper"], T_OPT)
    with pytest.raises(ValueError, match="training data"):
        offset_model.test(test_method="poibin_exact")
    with pytest.raises(NotImplementedError, match="OI-87"):
        offset_model.test(null_model="empirical")
    with pytest.raises(NotImplementedError, match="part 4d"):
        offset_model.test(offset_data, test_method="exact")
    with pytest.raises(ValueError, match="two-sided"):
        offset_model.calculate_confidence_intervals(option="alpha", alternative="greater")
