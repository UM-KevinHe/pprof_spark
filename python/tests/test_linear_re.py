"""The Python wrappers carry the engine's linear random-effect results unchanged (ADR-0009, slice 4a-1)."""
import json
import warnings

import pytest

from pprof_spark import LinearRandomEffect

from conftest import FIXTURES

FEATURES = ["x1", "x2", "x3"]
T_COEF, T_VAR, T_FN = (1e-8, 1e-10), (1e-7, 0.0), (1e-12, 0.0)


def fixture(case, name):
    return json.loads((FIXTURES / "linear-re" / case / name).read_text())


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


def read(spark, case):
    return spark.read.csv(str(FIXTURES / "linear-re" / case / "input.csv"), header=True, inferSchema=True)


@pytest.fixture(scope="module")
def offset_data(spark):
    return read(spark, "lre-offset")


@pytest.fixture(scope="module")
def offset_model(offset_data):
    return LinearRandomEffect("y", FEATURES, "provider", offset="o", row_id="id").fit(offset_data)


def test_fit_matches_both_references_tight_optima(offset_model):
    for reference in ("pprof_py.json", "r_lme4.json"):
        t = fixture("lre-offset", reference)["reml"]["tight"]
        assert within([c.estimate for c in offset_model.coefficients], t["beta"], T_COEF)
        assert within(column(offset_model.providers(), "blup"), t["blups"], T_COEF)
        for name, key in (("theta", "theta"), ("sigma", "sigma"), ("sigma_alpha", "sigma_alpha")):
            assert within([getattr(offset_model, name)], t[key], T_VAR), name
        assert within(offset_model.covariance, t["var_beta"], T_VAR)
        for name in ("deviance", "loglik", "aic", "bic"):
            assert within([getattr(offset_model, name)], t[name], T_FN), name
    assert offset_model.criterion == "REML" and offset_model.n_providers == 40
    assert offset_model.get_sigma() == offset_model.sigma == offset_model.residual_standard_error()
    assert offset_model.get_random_effect_sd() == offset_model.sigma_alpha
    assert list(offset_model.standard_errors()) == ["(Intercept)"] + FEATURES


def test_predictions_and_fitted_values(offset_model, offset_data):
    t = fixture("lre-offset", "pprof_py.json")["reml"]["tight"]
    fixed = offset_model.predict(offset_data, offset_var="o")
    assert within(first_hundred(fixed, "prediction"), t["predict_fixed"], T_COEF)
    with_blups = offset_model.predict(offset_data, use_re=True, offset_var="o")
    assert within(first_hundred(with_blups, "prediction"), t["predict_re"], T_COEF)
    assert within(first_hundred(offset_model.fitted_values(offset_data), "fitted"), t["fitted"], T_COEF)
    with pytest.raises(ValueError, match="offset_var"):
        offset_model.predict(offset_data, offset_var="x1")


def test_weights_under_ml_and_conditional_variances(spark):
    model = LinearRandomEffect("y", FEATURES, "provider", weights="w", row_id="id", reml=False).fit(
        read(spark, "lre-weights"))
    t = fixture("lre-weights", "r_lme4.json")["ml"]["tight"]
    assert model.criterion == "ML"
    assert within([c.estimate for c in model.coefficients], t["beta"], T_COEF)
    assert within(column(model.providers(), "variance"), t["condvar"], T_VAR)


def test_unused_optimizer_settings_warn_and_cluster_terms_wait():
    with pytest.warns(UserWarning, match="tol_outer"):
        LinearRandomEffect("y", FEATURES, "provider", tol_outer=1e-12)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        LinearRandomEffect("y", FEATURES, "provider", optimizer="Powell", max_iter_outer=50)
    with pytest.raises(NotImplementedError, match="OI-93"):
        LinearRandomEffect("y", FEATURES, "provider", cluster_vars=["hospital"])
    with pytest.raises(ValueError, match="optimizer"):
        LinearRandomEffect("y", FEATURES, "provider", optimizer="bfgs")


T_TEST, T_MEAS = (1e-8, 0.0), (1e-8, 0.0)


def p_close(actual, expected):
    """T-p: relative 1e-8 at or above 1e-10, log10 within 1e-6 below it."""
    import math
    for a, e in zip(actual, [float.fromhex(v) for v in expected]):
        ok = abs(a - e) <= 1e-8 * abs(e) if e >= 1e-10 else (a < 1e-300 if e == 0.0 else
                                                             a > 0 and abs(math.log10(a) - math.log10(e)) <= 1e-6)
        if not ok:
            return False
    return True


def test_covariate_tables_match_base_r(offset_model):
    r = fixture("lre-offset", "r_lme4.json")["reml"]["inference"]
    py = fixture("lre-offset", "inference_pprof_py.json")["reml"]
    s = offset_model.summary(level=0.9)
    assert within([t.statistic for t in s], r["summary_stat"], T_TEST)
    assert p_close([t.p_value for t in s], r["summary_p_two_sided"])
    assert within([t.lower for t in s], r["summary_lower_two_sided"], T_COEF)
    assert [t.feature for t in offset_model.summary(covariates=["x2"])] == ["x2"]
    table = offset_model.fixed_effects_table()
    assert within([e.statistic for e in table], py["fixed_effects_table"]["t"], T_TEST)
    assert p_close([e.p_value for e in table], r["fet_p"])
    bounds = offset_model.conf_int(0.95)
    assert within([bounds[f][0] for f in ["(Intercept)"] + FEATURES], r["conf_lower"], T_COEF)


def test_provider_tests_intervals_and_measures(offset_model, offset_data):
    r = fixture("lre-offset", "r_lme4.json")["reml"]["inference"]
    py = fixture("lre-offset", "inference_pprof_py.json")["reml"]
    tests = offset_model.test()
    assert within(column(tests, "z_raw"), r["test_zero_z"], T_TEST)
    assert p_close(column(tests, "p_value"), r["test_zero_p"])
    assert within(column(tests, "ci_lower"), r["test_zero_lower"], T_COEF)
    median = offset_model.test(reference="median", alternative="greater")
    assert within(column(median, "z_raw"), py["test"]["greater"]["z_raw"], T_TEST)
    alpha = offset_model.confidence_intervals(option="alpha")["alpha_ci"]
    assert within(column(alpha, "alpha_lower"), py["alpha_ci"]["alpha_lower"], T_COEF)
    sm = offset_model.confidence_intervals(offset_data, kinds=("indirect", "direct"))
    for kind in ("indirect", "direct"):
        for q in (f"{kind}_difference", "lower", "upper"):
            assert within(column(sm[f"{kind}_ci"], q), py["sm_ci_two_sided"][kind][q], T_MEAS), (kind, q)
    measures = offset_model.standardized_measures(offset_data, reference="mean")
    for kind in ("indirect", "direct"):
        for q in (f"{kind}_difference", "observed", "expected"):
            assert within(column(measures[kind], q), py["measures_mean"][kind][q], T_MEAS), (kind, q)
    with pytest.raises(ValueError, match="two-sided"):
        offset_model.confidence_intervals(option="alpha", alternative="greater")
    with pytest.raises(ValueError, match="training data"):
        offset_model.confidence_intervals(option="SM")


def test_save_and_load_and_the_facade_parser(offset_model, offset_data, tmp_path):
    from pprof_spark import LinearRandomEffectModel
    path = str(tmp_path / "fit")
    offset_model.save(path)
    loaded = LinearRandomEffectModel.load(offset_data.sparkSession, path)
    assert [c.estimate for c in loaded.coefficients] == [c.estimate for c in offset_model.coefficients]
    assert (loaded.theta, loaded.sigma, loaded.deviance) == (offset_model.theta, offset_model.sigma, offset_model.deviance)
    assert column(loaded.providers(), "blup") == column(offset_model.providers(), "blup")
    with pytest.raises(Exception, match="path|exist"):
        offset_model.save(path)
    with pytest.raises(Exception, match="fit.blocks.bogus does not belong"):
        LinearRandomEffect("y", FEATURES, "provider", blocks={"bogus": 1}).fit(offset_data)
