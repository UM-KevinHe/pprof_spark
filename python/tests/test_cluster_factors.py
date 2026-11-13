"""Python's cluster_vars for both random-effect models (slice 4d-3 §3) against fixtures/linear-re-clusters and
fixtures/logistic-re-clusters."""
import json

import pytest

from pprof_spark import LinearRandomEffect, LinearRandomEffectClusterModel, LogisticRandomEffect, LogisticRandomEffectClusterModel

from conftest import FIXTURES

T_OPT = (2e-4, 1e-6)


def fixture(family, case, name):
    return json.loads((FIXTURES / family / case / name).read_text())


def hexes(values):
    return [float.fromhex(v) for v in (values if isinstance(values, list) else [values])]


def within(actual, expected, tolerance=T_OPT):
    rtol, atol = tolerance
    actual, expected = list(actual), list(expected)
    return len(actual) == len(expected) and all(abs(a - e) <= atol + rtol * abs(e) for a, e in zip(actual, expected))


def read(spark, family, case):
    return spark.read.csv(str(FIXTURES / family / case / "input.csv"), header=True, inferSchema=True)


def test_linear_cluster_vars_against_pprof_py_and_save_load(spark, tmp_path):
    df = read(spark, "linear-re-clusters", "lrc-two")
    fit = LinearRandomEffect("y", ["x1", "x2"], "provider", row_id="id", cluster_vars=["c1", "c2"]).fit(df)
    assert isinstance(fit, LinearRandomEffectClusterModel) and fit.factors == ["provider", "c1", "c2"]
    t = fixture("linear-re-clusters", "lrc-two", "pprof_py.json")["reml_tight"]
    assert within([fit.theta[f] for f in fit.factors], hexes(t["theta"]))
    assert within([c.estimate for c in fit.coefficients], hexes(t["beta"]))
    table = fit.fixed_effects_table()
    assert within([r[3] for r in table], hexes(t["fixed_effects_table"]["t value"]))
    blups = {str(r["c1"]): r["blup"] for r in fit.get_random_effects("c1").collect()}
    pairs = zip(t["random_effects"]["c1"]["keys"], hexes(t["random_effects"]["c1"]["blup"]))
    assert all(abs(blups[str(k)] - v) <= 1e-6 + 2e-4 * abs(v) for k, v in pairs)
    with_all = [r["prediction"] for r in fit.predict(df, use_re=True).orderBy("id").collect()]
    fitted = [r["fitted"] for r in fit.fitted_values(df).orderBy("id").collect()]
    assert within(with_all, fitted, (1e-14, 1e-14))
    with pytest.raises(ValueError, match="cluster_vars"):
        fit.test()
    path = str(tmp_path / "lrc")
    fit.save(path)
    loaded = LinearRandomEffectClusterModel.load(spark, path)
    assert loaded.theta == fit.theta and loaded.sigma == fit.sigma


def test_logistic_cluster_vars_against_pprof_py_inference_and_save_load(spark, tmp_path):
    df = read(spark, "logistic-re-clusters", "lgc-one")
    fit = LogisticRandomEffect("y", ["x1", "x2"], "provider", row_id="id", cluster_vars="c1").fit(df)
    assert isinstance(fit, LogisticRandomEffectClusterModel)
    t = fixture("logistic-re-clusters", "lgc-one", "pprof_py.json")
    assert within([fit.sigma[f] for f in fit.factors], hexes(t["tight"]["sigma"]))
    assert within([c.estimate for c in fit.coefficients], hexes(t["tight"]["beta"]))
    exact = fit.test(test_method="exact")
    assert exact.count() == 30 and "z_raw" in exact.columns
    measures = fit.calculate_standardized_measures(stdz=["indirect", "direct"])
    assert set(measures) == {"indirect", "direct"}
    sm = fit.calculate_confidence_intervals(stdz=["indirect"], measure=["ratio"])
    assert "indirect_ratio" in sm["indirect_ratio"].columns
    profile = fit.profile_sigma("c1")["c1"]
    expected = t["inference"]["profile_sigma"]["c1"]
    assert within([profile["lower"], profile["upper"]], hexes(expected["lower"]) + hexes(expected["upper"]))
    path = str(tmp_path / "lgc")
    fit.save(path)
    tables_only = LogisticRandomEffectClusterModel.load(spark, path)
    assert tables_only.sigma == fit.sigma
    with pytest.raises(RuntimeError, match="training data"):
        tables_only.test()
    attached = LogisticRandomEffectClusterModel.load(spark, path, df)
    assert [r["z_raw"] for r in attached.test().orderBy("provider").collect()] == \
        [r["z_raw"] for r in fit.test().orderBy("provider").collect()]
    two = LogisticRandomEffect("y", ["x1", "x2"], "provider", row_id="id", cluster_vars=["c1", "c2"]).fit(
        read(spark, "logistic-re-clusters", "lgc-two"))
    with pytest.raises(Exception, match="exactly one cluster factor"):
        two.test(test_method="exact")
