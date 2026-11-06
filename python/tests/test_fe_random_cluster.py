"""The fixed-effect random-cluster wrapper (slice 4c-1) against fixtures/fe-random-cluster (D-64)."""
import json

import pytest

from pprof_spark import LogisticFERandomCluster

from conftest import FIXTURES

T_COEF, T_TEST, T_MEAS = (1e-8, 1e-10), (1e-8, 0.0), (1e-8, 1e-10)


def fixture(case):
    return json.loads((FIXTURES / "fe-random-cluster" / case / "pprof_py.json").read_text())


def within(actual, expected, tolerance):
    rtol, atol = tolerance
    actual, expected = list(actual), [float.fromhex(v) for v in expected]
    scale = max(abs(e) for e in expected)
    return len(actual) == len(expected) and all(
        abs(a - e) <= atol + rtol * max(abs(e), scale) for a, e in zip(actual, expected))


def column(table, name, key="provider"):
    return [row[name] for row in table.orderBy(key).collect()]


def read(spark, case):
    return spark.read.csv(str(FIXTURES / "fe-random-cluster" / case / "input.csv"), header=True, inferSchema=True)


def given_fit(spark, case, observed=None):
    py = fixture(case)
    g = py["given"]
    model = LogisticFERandomCluster("y", ["x1", "x2"], "provider", "cluster", observed=observed, row_id="id")
    fit = model.fit(read(spark, case), beta=[float.fromhex(v) for v in g["beta"]], sigma=float.fromhex(g["sigma"][0]),
                    gamma_init=[float.fromhex(v) for v in g["start"]])
    return fit, py


def test_fit_tests_measures_and_intervals_against_pprof_py(spark):
    fit, py = given_fit(spark, "frc-base")
    t, inf = py["tight"], py["inference"]
    assert fit.converged and fit.n_providers == 30 and fit.n_clusters == 12
    assert within(column(fit.providers(), "gamma"), t["gamma"], T_COEF)
    assert within(column(fit.clusters(), "alpha_mean", "cluster"), t["alpha_mean"], T_MEAS)
    assert within([fit.loglik], t["loglik"], T_COEF)
    exact = fit.test()
    p = [float.fromhex(v) for v in inf["test"]["exact_median"]["p_value"]]
    clear = [i for i, v in enumerate(p) if v >= 1e-6]  # X-024
    z = column(exact, "z_raw")
    assert within([z[i] for i in clear], [inf["test"]["exact_median"]["z_raw"][i] for i in clear], T_TEST)
    measures = fit.calculate_standardized_measures(stdz=["indirect", "direct"])
    assert within(column(measures["indirect"], "indirect_ratio"), inf["measures_median"]["indirect"]["indirect_ratio"], T_MEAS)
    gamma = fit.calculate_confidence_intervals(option="gamma")["gamma_ci"]
    assert within(column(gamma, "gamma_lower"), inf["gamma_ci"]["lower"], T_COEF)


def test_observed_outcome_and_refusals(spark):
    fit, py = given_fit(spark, "frc-obs", observed="obs")
    assert within(column(fit.providers(), "gamma"), py["tight"]["gamma"], T_COEF)
    m = LogisticFERandomCluster("y", ["x1", "x2"], "provider", "cluster")
    df = read(spark, "frc-base")
    with pytest.raises(ValueError, match="missing"):
        m.fit(df, beta=[0.0, 0.0])
    with pytest.raises(ValueError, match="both stage1 and stage2"):
        m.fit(df, stage1=object())
    with pytest.raises(NotImplementedError, match="4c-2"):
        LogisticFERandomCluster("y", ["x1", "x2"], "provider", "cluster", estimator="he2013")
    with pytest.raises(NotImplementedError, match="OI-87"):
        fit.test(null_model="empirical")
    with pytest.raises(Exception, match="stage 1"):
        fit.summary()
