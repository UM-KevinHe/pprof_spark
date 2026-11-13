"""The logistic random-effect model with cluster factors: reference fixtures (slice 4d-2; ADR-0005, D-71).

pprof_py's `LogisticRandomEffectModel(...).fit(..., cluster_vars=...)` and lme4's `glmer` (Laplace) with crossed
intercepts, on synthetic data exact in text (features on a 1/64 grid, binary outcomes). Each case holds pprof_py's fits
at its defaults and tightened, and on the tightened fit its summary, tests, measures, intervals and the profile limits
of every factor's σ, with a negative control; `logistic_re_cluster_r.R` adds glmer's tightened Laplace fit.
generate.py calls `generate(out, inputs, lock, write_json)`; the output is deterministic.
"""

import os
import shutil
import subprocess
import warnings

import numpy as np
import pandas as pd
from scipy.special import expit
from pprof_py.models.logistic.random_effect import LogisticRandomEffectModel

CASES = (
    {"id": "lgc-one", "seed": 81, "clusters": [8]},
    {"id": "lgc-two", "seed": 82, "clusters": [8, 5]},
    {"id": "lgc-text", "seed": 83, "clusters": [8], "text": True},
    {"id": "lgc-offset", "seed": 84, "clusters": [8], "offset": True},
    {"id": "lgc-nested", "seed": 85, "clusters": [6], "nested": True},
    {"id": "lgc-boundary", "seed": 86, "clusters": [4], "cluster_sd": [0.0]},
)
PROVIDERS, ROWS, BETA = 30, 3000, np.array([-1.0, 0.6, -0.4])
TIGHT = {"tol_outer": 1e-10, "tol_pirls": 1e-13, "max_iter_outer": 20000, "max_iter_pirls": 200}
OPTIONS = {
    "pprof_py": "LogisticRandomEffectModel(verbose=False) at its defaults and with tol_outer=1e-10, tol_pirls=1e-13, "
                "max_iter_outer=20000, max_iter_pirls=200 (tight); fit(data, 'y', ['x1', 'x2'], 'provider', "
                "cluster_vars=[...], offset_var)",
    "inference": "on the tightened fit: summary(); test with wald, poibin_exact, resampling (n_resample 20000, seed 7) "
                 "and, with one cluster factor, exact, all at reference median; calculate_standardized_measures "
                 "(indirect and direct, median); calculate_confidence_intervals options alpha and SM (indirect and "
                 "direct ratios); profile_sigma(var) for every factor",
    "r": "glmer(y ~ x1 + x2 + (1 | provider) + (1 | c1) [+ (1 | c2)] [+ offset(o)], binomial, nAGQ = 1, bobyqa with "
         "rhoend 1e-12, maxfun 1e6, tolPwrss 1e-13), in logistic_re_cluster_r.R",
    "negative_controls": "the tightened fit with every outcome of the largest provider replaced by 1 - y",
}
TEST_COLUMNS = ("estimate", "null_value", "z_raw", "p_value", "flag", "ci_lower", "ci_upper")


def hexes(values):
    return [float(v).hex() for v in np.ravel(np.asarray(values, dtype=float))]


def label(value):
    return value.item() if hasattr(value, "item") else value


def cluster_names(case):
    return [f"c{k + 1}" for k in range(len(case["clusters"]))]


def make_data(case):
    rng = np.random.Generator(np.random.PCG64(case["seed"]))
    n, m = ROWS, PROVIDERS
    provider = rng.integers(0, m, n)
    sds = case.get("cluster_sd", [0.6, 0.4][: len(case["clusters"])])
    if case.get("nested"):
        clusters = [rng.integers(0, case["clusters"][0], m)[provider]]
    else:
        clusters = [rng.integers(0, levels, n) for levels in case["clusters"]]
    x1 = np.rint(rng.standard_normal(n) * 64) / 64
    x2 = (rng.random(n) < 0.4).astype(float)
    eta = BETA[0] + BETA[1] * x1 + BETA[2] * x2 + 0.5 * rng.standard_normal(m)[provider]
    for c, levels, sd in zip(clusters, case["clusters"], sds):
        eta = eta + sd * rng.standard_normal(levels)[c]
    o = np.rint(rng.standard_normal(n) * 16) / 64 if case.get("offset") else np.zeros(n)
    y = (rng.random(n) < expit(eta + o)).astype(float)
    df = pd.DataFrame({"id": np.arange(n)})
    df["provider"] = [f"prov-{j + 1}" for j in provider] if case.get("text") else provider + 1
    for name, c in zip(cluster_names(case), clusters):
        df[name] = [f"{name}-{h + 1}" for h in c] if case.get("text") else c + 1
    df["x1"], df["x2"], df["y"] = x1, x2, y
    if case.get("offset"):
        df["o"] = o
    return df


def fit(df, case, tight):
    model = LogisticRandomEffectModel(verbose=False, **(TIGHT if tight else {}))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        model.fit(df, "y", ["x1", "x2"], "provider", cluster_vars=cluster_names(case),
                  offset_var="o" if case.get("offset") else None, verbose=False)
    return model, sorted({str(w.message)[:200] for w in caught})


def record(model, case, caught):
    beta = model.coefficients_["beta"] if isinstance(model.coefficients_, dict) else model.coefficients_
    factors = ["provider"] + cluster_names(case)
    effects = {}
    for var in factors:
        re = model.get_random_effects(var)
        effects[var] = {"keys": [label(k) for k in re.index], "blup": hexes(re.to_numpy()),
                        "se": hexes(model._get_posterior_se(var).to_numpy())}
    return {
        "beta": hexes(np.asarray(beta)), "sigma": hexes([model.sigma_[f] for f in factors]),
        "deviance": hexes([model.stage2_objective_]), "stage1_deviance": hexes([model.stage1_objective_]),
        "loglik": hexes([model.loglike_]), "aic": hexes([model.aic_]), "bic": hexes([model.bic_]),
        "random_effects": effects, "fitted": hexes(np.asarray(model.fitted_)[:100]),
        "converged": bool(getattr(model, "converged_", True)), "warnings": caught,
    }


def frame(table):
    return {c: hexes(pd.to_numeric(table[c], errors="coerce").astype(float)) for c in table.columns
            if pd.api.types.is_numeric_dtype(table[c])}


def inference(model, case):
    out = {"summary": frame(model.summary())}
    methods = [("wald", {}), ("poibin_exact", {}), ("resampling", {"n_resample": 20000, "seed": 7})]
    if len(case["clusters"]) == 1:
        methods.append(("exact", {}))
    out["test"] = {}
    for method, kwargs in methods:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            t = model.test(test_method=method, **kwargs)
        out["test"][method] = {c: hexes(pd.to_numeric(t[c], errors="coerce").astype(float)) for c in TEST_COLUMNS}
    me = model.calculate_standardized_measures(stdz=["indirect", "direct"], reference="median")
    out["measures_median"] = {k: frame(me[k]) for k in ("indirect", "direct")}
    out["alpha_ci"] = frame(model.calculate_confidence_intervals(option="alpha")["alpha_ci"])
    sm = model.calculate_confidence_intervals(option="SM", stdz=["indirect", "direct"], measure=["ratio"])
    out["sm_two_sided"] = {k: frame(v) for k, v in sm.items()}
    out["profile_sigma"] = {}
    for var in ["provider"] + cluster_names(case):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            p = model.profile_sigma(var=var)
        out["profile_sigma"][var] = frame(p.reset_index(drop=True))
    return out


def generate(out, inputs, lock, write_json):
    """Writes out/logistic-re-clusters/<case>/{case.json, input.csv, pprof_py.json, r_lme4.json}."""
    root = os.path.join(out, "logistic-re-clusters")
    shutil.rmtree(root, ignore_errors=True)
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logistic_re_cluster_r.R")
    names = []
    for case in CASES:
        case_dir = os.path.join(root, case["id"])
        os.makedirs(case_dir)
        if inputs:
            shutil.copyfile(os.path.join(inputs, "logistic-re-clusters", case["id"], "input.csv"),
                            os.path.join(case_dir, "input.csv"))
            df = pd.read_csv(os.path.join(case_dir, "input.csv"))
        else:
            df = make_data(case)
            df.to_csv(os.path.join(case_dir, "input.csv"), index=False)
        write_json(os.path.join(case_dir, "case.json"), dict(case, features=["x1", "x2"], cluster_vars=cluster_names(case),
                                                            rows=len(df)))
        default, caught = fit(df, case, False)
        tight, caught_tight = fit(df, case, True)
        flipped = df.copy()
        largest = flipped["provider"].value_counts().idxmax()
        rows = flipped["provider"] == largest
        flipped.loc[rows, "y"] = 1.0 - flipped.loc[rows, "y"]
        control, _ = fit(flipped, case, True)
        reference = {"default": record(default, case, caught), "tight": record(tight, case, caught_tight),
                     "inference": inference(tight, case),
                     "negative_controls": {"flipped_provider": {"beta": hexes(np.asarray(
                         control.coefficients_["beta"] if isinstance(control.coefficients_, dict) else control.coefficients_)),
                         "sigma": hexes([control.sigma_[f] for f in ["provider"] + cluster_names(case)])}}}
        write_json(os.path.join(case_dir, "pprof_py.json"), reference)
        subprocess.run(["Rscript", script, case_dir], check=True)
        names.append(case["id"])
        print("logistic-re-clusters", case["id"], "done", flush=True)
    return names
