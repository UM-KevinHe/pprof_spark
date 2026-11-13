"""The linear random-effect model with cluster factors: reference fixtures (slice 4d-1; ADR-0005, D-69).

pprof_py's `LinearRandomEffectModel(...).fit(..., cluster_vars=...)` and lme4's `lmer` with crossed intercepts, on
synthetic data exact in text (features and outcomes on a 1/64 grid, weights in {1/2, 1, 2}). Each case holds pprof_py's
REML and ML fits at its defaults (Powell, tol 1e-8) and tightened (tol 1e-13), lme4's REML and ML fits with tight
optimizer tolerances (linear_re_cluster_r.R), and negative controls. generate.py calls `generate(out, inputs, lock,
write_json)`; the output is deterministic.
"""

import json
import os
import shutil
import subprocess
import warnings

import numpy as np
import pandas as pd
from pprof_py.models.linear.random_effect import LinearRandomEffectModel

CASES = (
    {"id": "lrc-one", "seed": 71, "clusters": [8]},
    {"id": "lrc-two", "seed": 72, "clusters": [8, 5]},
    {"id": "lrc-text", "seed": 73, "clusters": [8], "text": True},
    {"id": "lrc-weights", "seed": 74, "clusters": [8], "weights": True},
    {"id": "lrc-offset", "seed": 75, "clusters": [8], "offset": True},
    {"id": "lrc-nested", "seed": 76, "clusters": [6], "nested": True},
    {"id": "lrc-boundary", "seed": 77, "clusters": [5], "cluster_sd": [0.0]},
)
PROVIDERS, ROWS, BETA = 30, 3000, np.array([1.0, 0.8, -0.5])
TIGHT = {"tol_outer": 1e-13, "max_iter_outer": 20000}
OPTIONS = {
    "pprof_py": "LinearRandomEffectModel(optimizer='powell', tol_outer=1e-8 (default) and 1e-13 with max_iter_outer=20000 "
                "(tight), reml=True and False).fit(data, 'y', ['x1', 'x2'], 'provider', cluster_vars=[...], offset_var, "
                "weights_var)",
    "r": "lmer(y ~ x1 + x2 + (1 | provider) + (1 | c1) [+ (1 | c2)] [+ offset(o)], weights, REML = TRUE and FALSE, "
         "nloptwrap with xtol_abs = ftol_abs = 1e-14 and maxeval 1e5), in linear_re_cluster_r.R",
    "negative_controls": "the tightened REML fit with 0.5 added to every outcome of the first cluster level of the first factor",
    "tables": "every fit also records pprof_py's summary(), fixed_effects_table() and conf_int() at their defaults (round 144)",
}


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
    clusters = []
    sds = case.get("cluster_sd", [0.6, 0.4][: len(case["clusters"])])
    if case.get("nested"):
        home = rng.integers(0, case["clusters"][0], m)  # each provider sits in one cluster
        clusters.append(home[provider])
    else:
        clusters = [rng.integers(0, levels, n) for levels in case["clusters"]]
    x1 = np.rint(rng.standard_normal(n) * 64) / 64
    x2 = (rng.random(n) < 0.4).astype(float)
    gp = 0.5 * rng.standard_normal(m)
    eta = BETA[0] + BETA[1] * x1 + BETA[2] * x2 + gp[provider]
    for c, levels, sd in zip(clusters, case["clusters"], sds):
        eta = eta + sd * rng.standard_normal(levels)[c]
    w = rng.choice([0.5, 1.0, 2.0], n) if case.get("weights") else np.ones(n)
    o = np.rint(rng.standard_normal(n) * 16) / 64 if case.get("offset") else np.zeros(n)
    y = np.rint((eta + o + rng.standard_normal(n) / np.sqrt(w)) * 64) / 64
    df = pd.DataFrame({"id": np.arange(n)})
    df["provider"] = [f"prov-{j + 1}" for j in provider] if case.get("text") else provider + 1
    for name, c in zip(cluster_names(case), clusters):
        df[name] = [f"{name}-{h + 1}" for h in c] if case.get("text") else c + 1
    df["x1"], df["x2"], df["y"] = x1, x2, y
    if case.get("weights"):
        df["w"] = w
    if case.get("offset"):
        df["o"] = o
    return df


def fit(df, case, reml, tight):
    model = LinearRandomEffectModel(optimizer="powell", reml=reml, verbose=False, **(TIGHT if tight else {}))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        model.fit(df, "y", ["x1", "x2"], "provider", cluster_vars=cluster_names(case),
                  offset_var="o" if case.get("offset") else None, weights_var="w" if case.get("weights") else None,
                  verbose=False)
    return model, sorted({str(w.message)[:200] for w in caught})


def record(model, case, caught):
    beta = model.coefficients_["beta"]
    effects = {}
    for var in ["provider"] + cluster_names(case):
        re = model.get_random_effects(var)
        effects[var] = {"keys": [label(k) for k in re.index], "blup": hexes(re.to_numpy())}
    return {
        "beta_names": list(beta.index), "beta": hexes(beta.to_numpy()), "theta": hexes(model.theta_),
        "sigma": hexes([model.sigma_]), "objective": hexes([model.objective_]), "loglik": hexes([model.loglike_]),
        "aic": hexes([model.aic_]), "bic": hexes([model.bic_]), "se": hexes(model.standard_errors().to_numpy()),
        "random_effects": effects, "fitted": hexes(np.asarray(model.fitted_values())[:100]),
        "converged": bool(model.converged_), "warnings": caught,
        "summary": frame(model.summary()), "fixed_effects_table": frame(model.fixed_effects_table()),
        "conf_int": frame(model.conf_int()),
    }


def frame(table):
    """A table's numeric columns as hex doubles, in the table's row order."""
    return {str(c): hexes(pd.to_numeric(table[c], errors="coerce").astype(float)) for c in table.columns
            if pd.api.types.is_numeric_dtype(table[c])}


def generate(out, inputs, lock, write_json):
    """Writes out/linear-re-clusters/<case>/{case.json, input.csv, pprof_py.json, r_lme4.json}."""
    root = os.path.join(out, "linear-re-clusters")
    shutil.rmtree(root, ignore_errors=True)
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "linear_re_cluster_r.R")
    names = []
    for case in CASES:
        case_dir = os.path.join(root, case["id"])
        os.makedirs(case_dir)
        if inputs:
            shutil.copyfile(os.path.join(inputs, "linear-re-clusters", case["id"], "input.csv"),
                            os.path.join(case_dir, "input.csv"))
            df = pd.read_csv(os.path.join(case_dir, "input.csv"))
        else:
            df = make_data(case)
            df.to_csv(os.path.join(case_dir, "input.csv"), index=False)
        description = dict(case, features=["x1", "x2"], cluster_vars=cluster_names(case), rows=len(df))
        write_json(os.path.join(case_dir, "case.json"), description)
        reference = {}
        for reml in (True, False):
            for tight in (False, True):
                model, caught = fit(df, case, reml, tight)
                reference[f"{'reml' if reml else 'ml'}_{'tight' if tight else 'default'}"] = record(model, case, caught)
        shifted = df.copy()
        first = cluster_names(case)[0]
        shifted.loc[shifted[first] == shifted[first].iloc[0], "y"] += 0.5
        control, _ = fit(shifted, case, True, True)
        reference["negative_controls"] = {"shifted_cluster": {"theta": hexes(control.theta_), "beta": hexes(control.coefficients_["beta"].to_numpy())}}
        write_json(os.path.join(case_dir, "pprof_py.json"), reference)
        subprocess.run(["Rscript", script, case_dir], check=True)
        names.append(case["id"])
        print("linear-re-clusters", case["id"], "done", flush=True)
    return names
