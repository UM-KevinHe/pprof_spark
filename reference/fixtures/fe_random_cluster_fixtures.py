"""Fixed provider effects with random cluster effects: reference fixtures (slice 4c-1; ADR-0005, D-63).

pprof_py's `LogisticFERandomClusterModel` with β, σ and the start given (spec 4c-1 §2), on synthetic crossed data:
records belong to a provider and to a cluster, clusters span providers. Inputs are exact in text (features on a 1/64
grid, binary outcomes; frc-obs adds a fractional adjusted outcome on a 1/64 grid). Each case holds pprof_py's fit at its
defaults (tol 1e-5) and tightened (tol 1e-12), and on the tightened fit its tests, intervals and measures, with
negative controls. generate.py calls `generate(out, inputs, lock, write_json)`; the output is deterministic.
"""

import os
import shutil
import warnings

import numpy as np
import pandas as pd
from scipy.special import expit
from pprof_py.models.logistic.fe_random_cluster import LogisticFERandomClusterModel

BASE = {"providers": 30, "clusters": 12, "rows": 3000, "sigma": 0.7, "gamma_sd": 0.5}
CASES = (
    {"id": "frc-base", "seed": 61},
    {"id": "frc-obs", "seed": 62, "adjusted": True},
    {"id": "frc-absolute", "seed": 63, "bound_mode": "absolute", "extreme": True},
    {"id": "frc-text", "seed": 64, "text": True},
    # σ = 0 itself: pprof_py divides by σ² (X-005), so the case takes a small σ; spec 4c-1 §6 checks σ = 0 otherwise
    {"id": "frc-small-sigma", "seed": 65, "given_sigma": 0.05},
)
BETA_TRUE = np.array([0.6, -0.4])
TIGHT = {"tol": 1e-12}
OPTIONS = {
    "pprof_py": "LogisticFERandomClusterModel(n_nodes=20, max_iter=10000, tol=1e-5 (default) and 1e-12 (tight), bound=10, "
                "bound_mode as the case sets, estimator='marginal').fit(data, 'y', ['x1', 'x2'], 'provider', 'cluster', "
                "beta=, sigma=, gamma_init= given (beta: the truth plus 0.05; sigma: 0.7 unless the case sets it; start: "
                "the logit of each provider's outcome mean, clipped to [0.02, 0.98]), obs_var='obs' in frc-obs)",
    "inference": "on the tightened fit: test with exact (reference median and 0, greater at the median), poibin_exact "
                 "(median), resampling (median, n_resample 20000, seed 7); calculate_confidence_intervals with option "
                 "gamma (exact) and option SM (indirect and direct, ratio and rate, exact; two-sided and greater); "
                 "calculate_standardized_measures (indirect and direct) at the median, the mean and 0",
    "negative_controls": "the tightened fit with every outcome of the largest provider replaced by 1 - y",
    "he2013": "estimator='he2013' (slice 4c-2, D-65) with convergence_criterion max_delta_gamma and relative, each at "
              "its defaults (tol 1e-5) and at tol 1e-12; on the max_delta_gamma fixed point the exact test at the median "
              "and the measures at the median; a negative control with the largest provider's outcomes flipped",
}


def hexes(values):
    return [float(v).hex() for v in np.ravel(np.asarray(values, dtype=float))]


def flags(values):
    return hexes([np.nan if v is None or pd.isna(v) else float(v) for v in values])


def label(value):
    return value.item() if hasattr(value, "item") else value


def make_data(case):
    rng = np.random.Generator(np.random.PCG64(case["seed"]))
    m, k, n = BASE["providers"], BASE["clusters"], BASE["rows"]
    gamma = -1.0 + BASE["gamma_sd"] * rng.standard_normal(m)
    alpha = BASE["sigma"] * rng.standard_normal(k)
    provider = rng.integers(0, m, n)
    cluster = rng.integers(0, k, n)
    x1 = np.rint(rng.standard_normal(n) * 64) / 64
    x2 = (rng.random(n) < 0.3).astype(float)
    eta = gamma[provider] + BETA_TRUE[0] * x1 + BETA_TRUE[1] * x2 + alpha[cluster]
    y = (rng.random(n) < expit(eta)).astype(float)
    if case.get("extreme"):
        y[provider == 0] = 0.0  # a provider whose outcomes are all 0: its effect reaches the bound
    df = pd.DataFrame({
        "provider": [f"prov-{j + 1}" for j in provider] if case.get("text") else provider + 1,
        "cluster": [f"clus-{h + 1}" for h in cluster] if case.get("text") else cluster + 1,
        "x1": x1, "x2": x2, "y": y,
    })
    if case.get("adjusted"):
        df["obs"] = y
        df["y"] = np.rint((0.8 * y + 0.2 * expit(eta)) * 64) / 64  # an adjusted outcome in [0, 1]
    df.insert(0, "id", np.arange(n))
    return df


def given(df, case):
    """β (the truth plus 0.05), σ and the start: the logit of each provider's outcome mean, clipped."""
    keys = sorted(df["provider"].unique(), key=lambda v: (str(type(v)), v))
    means = df.groupby("provider")["obs" if case.get("adjusted") else "y"].mean().reindex(keys).to_numpy()
    p = np.clip(means, 0.02, 0.98)
    return BETA_TRUE + 0.05, case.get("given_sigma", BASE["sigma"]), np.log(p / (1.0 - p))


def fit(df, case, tight, estimator="marginal", criterion="max_delta_gamma"):
    beta, sigma, start = given(df, case)
    model = LogisticFERandomClusterModel(bound_mode=case.get("bound_mode", "relative"), estimator=estimator,
                                         convergence_criterion=criterion, **(TIGHT if tight else {}))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        model.fit(df, "y", ["x1", "x2"], "provider", "cluster", beta=beta, sigma=sigma, gamma_init=start,
                  obs_var="obs" if case.get("adjusted") else None, verbose=False)
    return model, sorted({str(w.message)[:200] for w in caught})


def record(model, caught):
    return {
        "providers": [label(v) for v in model.provider_ids_], "clusters": [label(v) for v in model.cluster_ids_],
        "gamma": hexes(model.gamma_), "alpha_mean": hexes(model.alpha_mean_cluster_),
        "alpha_var": hexes(model.alpha_var_cluster_), "fitted": hexes(model.fitted_[:100]),
        "loglik": hexes([model.loglik_]), "iterations": int(model.iterations_), "convergence": hexes([model.convergence_]),
        "converged": bool(model.converged_), "warnings": caught,
    }


TEST_COLUMNS = ("estimate", "null_value", "z_raw", "p_value", "flag", "ci_lower", "ci_upper")


def inference(model):
    out = {"test": {}}
    for name, kwargs in (("exact_median", {}), ("exact_zero", {"reference": 0.0}),
                         ("exact_greater", {"alternative": "greater"}),
                         ("poibin_median", {"test_method": "poibin_exact"}),
                         ("resampling_median", {"test_method": "resampling", "n_resample": 20000, "seed": 7})):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            t = model.test(**kwargs)
        out["test"][name] = {c: (flags(t[c]) if c == "flag" else hexes(t[c].astype(float))) for c in TEST_COLUMNS}
    g = model.calculate_confidence_intervals(option="gamma")["gamma_ci"]
    out["gamma_ci"] = {"lower": hexes(g["gamma_lower"]), "upper": hexes(g["gamma_upper"])}
    for name, kwargs in (("sm_two_sided", {}), ("sm_greater", {"alternative": "greater"})):
        ci = model.calculate_confidence_intervals(option="SM", stdz=["indirect", "direct"], measure=["ratio", "rate"],
                                                  **kwargs)
        out[name] = {}
        for key, frame in ci.items():
            kind, measure = key.split("_")
            out[name][key] = {"value": hexes(frame[f"{kind}_{measure}"]),
                              "lower": hexes(frame[f"ci_{measure}_lower"]), "upper": hexes(frame[f"ci_{measure}_upper"])}
    for name, reference in (("median", "median"), ("mean", "mean"), ("zero", 0.0)):
        me = model.calculate_standardized_measures(stdz=["indirect", "direct"], reference=reference)
        out[f"measures_{name}"] = {kind: {c: hexes(me[kind][c]) for c in me[kind].columns if c != "provider_id"}
                                   for kind in ("indirect", "direct")}
    return out


def he2013(df, case):
    """Slice 4c-2 (D-65): the he2013 iteration under each criterion at its defaults and at tol 1e-12 (the fixed point),
    the exact tests and measures of the max_delta_gamma fixed point, and a negative control."""
    out = {}
    for criterion in ("max_delta_gamma", "relative"):
        default, caught = fit(df, case, False, "he2013", criterion)
        tight, caught_tight = fit(df, case, True, "he2013", criterion)
        out[criterion] = {"default": record(default, caught), "tight": record(tight, caught_tight)}
        if criterion == "max_delta_gamma":
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                t = tight.test()
            me = tight.calculate_standardized_measures(stdz=["indirect", "direct"], reference="median")
            out["inference"] = {
                "exact_median": {c: (flags(t[c]) if c == "flag" else hexes(t[c].astype(float))) for c in TEST_COLUMNS},
                "measures_median": {kind: {c: hexes(me[kind][c]) for c in me[kind].columns if c != "provider_id"}
                                    for kind in ("indirect", "direct")},
            }
    sizes = df.groupby("provider", sort=False).size()
    flipped = df.copy()
    rows = flipped["provider"] == sizes.idxmax()
    for column in ("y", "obs") if case.get("adjusted") else ("y",):
        flipped.loc[rows, column] = 1.0 - flipped.loc[rows, column]
    control, _ = fit(flipped, case, True, "he2013")
    out["negative_controls"] = {"flipped_provider": {"gamma": hexes(control.gamma_)}}
    return out


def negative_controls(df, case):
    sizes = df.groupby("provider", sort=False).size()
    key = sizes.idxmax()
    flipped = df.copy()
    rows = flipped["provider"] == key
    column = "obs" if case.get("adjusted") else "y"
    flipped.loc[rows, column] = 1.0 - flipped.loc[rows, column]
    if case.get("adjusted"):
        flipped.loc[rows, "y"] = 1.0 - flipped.loc[rows, "y"]
    model, _ = fit(flipped, case, True)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        z = model.test()["z_raw"]
    return {"flipped_provider": {"gamma": hexes(model.gamma_), "exact_median_z": hexes(z)}}


def generate(out, inputs, lock, write_json):
    """Writes out/fe-random-cluster/<case>/{case.json, input.csv, pprof_py.json}."""
    root = os.path.join(out, "fe-random-cluster")
    shutil.rmtree(root, ignore_errors=True)
    names = []
    for case in CASES:
        case_dir = os.path.join(root, case["id"])
        os.makedirs(case_dir)
        if inputs:
            shutil.copyfile(os.path.join(inputs, "fe-random-cluster", case["id"], "input.csv"),
                            os.path.join(case_dir, "input.csv"))
            df = pd.read_csv(os.path.join(case_dir, "input.csv"))
        else:
            df = make_data(case)
            df.to_csv(os.path.join(case_dir, "input.csv"), index=False)
        beta, sigma, start = given(df, case)
        default, caught = fit(df, case, False)
        tight, caught_tight = fit(df, case, True)
        reference = {"given": {"beta": hexes(beta), "sigma": hexes([sigma]), "start": hexes(start)},
                     "default": record(default, caught), "tight": record(tight, caught_tight),
                     "inference": inference(tight), "negative_controls": negative_controls(df, case),
                     "he2013": he2013(df, case)}
        write_json(os.path.join(case_dir, "case.json"), dict(case, features=["x1", "x2"], rows=len(df),
                                                            text=bool(case.get("text"))))
        write_json(os.path.join(case_dir, "pprof_py.json"), reference)
        names.append(case["id"])
        print("fe-random-cluster", case["id"], "done", flush=True)
    return names
