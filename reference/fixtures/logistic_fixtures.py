"""Logistic fixed-effect reference fixtures (Phase 2a; ADR-0005, D-30).

The cases of docs/spec/logistic/fixed-effect-estimation.md §12. Inputs are synthetic and exact in
text: integer outcomes and trials, features on a 1/64 grid (x3 is 0 or 1). pprof_py's outputs
come from LogisticFixedEffectModel (SerBIN, its defaults and a tight fit); R's from stats::glm
(the maximum-likelihood estimate and its variances) and from R pprof's SerBIN
(`logis_BIN_fe_prov`, compiled from the commit pinned in REFERENCE.lock). generate.py calls
`generate(out, inputs, lock)`; the output is deterministic.
"""

import hashlib
import logging
import os
import shutil
import subprocess
import tempfile
import urllib.request
import warnings

import numpy as np
import pandas as pd
from pprof_py.algorithms.logistic.fixed_effect import AlgorithmOptions, SerbinAlgorithm
from pprof_py.inference.standardized import at_bound
from pprof_py.models.logistic import LogisticFixedEffectModel
from scipy.special import expit

HERE = os.path.dirname(os.path.abspath(__file__))
FEATURES = ("x1", "x2", "x3")
TRUE_BETA = (0.5, -0.3, 0.8)
FIXED_BETA = (0.375, -0.1875, 0.0625)
TIGHT = {"tol": 1e-13}
R_PPROF_FILES = ("Fixed_effect.cpp", "header.h", "myomp.h")
BASE_SIZES = [11 + (7 * j) % 50 for j in range(30)]

CASES = (
    {"id": "lfe-base", "seed": 21, "sizes": BASE_SIZES},
    {"id": "lfe-degenerate", "seed": 22, "sizes": [11 + (5 * j) % 40 for j in range(20)] + [15, 12, 8, 10],
     "zero_events": [21], "all_events": [22]},
    {"id": "lfe-shifted", "seed": 21, "sizes": BASE_SIZES, "shift": [50.0, -30.0, 0.0]},
    {"id": "lfe-binomial", "seed": 24, "sizes": [11 + (3 * j) % 30 for j in range(30)], "max_trials": 20},
    {"id": "lfe-many", "seed": 25, "sizes": [20 + j % 20 for j in range(500)]},
    {"id": "lfe-clustered", "seed": 26, "sizes": BASE_SIZES, "patients": 3},
)

OPTIONS = {
    "pprof_py": "LogisticFixedEffectModel(algorithm='Serbin'): default fit (max_iter=10000, tol=1e-8, "
                "bound=10, backtrack=True, screening of providers with at most 10 records)",
    "tight": TIGHT,
    "iterates": "fit(max_iter=k - 1) gives the state after k Newton steps, k = 1 to 5",
    "function_level": "SerbinAlgorithm._compute_scores_and_info and _loglikelihood at 'start' and 'fixed'",
    "glm": "glm(cbind(y, n - y) ~ 0 + factor(provider) + x1 + x2 + x3, binomial, "
           "control = glm.control(epsilon = 1e-15, maxit = 200)); loglik without sum(lchoose(n, y))",
    "inference": "slice 2b at the default and tight fits: summary(test_method='lr' and 'score'), summary(null=0.25, "
                 "alternative='greater' and 'less'), auc_, predict for the first 100 rows of fitted providers; with "
                 "obs_id_var='patient' (lfe-clustered) robust_variances_ and summary(variance_type='robust')",
    "r_inference": "glm cases: 2 (logLik(full) - logLik(reduced)), anova(reduced, full, test = 'Rao'), the Mann-Whitney "
                   "AUC with rank() for Bernoulli rows, fitted values of the first 100 fitted rows; lfe-clustered: "
                   "sandwich::vcovCL(full, cluster = interaction(provider, patient, drop = TRUE), type = 'HC0', "
                   "cadjust = FALSE) and the beta-known variance A0 / I^2 from glm's residuals",
    "serbin": "R pprof logis_BIN_fe_prov(threads = 1, max_iter = 10000, bound = 10, backtrack = TRUE, "
              "stop = 'beta'), tol = 1e-8 (default) and 1e-13 (tight), on pprof_py's screened rows "
              "(binomial rows expanded to Bernoulli rows)",
}

_warnings = []


class _Count(logging.Handler):
    def emit(self, record):
        _warnings.append(record.getMessage())


logging.getLogger("pprof_py.algorithms.logistic.fixed_effect").addHandler(_Count(level=logging.WARNING))


def hexes(values):
    return [float(v).hex() for v in np.ravel(np.asarray(values, dtype=float))]


def packed(matrix):
    m = np.asarray(matrix, dtype=float)
    return [m[i, j] for i in range(m.shape[0]) for j in range(i, m.shape[0])]


def make_data(case):
    """Providers 1..m; within each, rows in generation order. Fails if an undesignated provider
    has no events or only events, so only lfe-degenerate has degenerate providers."""
    rng = np.random.Generator(np.random.PCG64(case["seed"]))
    binomial = "max_trials" in case
    parts = []
    for j, size in enumerate(case["sizes"], start=1):
        g = -0.5 + 0.5 * rng.standard_normal()
        x1 = np.rint(rng.standard_normal(size) * 64) / 64
        x2 = np.rint((0.5 * g + rng.standard_normal(size)) * 64) / 64
        x3 = (rng.random(size) < 0.4).astype(float)
        eta = g + TRUE_BETA[0] * x1 + TRUE_BETA[1] * x2 + TRUE_BETA[2] * x3
        n = rng.integers(1, case["max_trials"] + 1, size=size) if binomial else np.ones(size, dtype=np.int64)
        y = rng.binomial(n, 1.0 / (1.0 + np.exp(-eta)))
        if j in case.get("zero_events", ()):
            y[:] = 0
        elif j in case.get("all_events", ()):
            y[:] = n
        elif size > 10 and (y.sum() == 0 or y.sum() == n.sum()):
            raise ValueError(f"{case['id']}: provider {j} has no events or only events; change the seed")
        part = pd.DataFrame({"provider": j, "y": y, "x1": x1, "x2": x2, "x3": x3})
        if binomial:
            part.insert(2, "n", n)
        parts.append(part)
    df = pd.concat(parts, ignore_index=True)
    for name, shift in zip(FEATURES, case.get("shift", (0.0, 0.0, 0.0))):
        df[name] = df[name] + shift
    df.insert(0, "id", np.arange(len(df)))
    if "patients" in case:  # rows id // 3 share a patient; patients at a boundary span two providers
        df.insert(2, "patient", df["id"] // case["patients"])
    return df


def fit(df, **control):
    model = LogisticFixedEffectModel()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model.fit(df, x_vars=list(FEATURES), y_var="y", provider_var="provider",
                  n_var="n" if "n" in df.columns else None,
                  obs_id_var="patient" if "patient" in df.columns else None, **control)
    return model


def summary(model, logged):
    alg = model.algorithm
    beta = np.asarray(model.coefficients_["beta"], dtype=float).ravel()
    gamma = np.asarray(model.coefficients_["gamma"], dtype=float).ravel()
    wald = model.summary()
    excluded = model.excluded_providers_
    return {
        "beta": hexes(beta), "gamma": hexes(gamma),
        "var_beta": hexes(packed(model.variances_["beta"])),
        "var_gamma": hexes(model.variances_["gamma"]), "var_case_mix": hexes(model.variances_["gamma_case_mix"]),
        "se": hexes(wald["std_error"]), "z": hexes(wald["stat"]), "p": hexes(wald["p_value"]),
        "ci_lower": hexes(wald["ci_lower"]), "ci_upper": hexes(wald["ci_upper"]),
        "loglik": hexes([alg._loglikelihood(gamma[model.provider_indices_], beta)]),
        "aic": hexes([model.aic_]), "bic": hexes([model.bic_]),
        "iterations": int(alg.iter), "criterion": hexes([alg.beta_crit]),
        "last_step": hexes([alg._last_step[0]]) if hasattr(alg, "_last_step") else [],
        "providers": [int(p) for p in model.provider_ids_],
        "excluded": [] if excluded is None else [int(p) for p in excluded.index],
        "at_bound": [bool(v) for v in at_bound(model)],
        "warnings": logged,
    }


def fitted(df, **control):
    _warnings.clear()
    model = fit(df, **control)
    return model, summary(model, len(_warnings))


def iterates(df, count=5):
    """Lockstep parity (§9.2): the state after k Newton steps, k = 1 to count."""
    out = []
    for k in range(1, count + 1):
        model = fit(df, max_iter=k - 1)
        alg = model.algorithm
        out.append({"steps": int(alg.iter), "beta": hexes(model.coefficients_["beta"]),
                    "gamma": hexes(model.coefficients_["gamma"]), "step": hexes([alg._last_step[0]])})
    return out


def function_values(model, gamma, beta):
    """ℓ, scores and information blocks at fixed (γ, β), with pprof_py's own arithmetic."""
    X = np.asarray(model.X, dtype=float)
    idx = np.asarray(model.provider_indices_)
    m = len(model.provider_ids_)
    gamma, beta = np.asarray(gamma, dtype=float), np.asarray(beta, dtype=float)
    alg = SerbinAlgorithm(np.column_stack((model.outcome_, idx, X)), 0, X, 1, np.bincount(idx, minlength=m),
                          gamma.copy(), beta.copy(), AlgorithmOptions(), N=model.N_)
    p = expit(gamma[idx] + X @ beta)
    q = model.N_ * p * (1 - p)
    score_gamma, score_beta, info_gamma_inv, info_beta_gamma, info_beta = alg._compute_scores_and_info(p, q)
    schur = info_beta - (info_beta_gamma * info_gamma_inv) @ info_beta_gamma.T
    return {"gamma": hexes(gamma), "beta": hexes(beta),
            "loglik": hexes([alg._loglikelihood(gamma[idx], beta)]),
            "score_gamma": hexes(score_gamma), "score_beta": hexes(score_beta),
            "info_gamma": hexes(np.bincount(idx, weights=np.maximum(q, 1e-20), minlength=m)),
            "info_beta_gamma": hexes(info_beta_gamma), "info_beta": hexes(packed(info_beta)),
            "schur": hexes(packed(schur))}


def inference(model, df):
    """Slice 2b (docs/spec/logistic/covariate-inference.md): LR and score tests, Wald variants, AUC,
    predictions for the first 100 rows of fitted providers, and robust variances with clusters."""
    out = {}
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for test in ("lr", "score"):
            s = model.summary(test_method=test)
            out[test] = {"stat": hexes(s["stat"]), "p": hexes(s["p_value"])}
        for alternative in ("greater", "less"):
            s = model.summary(null=0.25, alternative=alternative)
            out[f"wald_{alternative}"] = {"stat": hexes(s["stat"]), "p": hexes(s["p_value"]),
                                          "ci_lower": hexes(s["ci_lower"]), "ci_upper": hexes(s["ci_upper"])}
        out["auc"] = [] if model.auc_ is None else hexes([model.auc_])
        rows = df[df["provider"].isin(model.provider_ids_)].head(100)
        out["predict_ids"] = [int(i) for i in rows["id"]]
        out["predict"] = hexes(model.predict(rows, x_vars=list(FEATURES), provider_var="provider"))
        if model.robust_variances_ is not None:
            robust = model.robust_variances_
            out["robust"] = {"var_beta": hexes(packed(robust["beta"])), "var_case_mix": hexes(robust["gamma"]),
                             "var_fixed_beta": hexes(robust["gamma_fixed_beta"])}
            s = model.summary(variance_type="robust")
            out["wald_robust"] = {"stat": hexes(s["stat"]), "p": hexes(s["p_value"]), "se": hexes(s["std_error"])}
    return out


def negative_controls(df, case):
    controls = {}
    flipped = df.copy()
    row = int(np.flatnonzero(flipped["provider"].to_numpy() == 1)[0])
    if "n" in flipped.columns:
        y, n = int(flipped.at[row, "y"]), int(flipped.at[row, "n"])
        flipped.at[row, "y"] = y + 1 if y < n else y - 1
    else:
        flipped.at[row, "y"] = 1 - int(flipped.at[row, "y"])
    flipped_model, controls["flipped_outcome"] = fitted(flipped, **TIGHT)
    controls["flipped_outcome"]["inference"] = inference(flipped_model, flipped)
    if case.get("zero_events") or case.get("all_events"):
        bound_model, controls["bound_5"] = fitted(df, bound=5.0, **TIGHT)
        controls["bound_5"]["inference"] = inference(bound_model, df)
    if "n" in df.columns:
        added = df.copy()
        added.at[0, "n"] = int(added.at[0, "n"]) + 1
        added_model, controls["one_trial_added"] = fitted(added, **TIGHT)
        controls["one_trial_added"]["inference"] = inference(added_model, added)
    if "patient" in df.columns:
        shifted = df.copy()
        shifted["patient"] = (shifted["id"] + 1) // case["patients"]
        shifted_model, controls["cluster_shifted"] = fitted(shifted, **TIGHT)
        controls["cluster_shifted"]["inference"] = inference(shifted_model, shifted)
    return controls


def r_pprof_sources(lock, directory):
    """R pprof's SerBIN sources at the pinned commit, checked against REFERENCE.lock."""
    pin = lock["r_pprof"]
    os.makedirs(directory, exist_ok=True)
    for name in R_PPROF_FILES:
        path = os.path.join(directory, name)
        if not os.path.exists(path):
            url = f"https://raw.githubusercontent.com/cran/pprof/{pin['commit']}/src/{name}"
            with urllib.request.urlopen(url, timeout=60) as response, open(path, "wb") as handle:
                handle.write(response.read())
        with open(path, "rb") as handle:
            digest = hashlib.sha256(handle.read()).hexdigest()
        if digest != pin["sha256"][name]:
            raise SystemExit(f"{name}: sha256 {digest} differs from REFERENCE.lock")
    return directory


def r_outputs(case_dir, case, excluded, sources):
    degenerate = case.get("zero_events", []) + case.get("all_events", [])
    subprocess.run(["Rscript", os.path.join(HERE, "logistic_r.R"), case_dir, str("max_trials" in case).lower(),
                    sources, ",".join(map(str, excluded)), ",".join(map(str, degenerate)),
                    str("patients" in case).lower()], check=True)
    path = os.path.join(case_dir, "r_logistic.txt")
    result = {}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            key, values = line.rstrip("\n").split("\t")
            label, quantity = key.split(".")
            parsed = [float.fromhex(v) for v in values.split(",")]
            result.setdefault(label, {})[quantity] = int(parsed[0]) if quantity == "iterations" else hexes(parsed)
    os.remove(path)
    return result


def generate(out, inputs, lock, write_json):
    """Writes out/logistic/<case>/{case.json, input.csv, pprof_py.json, r_logistic.json}."""
    root = os.path.join(out, "logistic")
    shutil.rmtree(root, ignore_errors=True)
    sources = r_pprof_sources(lock, os.path.join(tempfile.gettempdir(), "pprof_spark_r_pprof_" + lock["r_pprof"]["commit"][:12]))
    names = []
    for case in CASES:
        case_dir = os.path.join(root, case["id"])
        os.makedirs(case_dir)
        if inputs:
            shutil.copyfile(os.path.join(inputs, "logistic", case["id"], "input.csv"), os.path.join(case_dir, "input.csv"))
            df = pd.read_csv(os.path.join(case_dir, "input.csv"))
        else:
            df = make_data(case)
            df.to_csv(os.path.join(case_dir, "input.csv"), index=False)
        model, default = fitted(df)
        tight_model, tight = fitted(df, **TIGHT)
        y_bar = float(np.sum(tight_model.outcome_) / np.sum(tight_model.N_))
        m = len(tight_model.provider_ids_)
        start = function_values(tight_model, np.full(m, np.log(y_bar / (1 - y_bar))), np.zeros(len(FEATURES)))
        fixed = function_values(tight_model, -0.5 + 0.125 * (np.arange(m) % 5 - 2), FIXED_BETA)
        definition = dict(case, features=list(FEATURES), fixed_beta=list(FIXED_BETA), rows=len(df),
                          trials=int(df["n"].sum()) if "n" in df.columns else len(df), events=int(df["y"].sum()),
                          binomial="max_trials" in case)
        write_json(os.path.join(case_dir, "case.json"), definition)
        write_json(os.path.join(case_dir, "pprof_py.json"), {
            "default": default, "tight": tight, "iterates": iterates(df),
            "function": {"start": start, "fixed": fixed}, "negative_controls": negative_controls(df, case),
            "inference": {"default": inference(model, df), "tight": inference(tight_model, df)}})
        write_json(os.path.join(case_dir, "r_logistic.json"), r_outputs(case_dir, case, default["excluded"], sources))
        names.append(case["id"])
    return names
