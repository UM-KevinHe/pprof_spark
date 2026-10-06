#!/usr/bin/env python3
"""Generate pprof_spark's Cox reference fixtures (PROJECT_CONTEXT §9.3, PAR-1; ADR-0005).

Inputs are synthetic and exact in text: integer event times, covariates on a 1/64 grid, offsets
on a 1/16 grid and case weights on a 1/2 grid, so every implementation reads the same numbers.
Reference outputs come from pprof_py (the pin in reference/REFERENCE.lock) and from R's
survival::coxph, stored as hexadecimal floating-point strings, which round-trip exactly in Python,
R and Java. Negative controls (other tie method, one tied time shifted by a day, one weight
dropped) are stored next to them, so tests can show that the tolerances still detect real
defects (NN-9, D-09). Output is deterministic: rerunning reproduces every file byte for byte.

Usage, from the repository root, in the fixture environment (requirements.txt, pprof_py at the
pinned commit, R 4.3.3 with survival 3.5-8):
    python reference/fixtures/generate.py [--out fixtures] [--inputs fixtures]
With --inputs, the inputs are copied from an existing fixture tree instead of being generated:
inputs are generated once (§9.3), and a re-pin or a check recomputes only the outputs.
"""

import argparse
import hashlib
import warnings
import json
import os
import shutil
import subprocess
import sys
import tomllib
from importlib import metadata

import numpy as np
import pandas as pd
from pprof_py.algorithms.survival.cox_likelihood import cox_partial_likelihood
from pprof_py.data.survival_data import SurvivalData
from pprof_py.data.survival_validation import validate_fit_inputs
from pprof_py.models.survival.coxph import CoxPH

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
FORMAT_VERSION = 1
TIES = ("breslow", "efron")
FIXED_BETA = (0.375, -0.1875, 0.0625)
TIGHT = {"max_iter": 100, "eps": 1e-11}

CASES = (
    {"id": "tiny-ties", "p": 1, "stratified": False, "weighted": False, "offset": False},
    {"id": "rc-unstratified", "seed": 2, "n": 400, "p": 3, "strata": 1, "max_time": 60,
     "stratified": False, "weighted": False, "offset": False},
    {"id": "rc-stratified", "seed": 3, "n": 600, "p": 3, "strata": 25, "max_time": 60,
     "stratified": True, "weighted": False, "offset": False},
    {"id": "rc-stratified-weights-offset", "seed": 4, "n": 600, "p": 3, "strata": 25, "max_time": 60,
     "stratified": True, "weighted": True, "offset": True},
    {"id": "lt-stratified", "seed": 5, "n": 600, "p": 3, "strata": 10, "max_time": 60,
     "stratified": True, "weighted": False, "offset": False, "truncated": True},
    {"id": "lt-weights-offset", "seed": 6, "n": 600, "p": 3, "strata": 10, "max_time": 60,
     "stratified": True, "weighted": True, "offset": True, "truncated": True},
)

# Covariate profiles for prediction fixtures (Phase 1b specification §3).
PROFILES = ((0.0, 0.0, 0.0), (0.5, -0.25, 0.125), (-1.0, 0.75, 0.5))
PROFILE_OFFSETS = (0.0, 0.25, -0.5)


def tiny():
    """Eight rows, one covariate, two tied event times: small enough to check by hand."""
    return pd.DataFrame({
        "id": range(8), "stratum": [0] * 8, "time": [1, 1, 2, 2, 3, 3, 4, 4],
        "event": [1, 1, 0, 1, 1, 1, 0, 1], "weight": [1.0] * 8, "offset": [0.0] * 8,
        "x1": [0.5, -0.25, 1.0, 0.0, -1.0, 0.75, 0.25, -0.5],
    })


def make_data(case):
    if case["id"] == "tiny-ties":
        return tiny()
    rng = np.random.Generator(np.random.PCG64(case["seed"]))
    n, p = case["n"], case["p"]
    x = rng.integers(-128, 129, size=(n, p)) / 64.0
    stratum = rng.integers(0, case["strata"], size=n)
    offset = rng.integers(-8, 9, size=n) / 16.0 if case["offset"] else np.zeros(n)
    weight = rng.integers(1, 5, size=n) / 2.0 if case["weighted"] else np.ones(n)
    eta = x @ np.array([0.5, -0.25, 0.125][:p]) + offset
    t_event = np.ceil(-np.log(rng.random(n)) / np.exp(eta) * case["max_time"] / 4.0)
    t_censor = rng.integers(1, case["max_time"] + 1, size=n)
    time = np.clip(np.minimum(t_event, t_censor), 1, case["max_time"]).astype(np.int64)
    event = (t_event <= t_censor).astype(np.int64)
    if case["stratified"]:
        event[stratum == 0] = 0  # one stratum without events (§9.5)
    df = pd.DataFrame({"id": np.arange(n), "stratum": stratum, "time": time, "event": event,
                       "weight": weight, "offset": offset})
    if case.get("truncated"):
        # Delayed entry for about half the rows, strictly before exit (Phase 1b specification §1).
        delayed = rng.random(n) < 0.5
        df.insert(2, "entry", np.where(delayed, rng.integers(0, time), 0).astype(np.int64))
    for j in range(p):
        df[f"x{j + 1}"] = x[:, j]
    return df


def features(case):
    return [f"x{j + 1}" for j in range(case["p"])]


def fit_arguments(df, case):
    if case.get("truncated"):
        arguments = {"start": df["entry"].to_numpy(dtype=float), "stop": df["time"].to_numpy(dtype=float),
                     "event": df["event"].to_numpy()}
    else:
        arguments = {"duration": df["time"].to_numpy(dtype=float), "event": df["event"].to_numpy()}
    if case["stratified"]:
        arguments["strata"] = df["stratum"].to_numpy()
    if case["offset"]:
        arguments["offset"] = df["offset"].to_numpy(dtype=float)
    if case["weighted"]:
        arguments["sample_weight"] = df["weight"].to_numpy(dtype=float)
    return arguments


def hexes(values):
    return [float(v).hex() for v in np.ravel(np.asarray(values, dtype=float))]


def packed(matrix):
    m = np.asarray(matrix, dtype=float)
    return [m[i, j] for i in range(m.shape[0]) for j in range(i, m.shape[0])]


def pprof_fit(df, case, ties, **control):
    model = CoxPH(ties=ties, **control).fit(df[features(case)].to_numpy(dtype=float), **fit_arguments(df, case))
    return {
        "coef": hexes(model.coef_),
        "se": hexes(model.standard_errors_),
        "covariance": hexes(packed(model.covariance_)),
        "loglik": hexes([model.log_likelihood_]),
        "loglik_null": hexes([model.log_likelihood_null_]),
        "iterations": int(model.n_iter_),
        "converged": bool(model.converged_),
        "z": hexes(model.z_scores_),
        "p": hexes(model.p_values_),
        "ci_lower": hexes(np.asarray(model.confidence_intervals_)[:, 0]),
        "ci_upper": hexes(np.asarray(model.confidence_intervals_)[:, 1]),
    }


def pprof_iterates(df, case, ties, count=5):
    """Newton iterates for lockstep parity (§9.2): the fit after k iterations, k = 1 to count."""
    iterates = []
    for k in range(1, count + 1):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # CoxPH warns when max_iter stops it early
            model = CoxPH(ties=ties, max_iter=k).fit(df[features(case)].to_numpy(dtype=float), **fit_arguments(df, case))
        iterates.append({"iterations": int(model.n_iter_), "beta": hexes(model.coef_),
                         "loglik": hexes([model.log_likelihood_])})
    return iterates


def label(value):
    """A stratum label as JSON: an integer when it is one."""
    return int(value) if float(value).is_integer() else str(value)


def pprof_baseline(df, case, ties):
    """Raw and public baselines and predictions of the tight fit (Phase 1b specification §2, §3)."""
    model = CoxPH(ties=ties, **TIGHT).fit(df[features(case)].to_numpy(dtype=float), **fit_arguments(df, case))
    raw = model._baseline_hazard_raw.sort_values(["stratum", "time"], kind="stable")
    public = model.baseline_hazard_.sort_values(["stratum", "time"], kind="stable")
    p = case["p"]
    x = np.array([profile[:p] for profile in PROFILES], dtype=float)
    offsets = np.array(PROFILE_OFFSETS, dtype=float) if case["offset"] else None
    predictions = []
    for stratum in list(dict.fromkeys(raw["stratum"].tolist()))[:2]:
        hazard = model.predict_cumulative_hazard(x, offset=offsets, stratum=stratum)
        survival = model.predict_survival_function(x, offset=offsets, stratum=stratum)
        predictions.append({"stratum": label(stratum), "time": hexes(hazard.index.to_numpy(dtype=float)),
                            "cumulative_hazard": [hexes(hazard[c].to_numpy()) for c in hazard.columns],
                            "survival": [hexes(survival[c].to_numpy()) for c in survival.columns]})
    weights = df["weight"].to_numpy(dtype=float) if case["weighted"] else np.ones(len(df))
    return {
        "raw": {"stratum": [label(s) for s in raw["stratum"]], "time": hexes(raw["time"].to_numpy(dtype=float)),
                "cumulative_hazard": hexes(raw["hazard"].to_numpy()), "survival": hexes(raw["survival"].to_numpy())},
        "public_cumulative_hazard": hexes(public["hazard"].to_numpy()),
        "offset_mean": hexes([np.average(df["offset"].to_numpy(dtype=float), weights=weights) if case["offset"] else 0.0]),
        "profiles": {"x": [hexes(row) for row in x], "offset": hexes(offsets if offsets is not None else np.zeros(len(x))),
                     "linear": hexes(model.predict_linear(x, offset=offsets)),
                     "relative_hazard": hexes(model.predict_partial_hazard(x, offset=offsets))},
        "predictions": predictions,
    }


def pprof_function(df, case, ties, beta):
    clean = validate_fit_inputs(df[features(case)].to_numpy(dtype=float), **fit_arguments(df, case))
    data = SurvivalData(**clean)
    loglik, score, information = cox_partial_likelihood(
        data.X, data.start, data.stop, data.event, np.asarray(beta, dtype=float),
        offset=data.offset, weight=data.weight, strata=data.strata_codes, ties=ties)
    return {"beta": hexes(beta), "loglik": hexes([loglik]), "score": hexes(score),
            "information": hexes(packed(information))}


def shifted_tie(df):
    """Moves one of two or more tied events a day later: a real defect a tolerance must catch."""
    events = df[df["event"] == 1]
    counts = events.groupby(["stratum", "time"]).size()
    stratum, time = counts[counts >= 2].index[0]
    row = events[(events["stratum"] == stratum) & (events["time"] == time)].index[0]
    shifted = df.copy()
    shifted.loc[row, "time"] = time + 1
    return shifted


def dropped_weight(df):
    """Sets the first weight different from 1 to 1."""
    changed = df.copy()
    row = changed.index[changed["weight"] != 1.0][0]
    changed.loc[row, "weight"] = 1.0
    return changed


def r_outputs(case_dir, case):
    flags = [str(case.get(k, False)).lower() for k in ("stratified", "weighted", "offset", "truncated")]
    subprocess.run(["Rscript", os.path.join(HERE, "cox_survival.R"), case_dir, str(case["p"]), *flags], check=True)
    path = os.path.join(case_dir, "r_survival.txt")
    result = {}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            key, values = line.rstrip("\n").split("\t")
            ties, label, quantity = key.split(".")
            parsed = [float.fromhex(v) for v in values.split(",")]
            entry = result.setdefault(ties, {}).setdefault(label, {})
            entry[quantity] = int(parsed[0]) if quantity == "iterations" else hexes(parsed)
    os.remove(path)
    return result


def write_json(path, value):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(value, handle, indent=1, sort_keys=True)
        handle.write("\n")


def sha256(path):
    with open(path, "rb") as handle:
        return hashlib.sha256(handle.read()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default=os.path.join(ROOT, "fixtures"))
    parser.add_argument("--inputs", help="copy input.csv files from this fixture tree")
    options = parser.parse_args()
    out = options.out
    with open(os.path.join(ROOT, "reference", "REFERENCE.lock"), "rb") as handle:
        lock = tomllib.load(handle)
    installed = metadata.version("pprof_py")
    if installed != lock["pprof_py"]["version"]:
        sys.exit(f"pprof_py {installed} is installed; REFERENCE.lock pins {lock['pprof_py']['version']}")
    r_version = subprocess.run(
        ["Rscript", "-e", 'cat(R.version$major, R.version$minor, as.character(packageVersion("survival")), sep=" ")'],
        check=True, capture_output=True, text=True).stdout.split()

    shutil.rmtree(out, ignore_errors=True)
    cases = []
    for case in CASES:
        case_dir = os.path.join(out, "cox", case["id"])
        os.makedirs(case_dir)
        if options.inputs:
            shutil.copyfile(os.path.join(options.inputs, "cox", case["id"], "input.csv"),
                            os.path.join(case_dir, "input.csv"))
            df = pd.read_csv(os.path.join(case_dir, "input.csv"))
        else:
            df = make_data(case)
            df.to_csv(os.path.join(case_dir, "input.csv"), index=False)
        definition = dict(case, features=features(case), fixed_beta=list(FIXED_BETA[:case["p"]]),
                          rows=len(df), events=int(df["event"].sum()))
        write_json(os.path.join(case_dir, "case.json"), definition)
        reference = {}
        for ties in TIES:
            tight = pprof_fit(df, case, ties, **TIGHT)
            controls = {"other_ties": pprof_fit(df, case, "efron" if ties == "breslow" else "breslow", **TIGHT),
                        "shifted_tie": pprof_fit(shifted_tie(df), case, ties, **TIGHT)}
            if case["weighted"]:
                controls["dropped_weight"] = pprof_fit(dropped_weight(df), case, ties, **TIGHT)
            reference[ties] = {
                "default": pprof_fit(df, case, ties),
                "tight": tight,
                "beta_zero": pprof_function(df, case, ties, [0.0] * case["p"]),
                "beta_fixed": pprof_function(df, case, ties, FIXED_BETA[:case["p"]]),
                "iterates": pprof_iterates(df, case, ties),
                "baseline": pprof_baseline(df, case, ties),
                "negative_controls": controls,
            }
        write_json(os.path.join(case_dir, "pprof_py.json"), reference)
        write_json(os.path.join(case_dir, "r_survival.json"), r_outputs(case_dir, case))
        cases.append(case["id"])

    files = {}
    for directory, _, names in os.walk(out):
        for name in names:
            path = os.path.join(directory, name)
            files[os.path.relpath(path, out).replace(os.sep, "/")] = sha256(path)
    packages = ("numpy", "scipy", "pandas", "numba", "llvmlite")
    write_json(os.path.join(out, "manifest.json"), {
        "formatVersion": FORMAT_VERSION,
        "generator": "reference/fixtures/generate.py",
        "cases": {"cox": cases},
        "reference": {"pprof_py": {"version": installed, "commit": lock["pprof_py"]["commit"]},
                      "python": sys.version.split()[0],
                      "packages": {name: metadata.version(name) for name in packages},
                      "r": {"version": f"{r_version[0]}.{r_version[1]}", "survival": r_version[2]}},
        "options": {"ties": list(TIES), "tight": TIGHT, "r_timefix": True,
                    "r_tight_control": "coxph.control(eps = 1e-11, iter.max = 100)",
                    "function_level": "R: coxph(init = beta, control = coxph.control(iter.max = 0))",
                    "r_variance": "model-based: coxph(robust = FALSE)"},
        "files": dict(sorted(files.items())),
    })
    print(f"wrote {len(files)} fixture files for {len(cases)} Cox cases to {out}")


if __name__ == "__main__":
    main()
