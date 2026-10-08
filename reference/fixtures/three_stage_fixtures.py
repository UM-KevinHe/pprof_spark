"""Three-stage model fixtures (docs/spec/logistic/three-stage-preparation.md, slice 2f-1).

Cases: two of pprof_py's own golden data sets with their R outputs (R's glmm.data.prep and the stage 1
beta R was given), downloaded at the pinned commit and checked against REFERENCE.lock, and one
generated case with text keys, degenerate providers and providers at the cutoff. Outputs: pprof_py's
preparation (glmm_data_prep) and stage 1 (LogisticFixedEffectModel on the included cells), at the
default and a tight tolerance, with negative controls; R's stage 1 by glm on the same cells.
"""
import hashlib
import json
import logging
import os
import shutil
import subprocess
import tempfile
import urllib.request
import warnings

import numpy as np
import pandas as pd

from logistic_fixtures import hexes, packed

HERE = os.path.dirname(os.path.abspath(__file__))
SEPARATOR = "\x1f"  # cell keys: cluster, U+001F, provider (spec 2f-1 §4)
TIGHT = {"tol": 1e-13}
CUTOFF = 10
CASES = (
    {"id": "ts-golden-prep", "source": "glmm_prep/raw.csv", "features": ["age"], "r_prep": True},
    {"id": "ts-golden", "source": "three_stage/raw.csv", "features": ["age", "diabetes", "chf", "comorb", "female"],
     "r_stage23": True, "stage3": True},
    {"id": "ts-synthetic", "seed": 31, "features": ["x1", "x2", "x3"], "stage3": True},
)
OPTIONS = {
    "cutoff": CUTOFF, "tight": TIGHT,
    "preparation": "pprof_py glmm_data_prep(data, 'Y', 'fac', 'hosp', cutoff=10); cells keyed hosp U+001F fac",
    "stage1": "LogisticFixedEffectModel(use_dataprep=False, screen_providers=False).fit(included rows, provider_var="
              "'cell_id'), default and tol 1e-13; offset = features @ beta (default fit)",
    "r_stage1": "glm(Y ~ 0 + cell + features, binomial, epsilon 1e-15, maxit 200) on the included rows",
    "stage3": "slice 2f-2: LogisticThreeStageModel() (marginal) gives beta, sigma and the start; "
              "LogisticFERandomClusterModel(tol=1e-5 and 1e-10).fit(beta=, sigma=, gamma_init=); iterates by max_iter "
              "1 to 3; marginal log-likelihood and score at the start and at start + 0.05 (k mod 5 - 2); sigma 1e-4 and the "
              "sigma = 0 limit (Newton in numpy); controls sigma + 0.01 and one y_adj flipped; Gauss-Hermite rules "
              "from mpmath (50 digits) and numpy's hermgauss",
    "golden": "pprof_py tests/data/glmm_prep (R glmm.data.prep) and tests/data/three_stage (R stage 1 beta, "
              "glmer sigma, glmm.fac.hosp gamma and SRR), files checked against REFERENCE.lock",
}

logging.disable(logging.WARNING)


def golden_file(lock, relative):
    """One of pprof_py's test data files at the pinned commit, checked against REFERENCE.lock."""
    pin = lock["pprof_py_test_data"]
    directory = os.path.join(tempfile.gettempdir(), "pprof_spark_pprof_py_data_" + lock["pprof_py"]["commit"][:12])
    path = os.path.join(directory, relative.replace("/", "_"))
    if not os.path.exists(path):
        os.makedirs(directory, exist_ok=True)
        url = f"https://raw.githubusercontent.com/UM-KevinHe/pprof_py/{lock['pprof_py']['commit']}/pprof_py/tests/data/{relative}"
        urllib.request.urlretrieve(url, path)
    digest = hashlib.sha256(open(path, "rb").read()).hexdigest()
    if digest != pin["sha256"][relative]:
        raise SystemExit(f"{relative}: sha256 {digest} does not match REFERENCE.lock")
    return path


def synthetic(case):
    """Text keys, a provider at the cutoff (10 records, excluded) and just above it (11), providers without
    events and with only events, records spread over two to four hospitals; covariates on exact grids."""
    rng = np.random.Generator(np.random.PCG64(case["seed"]))
    hospitals = [f"H{h}" for h in range(1, 9)]
    effects_h = rng.normal(0.0, 0.4, len(hospitals))
    rows = []
    for j in range(1, 31):
        size = {28: 10, 29: 11, 26: 25, 27: 25}.get(j, 40 + (13 * j) % 50)
        primary = j % len(hospitals)
        others = rng.choice([h for h in range(len(hospitals)) if h != primary], size=1 + j % 3, replace=False)
        u = rng.normal(-1.0, 0.5)
        for i in range(size):
            h = primary if rng.random() < 0.6 else int(rng.choice(others))
            x1 = np.rint(rng.normal() * 64) / 64
            x2 = np.rint(rng.uniform(-1, 1) * 64) / 64
            x3 = float(rng.random() < 0.3)
            p = 1.0 / (1.0 + np.exp(-(u + effects_h[h] + 0.4 * x1 - 0.3 * x2 + 0.5 * x3)))
            y = 0 if j == 26 else 1 if j == 27 else int(rng.random() < p)
            rows.append((f"F{j:02d}", hospitals[h], y, x1, x2, x3))
    df = pd.DataFrame(rows, columns=["fac", "hosp", "Y", "x1", "x2", "x3"])
    df.insert(0, "rid", np.arange(1, len(df) + 1))
    return df


def key(hosp, fac):
    return hosp.astype(str) + SEPARATOR + fac.astype(str)


def prepared(df, cutoff=CUTOFF):
    from pprof_py.data.glmm_prep import glmm_data_prep
    prep = glmm_data_prep(df, "Y", "fac", "hosp", cutoff=cutoff)
    return prep, prep.data.assign(cell=lambda d: key(d["hosp"], d["fac"]))


def preparation(df, cutoff=CUTOFF):
    prep, d = prepared(df, cutoff)
    rows = d.sort_values("rid")
    excluded = prep.excluded_providers  # indexed by provider_id, with n_records and reason
    return {"kept": len(d), "providers": int(prep.n_providers), "clusters": int(prep.n_clusters),
            "cells": int(d["cell"].nunique()), "included_cells": int(d.loc[d["included"] == 1, "cell"].nunique()),
            "excluded": sorted(str(v) for v in excluded.index),
            "excluded_records": [int(excluded.loc[v, "n_records"]) for v in sorted(excluded.index, key=str)],
            "rid": [int(v) for v in rows["rid"]], "y_adj": hexes(rows["y_adj"]),
            "cell": [str(v) for v in rows["cell"]], "included": [int(v) for v in rows["included"]]}


def stage1(df, features, **control):
    from pprof_py.models.logistic import LogisticFixedEffectModel
    _, d = prepared(df)
    included = d[d["included"] == 1]
    model = LogisticFixedEffectModel(use_dataprep=False, screen_providers=False)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model.fit(X=included, y_var="Y", x_vars=list(features), provider_var="cell_id", **control)
    cells = included.drop_duplicates("cell_id").set_index("cell_id")["cell"]
    beta = np.asarray(model.coefficients_["beta"], dtype=float).ravel()
    ids = [cells[int(c)] for c in model.provider_ids_]
    order = np.argsort(ids, kind="stable")
    out = {"beta": hexes(beta), "var_beta": hexes(packed(np.asarray(model.variances_["beta"]))),
           "cells": [ids[i] for i in order],
           "gamma": hexes(np.asarray(model.coefficients_["gamma"], dtype=float).ravel()[order])}
    rows = d.sort_values("rid")
    out["offset"] = hexes(rows[list(features)].to_numpy(dtype=np.float64) @ beta)
    return out


def r_stage1(df, features, case_dir):
    _, d = prepared(df)
    path = os.path.join(case_dir, "included.csv")
    d.loc[d["included"] == 1, ["cell", "Y"] + list(features)].to_csv(path, index=False)
    subprocess.run(["Rscript", os.path.join(HERE, "three_stage_r.R"), path, ",".join(features),
                    os.path.join(case_dir, "r.txt")], check=True)
    beta = [float.fromhex(v) for v in open(os.path.join(case_dir, "r.txt")).read().strip().split(",")]
    os.remove(path)
    os.remove(os.path.join(case_dir, "r.txt"))
    return hexes(beta)


def r_golden(lock, case, df):
    out = {}
    if case.get("r_prep"):
        r = pd.read_csv(golden_file(lock, "glmm_prep/prep_R.csv")).sort_values("rid")
        dense = json.load(open(golden_file(lock, "glmm_prep/prep_R.json")))
        out["prep"] = {"kept": len(r), "rid": [int(v) for v in r["rid"]], "y_adj": hexes(r["Y.adj"]),
                       "cell": [str(v) for v in key(r["hosp"], r["fac"])], "included": [int(v) for v in r["included"]],
                       "cells_present": int(np.count_nonzero(dense["n_fac_hosp"]))}
    if case.get("r_stage23"):
        golden = json.load(open(golden_file(lock, "three_stage/r_stage23.json")))
        out["stage23"] = {name: hexes(np.ravel(np.asarray(value, dtype=float))) for name, value in golden.items()}
        out["stage23"]["providers"] = sorted({str(v) for v in df["fac"]}, key=lambda s: (len(s), s))
    return out


def stage3_outputs(df, features):
    """Slice 2f-2 (docs/spec/logistic/three-stage-stage3.md): pprof_py's stage 3 with beta, sigma and the
    start from its own pipeline."""
    from pprof_py import LogisticThreeStageModel
    from pprof_py.models.logistic.fe_random_cluster import (LogisticFERandomClusterModel, _adaptive_rule,
                                                             _cluster_modes, _marginal_terms)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        pipeline = LogisticThreeStageModel().fit(df, "Y", list(features), "fac", "hosp")
    beta, sigma, start = pipeline.stage3_._values_from_stages(pipeline.stage1_, pipeline.stage2_, list(features),
                                                              "fac", "hosp")
    d = pipeline.data_

    def fit(tol=1e-5, sig=sigma, gamma0=start, max_iter=10000, data=d):
        model = LogisticFERandomClusterModel(tol=tol, max_iter=max_iter)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.fit(data, "y_adj", list(features), "fac", "hosp", beta=beta, sigma=sig, gamma_init=np.array(gamma0),
                      obs_var="Y", verbose=False)
        return model

    def summary(m):
        rows = d.assign(fitted=m.fitted_).sort_values("rid")
        return {"gamma": hexes(m.gamma_), "iterations": int(m.iterations_), "converged": bool(m.converged_),
                "criterion": hexes([m.convergence_]), "loglik": hexes([m.loglik_]),
                "alpha_mean": hexes(m.alpha_mean_cluster_), "alpha_var": hexes(m.alpha_var_cluster_),
                "fitted": hexes(rows["fitted"])}

    default, tight = fit(), fit(tol=1e-10)
    t_nodes, log_wt = _adaptive_rule(20)
    prov, clust, xb, y = tight._provider_idx, tight._cluster_idx, tight.xbeta_, tight._y

    def function(gamma):
        center, scale = _cluster_modes(gamma, xb, y, prov, clust, sigma, tight.n_clusters_)
        loglik, score, *_ = _marginal_terms(gamma, xb, y, prov, clust, t_nodes, log_wt, sigma, center, scale,
                                            tight.n_providers_, tight.n_clusters_)
        return {"gamma": hexes(gamma), "loglik": hexes([loglik]), "score": hexes(score)}

    fixed = np.asarray(start, dtype=float) + 0.05 * (np.arange(len(start)) % 5 - 2)
    limit = np.asarray(start, dtype=float).copy()
    for _ in range(200):  # sigma = 0: each provider's root of sum(y_adj - p) = 0 (X-005)
        p = 1.0 / (1.0 + np.exp(-(limit[prov] + xb)))
        step = np.bincount(prov, weights=y - p, minlength=limit.size) / np.bincount(prov, weights=p * (1 - p), minlength=limit.size)
        limit = limit + step
        if np.max(np.abs(step)) < 1e-15:
            break
    flipped = d.copy()
    flipped.loc[flipped.index[0], "y_adj"] = 1.0 - flipped["y_adj"].iloc[0]
    return {"providers": [str(v) for v in tight.provider_ids_], "clusters": [str(v) for v in tight.cluster_ids_],
            "beta": hexes(beta), "sigma": hexes([sigma]), "start": hexes(start),
            "default": summary(default), "tight": summary(tight),
            "iterates": [hexes(fit(tol=1e-14, max_iter=k).gamma_) for k in (1, 2, 3)],
            "function": {"start": function(np.asarray(start, dtype=float)), "fixed": function(fixed),
                         "tight": function(tight.gamma_)},
            "small_sigma": {"sigma": hexes([1e-4]), "gamma": hexes(fit(tol=1e-10, sig=1e-4, gamma0=limit).gamma_)},
            "zero_sigma_limit": hexes(limit),
            "negative_controls": {"sigma_plus_0.01": hexes(fit(tol=1e-10, sig=sigma + 0.01).gamma_),
                                  "flipped_y_adj": hexes(fit(tol=1e-10, data=flipped).gamma_)}}


def gauss_hermite():
    """Gauss-Hermite rules for weight e^(-t^2): mpmath at 50 digits, and numpy's hermgauss beside them."""
    import mpmath
    mpmath.mp.dps = 50
    out = {}
    for n in (5, 10, 20, 40):
        nodes, weights = mpmath.gauss_quadrature(n, "hermite") if hasattr(mpmath, "gauss_quadrature") else (None, None)
        if nodes is None:
            # Golub-Welsch at 50 digits: eigenvalues of the Jacobi matrix with off-diagonals sqrt(k/2)
            J = mpmath.matrix(n, n)
            for k in range(1, n):
                J[k, k - 1] = J[k - 1, k] = mpmath.sqrt(mpmath.mpf(k) / 2)
            E, Q = mpmath.eigsy(J)
            pairs = sorted((E[i], mpmath.sqrt(mpmath.pi) * Q[0, i] ** 2) for i in range(n))
            nodes, weights = [p[0] for p in pairs], [p[1] for p in pairs]
        t_np, w_np = np.polynomial.hermite.hermgauss(n)
        out[str(n)] = {"nodes": hexes([float(v) for v in nodes]), "weights": hexes([float(v) for v in weights]),
                       "numpy_nodes": hexes(t_np), "numpy_weights": hexes(w_np)}
    return out


def flipped(df):
    _, d = prepared(df)
    rid = int(d.loc[d["included"] == 1].sort_values("rid")["rid"].iloc[0])
    return df.assign(Y=np.where(df["rid"] == rid, 1 - df["Y"], df["Y"]))


def generate(out, inputs, lock, write_json):
    """Writes out/three-stage/<case>/{case.json, input.csv, pprof_py.json, r_threestage.json}."""
    root = os.path.join(out, "three-stage")
    shutil.rmtree(root, ignore_errors=True)
    names = []
    for case in CASES:
        case_dir = os.path.join(root, case["id"])
        os.makedirs(case_dir)
        if inputs:
            shutil.copyfile(os.path.join(inputs, "three-stage", case["id"], "input.csv"), os.path.join(case_dir, "input.csv"))
        elif "source" in case:
            df = pd.read_csv(golden_file(lock, case["source"]))
            if "rid" not in df.columns:
                df.insert(0, "rid", np.arange(1, len(df) + 1))
            df.to_csv(os.path.join(case_dir, "input.csv"), index=False)
        else:
            synthetic(case).to_csv(os.path.join(case_dir, "input.csv"), index=False)
        df = pd.read_csv(os.path.join(case_dir, "input.csv"))
        features = case["features"]
        write_json(os.path.join(case_dir, "case.json"), dict(case, rows=len(df), events=int(df["Y"].sum()), cutoff=CUTOFF))
        write_json(os.path.join(case_dir, "pprof_py.json"), {
            "preparation": preparation(df),
            "stage1": {"default": stage1(df, features), "tight": stage1(df, features, **TIGHT)},
            "negative_controls": {"cutoff_11": preparation(df, cutoff=11),
                                  "flipped_outcome": stage1(flipped(df), features, **TIGHT)}})
        if case.get("stage3"):
            reference = json.load(open(os.path.join(case_dir, "pprof_py.json")))
            reference["stage3"] = stage3_outputs(df, features)
            write_json(os.path.join(case_dir, "pprof_py.json"), reference)
        r = r_golden(lock, case, df)
        r["glm_stage1"] = {"beta": r_stage1(df, features, case_dir)}
        write_json(os.path.join(case_dir, "r_threestage.json"), r)
        names.append(case["id"])
    write_json(os.path.join(root, "gauss-hermite.json"), gauss_hermite())
    return names
