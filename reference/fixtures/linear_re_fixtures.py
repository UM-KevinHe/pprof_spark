"""Linear random-effect reference fixtures (slice 4a-1; ADR-0005, D-52).

The cases of docs/spec/linear/random-effect-estimation.md §11, each fitted under REML and ML. Inputs are synthetic
and exact in text: features on a 1/64 grid (x3 is 0 or 1, scaled by 1/1024 in lre-shifted), outcomes on a 1/1024
grid, weights, offsets and the provider-level feature z on a 1/64 grid. pprof_py's outputs come from
LinearRandomEffectModel at its defaults and at its tight optimum; lme4's from lmer at its defaults and from its
deviance function at its tight optimum (linear_re_r.R). A tight optimum is the root in tau = theta^2 of the
Richardson-extrapolated central differences of the reference's own deviance function, or 0 where that deviance
rises from 0. generate.py calls `generate(out, inputs, lock, write_json)`; the output is deterministic.
"""

import hashlib
import os
import shutil
import subprocess
import tempfile
import urllib.request
import warnings
from types import SimpleNamespace

import numpy as np
import pandas as pd
from scipy.optimize import brentq
from pprof_py.models.linear.random_effect import LinearRandomEffectModel

HERE = os.path.dirname(os.path.abspath(__file__))
BASE_SIZES = [1, 2] + [3 + (17 * j) % 117 for j in range(38)]
CASES = (
    {"id": "lre-base", "seed": 41, "sizes": BASE_SIZES},
    {"id": "lre-weights", "seed": 42, "sizes": BASE_SIZES, "weights": True},
    {"id": "lre-offset", "seed": 43, "sizes": BASE_SIZES, "offset": True},
    {"id": "lre-nointercept", "seed": 44, "sizes": BASE_SIZES, "intercept": False},
    {"id": "lre-provider-level", "seed": 45, "sizes": BASE_SIZES, "provider_level": True},
    {"id": "lre-boundary", "seed": 46, "sizes": BASE_SIZES, "boundary": True},
    {"id": "lre-shifted", "seed": 41, "sizes": BASE_SIZES, "shift": [10000.0, -10000.0, 0.0], "scale_x3": 1.0 / 1024},
    {"id": "lre-text", "seed": 47, "sizes": [5 + (7 * j) % 40 for j in range(30)], "text": True},
    {"id": "lre-many", "seed": 48, "sizes": [3 + j % 6 for j in range(2000)]},
)
CRITERIA = (("reml", True), ("ml", False))
THETAS = (0.0, 0.1, 0.3, 1.0, 3.0)
INFERENCE_ML = {"lre-base", "lre-weights"}  # slice 4a-2: every case under REML, these also under ML
TEST_COLUMNS = ("estimate", "se", "null_value", "z_raw", "p_value", "flag", "ci_lower", "ci_upper")
R_PPROF_RE_FILES = ("linear_re.R", "test.linear_re.R", "confint.linear_re.R")
TAU_SCALED = (0.01, 0.1, 1.0, 10.0)  # tau times the median provider weight total

OPTIONS = {
    "pprof_py": "LinearRandomEffectModel(reml=True and False, verbose=False) at its defaults (Powell, tol_outer 1e-8); "
                "fit(X, 'y', features, 'provider', offset_var='o', weights_var='w', include_intercept) as the case sets",
    "tight": "the root in tau = theta^2 of (4 D(h) - D(2h)) / 3, D(h) the central difference of the reference's deviance "
             "with h = 3e-3 tau (scipy brentq; R uniroot, tol 1e-14 tau), or 0 where the deviance rises from 0; pprof_py "
             "evaluated there by fit with _optimize returning that theta; lme4 by mkMerMod at it (lFormula, mkLmerDevfun)",
    "function_level": "the deviance at theta = 0, 0.1, 0.3, 1, 3 (pprof_py's _objective_from_state; lme4's deviance "
                      "function) and its central-difference slope in tau at tau = (0.01, 0.1, 1, 10) / W~, W~ the median "
                      "weight total; NumPy's summaries of spec 4a-1 §2.1 (within solution by lstsq)",
    "lmer": "lmer(y ~ features [+ offset(o)] + (1 | provider), weights = w, REML), provider levels sorted in the C locale; "
            "fixef, vcov, sigma, theta, ranef(condVar = TRUE), REMLcrit or deviance, logLik, AIC, BIC, devcomp's pwrss, "
            "ldL2 and ussq, fitted values and predictions (re.form = NA and with the random effects) of the first 100 rows",
    "negative_controls": "pprof_py's tight REML fit with y + 1 in the first row of the largest provider (nudged_outcome) "
                         "and with that provider's last row removed (dropped_row)",
    "inference": "slice 4a-2 (D-54) at the tight optima, every case under REML and lre-base and lre-weights under ML: "
                 "pprof_py's summary (level 0.9; two_sided, greater, less), fixed_effects_table, conf_int(0.95), test "
                 "(reference 0, median, mean, 0.1; greater and less at the median; critical 2.5; the first five "
                 "providers), calculate_confidence_intervals (alpha; SM indirect and direct at the median, two_sided "
                 "and greater) and calculate_standardized_measures (median, mean, 0); base R's t and normal tails and "
                 "quantiles on lme4's tight fit (summary with n - p - m degrees of freedom, fixed_effects_table and "
                 "conf_int with n - p, tests at 0 and the median from ranef(condVar = TRUE)); R pprof 1.0.3's "
                 "test.linear_re and confint.linear_re(option = 'alpha') on its own lmer fit for unweighted cases "
                 "for the cases without weights, offsets or a removed intercept (X-042); the negative controls' z at reference 0",
}


def hexes(values):
    return [float(v).hex() for v in np.ravel(np.asarray(values, dtype=float))]


def packed(matrix):
    m = np.asarray(matrix, dtype=float)
    return [m[i, j] for i in range(m.shape[0]) for j in range(i, m.shape[0])]


def label(value):
    return value.item() if hasattr(value, "item") else value


def features(case):
    return ["x1", "x2", "x3"] + (["z"] if case.get("provider_level") else [])


def fit_arguments(case):
    out = {"include_intercept": case.get("intercept", True)}
    if case.get("offset"):
        out["offset_var"] = "o"
    if case.get("weights"):
        out["weights_var"] = "w"
    return out


def make_data(case):
    """Providers 1..m (or "prov-1".. "prov-m"); within each, rows in generation order."""
    rng = np.random.Generator(np.random.PCG64(case["seed"]))
    parts = []
    for j, size in enumerate(case["sizes"], start=1):
        g = 0.0 if case.get("boundary") else 0.5 * rng.standard_normal()
        z = np.rint(rng.standard_normal() * 64) / 64
        x1 = np.rint(rng.standard_normal(size) * 64) / 64
        x2 = np.rint(rng.standard_normal(size) * 64) / 64
        x3 = (rng.random(size) < 0.4).astype(float)
        w = (32 + rng.integers(0, 97, size)) / 64 if case.get("weights") else np.ones(size)
        o = np.rint(0.3 * rng.standard_normal(size) * 64) / 64 if case.get("offset") else np.zeros(size)
        e = rng.standard_normal(size) / np.sqrt(w)
        if case.get("boundary"):  # no between-provider variation in the errors: the optimum lies at theta = 0
            e = e - np.average(e, weights=w)
        mean = (1.0 if case.get("intercept", True) else 0.0) + g + 0.8 * x1 - 0.5 * x2 + 0.3 * x3 + o
        if case.get("provider_level"):
            mean = mean + 0.6 * z
        part = {"provider": f"prov-{j}" if case.get("text") else j, "y": np.rint((mean + e) * 1024) / 1024,
                "x1": x1, "x2": x2, "x3": x3}
        if case.get("provider_level"):
            part["z"] = np.full(size, z)
        if case.get("weights"):
            part["w"] = w
        if case.get("offset"):
            part["o"] = o
        parts.append(pd.DataFrame(part))
    df = pd.concat(parts, ignore_index=True)
    for name, shift in zip(("x1", "x2", "x3"), case.get("shift", (0.0, 0.0, 0.0))):
        df[name] = df[name] + shift
    if "scale_x3" in case:
        df["x3"] = df["x3"] * case["scale_x3"]
    df.insert(0, "id", np.arange(len(df)))
    return df


def fit(df, case, reml, theta=None):
    model = LinearRandomEffectModel(reml=reml, verbose=False)
    if theta is not None:  # evaluate pprof_py's fit at a given theta
        model._optimize = lambda fun, x0: (np.array([theta]), SimpleNamespace(success=True, nfev=0, nit=0, x=np.array([theta])))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        model.fit(df, "y", features(case), "provider", **fit_arguments(case))
    return model, [str(w.message) for w in caught]


def record(model, df, case, caught):
    head = df.head(100)
    kwargs = {"x_vars": features(case), **({"offset_var": "o"} if case.get("offset") else {})}
    return {
        "providers": [label(v) for v in model.provider_ids_], "sizes": [int(v) for v in model.provider_sizes_],
        "theta": hexes(model.theta_), "sigma": hexes([model.sigma_]),
        "sigma_alpha": hexes([model.random_effect_sd_["provider"]]),
        "beta": hexes(model.coefficients_["beta"]), "var_beta": hexes(packed(model.variances_["beta"])),
        "blups": hexes(model.coefficients_["alpha"]),
        "deviance": hexes([-2.0 * model.loglike_]), "loglik": hexes([model.loglike_]),
        "aic": hexes([model.aic_]), "bic": hexes([model.bic_]),
        "pwrss": hexes([model.pwrss_]), "ldL2": hexes([model.ldL2_]), "ussq": hexes([model.ussq_]),
        "fitted": hexes(model.fitted_[:100]),
        "predict_fixed": hexes(model.predict(head, **kwargs)),
        "predict_re": hexes(model.predict(head, use_re=True, **kwargs)),
        "converged": bool(model.converged_), "evaluations": int(model.outer_iterations_), "warnings": caught,
    }


def slope(dev, tau):
    h = 3e-3 * tau
    d1 = (dev(tau + h) - dev(tau - h)) / (2 * h)
    d2 = (dev(tau + 2 * h) - dev(tau - 2 * h)) / (4 * h)
    return (4 * d1 - d2) / 3


def tight_tau(dev, tau0, w_tilde):
    """The root in tau of the deviance's Richardson slope, bracketed around the default optimum tau0; 0 where the
    deviance rises from 0."""
    h0 = 1e-6 / w_tilde
    if dev(h0) > dev(0.0) and dev(0.0) <= dev(max(tau0, h0)):
        return 0.0
    lo, hi = tau0 * (1 - 1e-3), tau0 * (1 + 1e-3)
    for _ in range(40):
        if slope(dev, lo) < 0 < slope(dev, hi):
            return brentq(lambda t: slope(dev, t), lo, hi, xtol=1e-14 * tau0, rtol=1e-15, maxiter=500)
        lo, hi = lo / 2, hi * 2
    raise RuntimeError("no bracket for the tight optimum")


def summaries(df, case):
    """Spec 4a-1 §2.1 by NumPy: per-provider counts, weight totals and weighted means; pooled weighted within
    co-moments, the within residual sum of squares (lstsq solution), X'WX, sum of log w, the median weight total."""
    X = df[features(case)].to_numpy(dtype=float)
    if case.get("intercept", True):
        X = np.column_stack([np.ones(len(df)), X])
    y = df["y"].to_numpy(dtype=float) - (df["o"].to_numpy(dtype=float) if case.get("offset") else 0.0)
    w = df["w"].to_numpy(dtype=float) if case.get("weights") else np.ones(len(df))
    cat = pd.Categorical(df["provider"])
    idx, m = cat.codes.astype(np.int64), len(cat.categories)
    W = np.bincount(idx, weights=w, minlength=m)
    xbar = np.column_stack([np.bincount(idx, weights=w * X[:, k], minlength=m) for k in range(X.shape[1])]) / W[:, None]
    ybar = np.bincount(idx, weights=w * y, minlength=m) / W
    Xc, yc = X - xbar[idx], y - ybar[idx]
    sw = np.sqrt(w)
    beta_w = np.linalg.lstsq(sw[:, None] * Xc, sw * yc, rcond=None)[0]
    return {"providers": [label(v) for v in cat.categories], "n_j": [int(v) for v in np.bincount(idx, minlength=m)],
            "W_j": hexes(W), "x_means": hexes(xbar), "y_means": hexes(ybar),
            "within_xx": hexes(packed(Xc.T @ (w[:, None] * Xc))), "within_xy": hexes(Xc.T @ (w * yc)),
            "rss_within": hexes([np.sum(w * (yc - Xc @ beta_w) ** 2)]), "xtwx": hexes(packed(X.T @ (w[:, None] * X))),
            "sum_log_w": hexes([np.sum(np.log(w))]), "w_tilde": hexes([np.median(W)])}


def pprof_criterion(df, case, reml, w_tilde, taus):
    model, caught = fit(df, case, reml)
    default = record(model, df, case, caught)
    b0, u0 = model._beta.copy(), model._u.copy()

    def dev_theta(theta):
        t = np.array([theta])
        return model._objective_from_state(t, model._solve_inner(t, b0, u0), reml)

    dev = lambda tau: dev_theta(np.sqrt(tau))  # noqa: E731
    function = {"theta": hexes(THETAS), "deviance": hexes([dev_theta(t) for t in THETAS]),
                "tau": hexes(taus), "slope": hexes([slope(dev, t) for t in taus])}
    tau = tight_tau(dev, float(model.theta_[0]) ** 2, w_tilde)
    tight_model, caught = fit(df, case, reml, theta=np.sqrt(tau))
    tight = dict(record(tight_model, df, case, caught), tau=hexes([tau]))
    return {"default": default, "tight": tight, "function": function}, tau, tight_model


def floats(values):
    return hexes(pd.to_numeric(pd.Series(values), errors="coerce").astype(float))


def inference(model):
    """Slice 4a-2's outputs of pprof_py at a fitted model (spec 4a-2 §10)."""
    out = {}
    for alternative in ("two_sided", "greater", "less"):
        s = model.summary(level=0.9, alternative=alternative)
        out[f"summary_{alternative}"] = {k: floats(s[k]) for k in ("estimate", "std_error", "stat", "p_value",
                                                                     "ci_lower", "ci_upper")}
    table = model.fixed_effects_table()
    out["fixed_effects_table"] = {"t": floats(table["t value"]), "df": floats(table["df"]), "p": floats(table["Pr(>|t|)"])}
    bounds = model.conf_int(0.95)
    out["conf_int"] = {"lower": floats(bounds["lower"]), "upper": floats(bounds["upper"])}
    subset = [label(v) for v in model.provider_ids_[:5]]
    tests = {}
    for name, kwargs in (("zero", {}), ("median", {"reference": "median"}), ("mean", {"reference": "mean"}),
                         ("given", {"reference": 0.1}), ("greater", {"reference": "median", "alternative": "greater"}),
                         ("less", {"reference": "median", "alternative": "less"}), ("critical", {"critical": 2.5}),
                         ("subset", {"providers": subset})):
        t = model.test(**kwargs)
        tests[name] = {"providers": [label(v) for v in t.index], **{k: floats(t[k]) for k in TEST_COLUMNS}}
    out["test"] = tests
    alpha = model.calculate_confidence_intervals(option="alpha")["alpha_ci"]
    out["alpha_ci"] = {k: floats(alpha[k]) for k in ("alpha", "alpha_lower", "alpha_upper")}
    for alternative in ("two_sided", "greater"):
        ci = model.calculate_confidence_intervals(option="SM", stdz=["indirect", "direct"], reference="median",
                                                  alternative=alternative)
        out[f"sm_ci_{alternative}"] = {kind: {k: floats(ci[f"{kind}_ci"][k]) for k in
                                              (f"{kind}_difference", "observed", "expected", "lower", "upper")}
                                       for kind in ("indirect", "direct")}
    for name, reference in (("median", "median"), ("mean", "mean"), ("zero", 0.0)):
        sm = model.calculate_standardized_measures(stdz=["indirect", "direct"], reference=reference)
        out[f"measures_{name}"] = {kind: {k: floats(sm[kind][k]) for k in (f"{kind}_difference", "observed", "expected")}
                                   for kind in ("indirect", "direct")}
    return out


def r_pprof_re_sources(lock, directory):
    """R pprof's linear random-effect functions at the pinned commit, checked against REFERENCE.lock."""
    pin = lock["r_pprof"]
    os.makedirs(directory, exist_ok=True)
    for name in R_PPROF_RE_FILES:
        path = os.path.join(directory, name)
        if not os.path.exists(path):
            url = f"https://raw.githubusercontent.com/cran/pprof/{pin['commit']}/R/{name}"
            with urllib.request.urlopen(url, timeout=60) as response, open(path, "wb") as handle:
                handle.write(response.read())
        with open(path, "rb") as handle:
            digest = hashlib.sha256(handle.read()).hexdigest()
        if digest != pin["sha256"][name]:
            raise SystemExit(f"{name}: sha256 {digest} differs from REFERENCE.lock")
    return directory


def negative_controls(df, case, w_tilde):
    largest = int(np.argmax(df.groupby("provider", sort=False).size().to_numpy()))
    key = df["provider"].drop_duplicates().iloc[largest]
    rows = np.flatnonzero(df["provider"].to_numpy() == key)
    nudged = df.copy()
    nudged.loc[rows[0], "y"] = nudged.loc[rows[0], "y"] + 1.0
    dropped = df.drop(index=rows[-1]).reset_index(drop=True)
    out = {}
    for name, data in (("nudged_outcome", nudged), ("dropped_row", dropped)):
        result, _, model = pprof_criterion(data, case, True, w_tilde, ())
        tight = result["tight"]
        out[name] = {q: tight[q] for q in ("theta", "sigma", "beta", "blups", "deviance")}
        out[name]["test_z"] = floats(model.test()["z_raw"])
    return out


def r_outputs(case_dir, case, w_tilde, taus, rpprof):
    # R pprof's linear_re parses its formula into columns and has no weights or offsets (X-042).
    plain = not (case.get("weights") or case.get("offset")) and case.get("intercept", True)
    args = {"rpprof": rpprof if plain else "", "features": ",".join(features(case)), "intercept": "1" if case.get("intercept", True) else "0",
            "weights": "1" if case.get("weights") else "0", "offset": "1" if case.get("offset") else "0",
            "thetas": ",".join(float(t).hex() for t in THETAS), "taus": ",".join(float(t).hex() for t in taus),
            "wtilde": float(w_tilde).hex()}
    path_args = os.path.join(case_dir, "r_args.txt")
    with open(path_args, "w", encoding="utf-8") as handle:
        handle.write("".join(f"{k}\t{v}\n" for k, v in args.items()))
    subprocess.run(["Rscript", os.path.join(HERE, "linear_re_r.R"), case_dir], check=True)
    os.remove(path_args)
    path = os.path.join(case_dir, "r_lme4.txt")
    result = {}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            key, values = line.rstrip("\n").split("\t")
            crit, kind, quantity = key.split(".")
            result.setdefault(crit, {}).setdefault(kind, {})[quantity] = hexes([float.fromhex(v) for v in values.split(",")])
    os.remove(path)
    return result


def generate(out, inputs, lock, write_json):
    """Writes out/linear-re/<case>/{case.json, input.csv, pprof_py.json, inference_pprof_py.json, r_lme4.json}."""
    root = os.path.join(out, "linear-re")
    shutil.rmtree(root, ignore_errors=True)
    rpprof = r_pprof_re_sources(lock, tempfile.mkdtemp(prefix="r_pprof_linear_re_"))
    names = []
    for case in CASES:
        case_dir = os.path.join(root, case["id"])
        os.makedirs(case_dir)
        if inputs:
            shutil.copyfile(os.path.join(inputs, "linear-re", case["id"], "input.csv"), os.path.join(case_dir, "input.csv"))
            df = pd.read_csv(os.path.join(case_dir, "input.csv"))
        else:
            df = make_data(case)
            df.to_csv(os.path.join(case_dir, "input.csv"), index=False)
        sums = summaries(df, case)
        w_tilde = float.fromhex(sums["w_tilde"][0])
        taus = [s / w_tilde for s in TAU_SCALED]
        reference = {"summaries": sums}
        inferred = {}
        for name, reml in CRITERIA:
            reference[name], _, model = pprof_criterion(df, case, reml, w_tilde, taus)
            if name == "reml" or case["id"] in INFERENCE_ML:
                inferred[name] = inference(model)
        controls = negative_controls(df, case, w_tilde)
        inferred["negative_controls"] = {k: {"test_z": v.pop("test_z")} for k, v in controls.items()}
        reference["negative_controls"] = controls
        write_json(os.path.join(case_dir, "case.json"), dict(case, features=features(case), rows=len(df),
                                                            providers=len(sums["providers"]), text=bool(case.get("text"))))
        write_json(os.path.join(case_dir, "pprof_py.json"), reference)
        write_json(os.path.join(case_dir, "inference_pprof_py.json"), inferred)
        write_json(os.path.join(case_dir, "r_lme4.json"), r_outputs(case_dir, case, w_tilde, taus, rpprof))
        names.append(case["id"])
    return names
