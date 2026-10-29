"""Logistic random-effect reference fixtures (slice 4b-1; ADR-0005, D-57).

The cases of docs/spec/logistic/random-effect-estimation.md §10, each fitted with nAGQ = 1 (`stage2`) and nAGQ = 0.
Inputs are synthetic and exact in text: features and offsets on a 1/64 grid, outcomes 0 or 1 (k/8 in
lgre-fractional). pprof_py's outputs come from LogisticRandomEffectModel at its defaults and with its tolerances
tightened (tol_outer 1e-10, tol_pirls 1e-13); lme4's from glmer at its defaults and with BOBYQA's rhoend at 1e-12
and tolPwrss at 1e-13 (logistic_re_r.R). Both are optimizer-limited, so they are compared at T-opt (D-42, D-51
item 8); the deviance at fixed parameters is not, and is compared at T-fn. generate.py calls
`generate(out, inputs, lock, write_json)`; the output is deterministic.
"""

import hashlib
import os
import shutil
import subprocess
import tempfile
import urllib.request
import warnings

import numpy as np
import pandas as pd
from scipy.special import expit
from pprof_py.models.logistic.random_effect import LogisticRandomEffectModel

HERE = os.path.dirname(os.path.abspath(__file__))
BASE_SIZES = [20 + (37 * j) % 131 for j in range(40)]
CASES = (
    {"id": "lgre-base", "seed": 51, "sizes": BASE_SIZES},
    {"id": "lgre-offset", "seed": 52, "sizes": BASE_SIZES, "offset": True},
    {"id": "lgre-nointercept", "seed": 53, "sizes": BASE_SIZES, "intercept": False},
    {"id": "lgre-provider-level", "seed": 54, "sizes": BASE_SIZES, "provider_level": True},
    {"id": "lgre-extreme", "seed": 55, "sizes": [6, 7, 8, 9] + BASE_SIZES[4:], "extreme": True},
    {"id": "lgre-small-sigma", "seed": 56, "sizes": BASE_SIZES, "sigma": 0.05},
    {"id": "lgre-shifted", "seed": 51, "sizes": BASE_SIZES, "shift": 1000.0},
    {"id": "lgre-text", "seed": 57, "sizes": [15 + (11 * j) % 60 for j in range(30)], "text": True},
    {"id": "lgre-many", "seed": 58, "sizes": [4 + j % 9 for j in range(500)]},
    {"id": "lgre-fractional", "seed": 59, "sizes": BASE_SIZES, "fractional": True},
)
STAGES = (("nagq1", True), ("nagq0", False))
R_PPROF_FILES = ("logis_re.R", "test.logis_re.R", "SM_output.logis_re.R", "confint.logis_re.R", "summary.logis_re.R",
                 "data_check.R")
# R pprof's logis_re takes neither an offset nor a model without an intercept, and glmer fits another model to
# fractional outcomes (D-58): those cases have no R pprof outputs.
R_PPROF_CASES = {"lgre-base", "lgre-provider-level", "lgre-extreme", "lgre-small-sigma", "lgre-shifted", "lgre-text",
                 "lgre-many"}
TIGHT = {"tol_outer": 1e-10, "tol_pirls": 1e-13, "max_iter_outer": 20000, "max_iter_pirls": 200}

OPTIONS = {
    "pprof_py": "LogisticRandomEffectModel(stage2=True and False, verbose=False) at its defaults (stage 1 Powell, as "
                "nlopt is absent; stage 2 Nelder-Mead; tol_outer 1e-7, tol_pirls 1e-8) and tightened (tol_outer 1e-10, "
                "tol_pirls 1e-13, max_iter_outer 20000, max_iter_pirls 200); fit(X, 'y', features, 'provider', "
                "offset_var='o', include_intercept) as the case sets",
    "glmer": "glmer(y ~ features [+ offset(o)] + (1 | provider), family = binomial, nAGQ = 1 and 0), provider levels "
             "sorted in the C locale, at its defaults and with glmerControl(optimizer = 'bobyqa', optCtrl = list(rhoend = "
             "1e-12, maxfun = 1e6), tolPwrss = 1e-13); vcov with use.hessian = FALSE (X-043) and its default, ranef "
             "(condVar = TRUE), logLik, AIC, BIC, devcomp's drsum, ldL2 and ussq, fitted values, predictions and "
             "residuals of the first 100 rows",
    "function_level": "the Laplace deviance at four (sigma, beta) points around pprof_py's tight nAGQ = 1 optimum, each moving sigma and at most one centred coefficient by at most two standard errors "
                      "(pprof_py: PIRLS over u from 0, then _laplace_deviance; lme4: the nAGQ = 1 deviance function of "
                      "c(theta, beta)), with pprof_py's Richardson central differences there; the nAGQ = 0 criterion at "
                      "three sigma values",
    "inference": "inference_pprof_py.json: pprof_py's summary (level 0.9), test (wald at references 0, median, mean, "
                 "0.1, greater and less at the median, critical 2.5, the first five providers; poibin_exact at 0, the "
                 "median and greater; resampling at 0 with n_resample 20000 and seed 7), option alpha, the SM intervals "
                 "(indirect and direct ratios and rates at the median for each alternative, and two-sided at the "
                 "size-weighted mean given as a number, X-046), the measures at the median, the mean and 0, and "
                 "profile_sigma, all on its tightened nAGQ = 1 fit; with negative controls from the flipped-provider fit",
    "r_pprof": "R pprof 1.0.3's logis_re (glmer with the tightened control passed through), test.logis_re, "
               "confint.logis_re (option alpha; option SM for each alternative) and SM_output.logis_re, with "
               "computeDirectExp written in R; and lme4's profile of the tightened glmer fit at zeta steps of 0.02, "
               "with confint",
    "negative_controls": "pprof_py's tight nAGQ = 1 fit with every outcome of the largest provider replaced by 1 - y "
                         "(flipped_provider) and with the first row of every provider so replaced (flipped_first_rows)",
}


def hexes(values):
    return [float(v).hex() for v in np.ravel(np.asarray(values, dtype=float))]


def packed(matrix):
    m = np.asarray(matrix, dtype=float)
    return [m[i, j] for i in range(m.shape[0]) for j in range(i, m.shape[0])]


def label(value):
    return value.item() if hasattr(value, "item") else value


def features(case):
    return ["x1", "x2"] + (["z"] if case.get("provider_level") else [])


def fit_arguments(case):
    out = {"include_intercept": case.get("intercept", True)}
    if case.get("offset"):
        out["offset_var"] = "o"
    return out


def make_data(case):
    """Providers 1..m (or "prov-1".. "prov-m"); within each, rows in generation order."""
    rng = np.random.Generator(np.random.PCG64(case["seed"]))
    sigma = case.get("sigma", 0.6)
    parts = []
    for j, size in enumerate(case["sizes"], start=1):
        a = sigma * rng.standard_normal()
        z = np.rint(rng.standard_normal() * 64) / 64
        x1 = np.rint(rng.standard_normal(size) * 64) / 64
        x2 = (rng.random(size) < 0.3).astype(float)
        o = np.rint(0.3 * rng.standard_normal(size) * 64) / 64 if case.get("offset") else np.zeros(size)
        eta = (-1.0 if case.get("intercept", True) else 0.0) + 0.7 * x1 - 0.4 * x2 + a + o
        if case.get("provider_level"):
            eta = eta + 0.5 * z
        p = expit(eta)
        y = rng.binomial(8, p) / 8.0 if case.get("fractional") else (rng.random(size) < p).astype(float)
        if case.get("extreme") and j <= 4:  # providers whose outcomes are all 1 or all 0
            y = np.full(size, 1.0 if j <= 2 else 0.0)
        part = {"provider": f"prov-{j}" if case.get("text") else j, "y": y, "x1": x1, "x2": x2}
        if case.get("provider_level"):
            part["z"] = np.full(size, z)
        if case.get("offset"):
            part["o"] = o
        parts.append(pd.DataFrame(part))
    df = pd.concat(parts, ignore_index=True)
    if "shift" in case:
        df["x1"] = df["x1"] + case["shift"]
    df.insert(0, "id", np.arange(len(df)))
    return df


def fit(df, case, stage2, tight):
    model = LogisticRandomEffectModel(stage2=stage2, verbose=False, **(TIGHT if tight else {}))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        model.fit(df, "y", features(case), "provider", **fit_arguments(case))
    return model, sorted({str(w.message)[:200] for w in caught})


def record(model, df, case, caught):
    head = df.head(100)
    kwargs = {"x_vars": features(case), **({"offset_var": "o"} if case.get("offset") else {})}
    posterior = model._get_posterior_var("provider")
    return {
        "providers": [label(v) for v in model.coefficients_["alpha"]["provider"].index],
        "sigma": hexes([model.sigma_["provider"]]),
        "beta": hexes(model.coefficients_["beta"]), "var_beta": hexes(packed(model.variances_["beta"])),
        "blups": hexes(model.coefficients_["alpha"]["provider"]), "condvar": hexes(posterior),
        "deviance": hexes([-2.0 * model.loglike_]), "loglik": hexes([model.loglike_]),
        "aic": hexes([model.aic_]), "bic": hexes([model.bic_]),
        "pwrss": hexes([model.pwrss_]), "ldL2": hexes([model.ldL2_]), "ussq": hexes([model.ussq_]),
        "fitted": hexes(model.fitted_[:100]),
        "predict_fixed": hexes(model.predict(head, **kwargs)),
        "predict_re": hexes(model.predict(head, use_re=True, **kwargs)),
        "predict_link_re": hexes(model.predict(head, use_re=True, type="link", **kwargs)),
        "pearson": hexes(model.pearson_residuals()[:100]), "deviance_residuals": hexes(model.deviance_residuals()[:100]),
        "converged": bool(model.converged_), "warnings": caught,
    }


def richardson(f, x, k, h):
    def at(t):
        y = np.array(x, dtype=float)
        y[k] += t
        return f(y)
    d1 = (at(h) - at(-h)) / (2 * h)
    d2 = (at(2 * h) - at(-2 * h)) / (4 * h)
    return (4 * d1 - d2) / 3


def function_level(model, sigma_star, beta_star, sigma0_star, centre, intercept):
    """The Laplace deviance at four points around the tight optimum, its Richardson slopes at two of them, and the
    nAGQ = 0 criterion at three sigma values (§10). Each point moves sigma and at most one coefficient, by at most two
    standard errors, in the centred coordinates: a slope's move is offset in the intercept so that the linear
    predictor's mean stays put (round 112; before, every coefficient moved at once, which on lgre-shifted pushed the
    linear predictor to extremes)."""
    p = len(beta_star)
    zeros = np.zeros(model._u.size if hasattr(model, "_u") else 0)

    def laplace(par):
        sigma, beta = np.array([max(par[0], 0.0)]), np.asarray(par[1:], dtype=float)
        state = model._pirls(sigma=sigma, beta0=beta, u0=zeros, update_beta=False)
        return model._laplace_deviance(sigma, beta, state.u)

    def nagq0(s):
        sigma = np.array([s])
        state = model._pirls(sigma=sigma, beta0=np.asarray(beta_star, dtype=float), u0=zeros, update_beta=True)
        return model._laplace_deviance(sigma, state.beta, state.u)

    se = np.sqrt(np.diag(model.variances_["beta"].to_numpy()))

    def move(k, delta):
        beta = np.array(beta_star, dtype=float)
        beta[k] += delta
        if intercept and k > 0:
            beta[0] -= centre[k] * delta
        return beta

    first_slope = 1 if intercept and p > 1 else 0
    points = [np.r_[sigma_star, beta_star], np.r_[1.5 * sigma_star, beta_star],
              np.r_[0.5 * sigma_star, move(first_slope, 2.0 * se[first_slope])],
              np.r_[sigma_star, move(p - 1, -se[p - 1])]]
    slopes = []
    for point in points[1:3]:
        slopes.extend(richardson(laplace, point, k, 1e-4 * max(abs(point[k]), 1e-2)) for k in range(p + 1))
    sigmas = [0.5 * sigma0_star, sigma0_star, 2.0 * sigma0_star]
    return {"points": hexes(np.concatenate(points)), "deviance": hexes([laplace(x) for x in points]),
            "slope_points": hexes(np.concatenate(points[1:3])), "slope": hexes(slopes),
            "sigma0": hexes(sigmas), "deviance0": hexes([nagq0(s) for s in sigmas])}


def negative_controls(df, case):
    """Defects T-opt must catch: the largest provider's outcomes all replaced by 1 - y, and the first row of every
    provider so replaced. A single row moves an optimizer-limited fit too little for T-opt (round 110)."""
    sizes = df.groupby("provider", sort=False).size().to_numpy()
    key = df["provider"].drop_duplicates().iloc[int(np.argmax(sizes))]
    provider = df.copy()
    rows = df["provider"].to_numpy() == key
    provider.loc[rows, "y"] = 1.0 - provider.loc[rows, "y"]
    first = df.copy()
    starts = ~df["provider"].duplicated().to_numpy()
    first.loc[starts, "y"] = 1.0 - first.loc[starts, "y"]
    out = {}
    for name, data in (("flipped_provider", provider), ("flipped_first_rows", first)):
        model, _ = fit(data, case, True, True)
        out[name] = {"sigma": hexes([model.sigma_["provider"]]), "beta": hexes(model.coefficients_["beta"]),
                     "blups": hexes(model.coefficients_["alpha"]["provider"]), "deviance": hexes([-2.0 * model.loglike_])}
    return out


def r_pprof_sources(lock, directory):
    """R pprof's logistic random-effect functions at the pinned commit, checked against REFERENCE.lock."""
    pin = lock["r_pprof"]
    os.makedirs(directory, exist_ok=True)
    for name in R_PPROF_FILES:
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


def flags(values):
    return hexes([np.nan if v is None or (isinstance(v, float) and np.isnan(v)) or pd.isna(v) else float(v)
                  for v in values])


def table(frame, columns):
    return {c: (flags(frame[c]) if c == "flag" else hexes(frame[c].astype(float))) for c in columns}


TEST_COLUMNS = ("estimate", "se", "null_value", "z_raw", "p_value", "flag", "ci_lower", "ci_upper")


def inference(model, df, case):
    """pprof_py's inference on its tightened nAGQ = 1 fit (spec 4b-2 §10)."""
    out = {"estimates": {"sigma": hexes([model.sigma_["provider"]]), "beta": hexes(model.coefficients_["beta"])}}
    s = model.summary(level=0.9)
    out["summary"] = {"estimate": hexes(s["Estimate"]), "se": hexes(s["Std.Error"]), "z": hexes(s["z value"]),
                      "p": hexes(s["Pr(>|z|)"]), "lower": hexes(s["ci_lower"]), "upper": hexes(s["ci_upper"])}
    labels = [label(v) for v in model.coefficients_["alpha"]["provider"].index]
    variants = (("wald_zero", {}), ("wald_median", {"reference": "median"}), ("wald_mean", {"reference": "mean"}),
                ("wald_given", {"reference": 0.1}),
                ("wald_greater", {"reference": "median", "alternative": "greater"}),
                ("wald_less", {"reference": "median", "alternative": "less"}),
                ("wald_critical", {"critical": 2.5}), ("wald_subset", {"providers": labels[:5]}),
                ("poibin_zero", {"test_method": "poibin_exact"}),
                ("poibin_median", {"test_method": "poibin_exact", "reference": "median"}),
                ("poibin_greater", {"test_method": "poibin_exact", "reference": "median", "alternative": "greater"}),
                ("resampling_zero", {"test_method": "resampling", "n_resample": 20000, "seed": 7}))
    out["test"] = {}
    for name, kwargs in variants:
        t = model.test(**kwargs)
        out["test"][name] = dict(table(t, TEST_COLUMNS), providers=[label(v) for v in t.index], options={
            k: (v if not isinstance(v, list) else v) for k, v in kwargs.items()})
    a = model.calculate_confidence_intervals(option="alpha")["alpha_ci"]
    out["alpha_ci"] = {"alpha": hexes(a["alpha"]), "lower": hexes(a["alpha_lower"]), "upper": hexes(a["alpha_upper"])}
    idx = np.asarray(model._group_indices[0]).ravel()
    blups = model.coefficients_["alpha"]["provider"].to_numpy()
    weighted = float(np.average(blups, weights=np.bincount(idx)))
    out["weighted_mean"] = hexes([weighted])
    for name, kwargs in (("two_sided", {}), ("greater", {"alternative": "greater"}), ("less", {"alternative": "less"}),
                         ("mean_weighted", {"reference": weighted})):
        ci = model.calculate_confidence_intervals(option="SM", stdz=["indirect", "direct"], measure=["ratio", "rate"],
                                                  **kwargs)
        out[f"sm_{name}"] = {key: {"value": hexes(ci[key][key]), "lower": hexes(ci[key]["lower"]),
                                   "upper": hexes(ci[key]["upper"])} for key in
                             ("indirect_ratio", "indirect_rate", "direct_ratio", "direct_rate")}
    for name, reference in (("median", "median"), ("mean", "mean"), ("zero", 0.0)):
        me = model.calculate_standardized_measures(stdz=["indirect", "direct"], reference=reference)
        out[f"measures_{name}"] = {kind: {c: hexes(me[kind][c]) for c in me[kind].columns if c != "provider_id"}
                                   for kind in ("indirect", "direct")}
    pr = model.profile_sigma(level=0.95)
    out["profile_sigma"] = {"sigma": hexes(pr["sigma"]), "lower": hexes(pr["lower"]), "upper": hexes(pr["upper"])}
    return out


def inference_controls(df, case):
    """The inference of pprof_py's tight fit with the largest provider's outcomes flipped: a defect T-test and T-meas
    must catch."""
    sizes = df.groupby("provider", sort=False).size().to_numpy()
    key = df["provider"].drop_duplicates().iloc[int(np.argmax(sizes))]
    flipped = df.copy()
    rows = df["provider"].to_numpy() == key
    flipped.loc[rows, "y"] = 1.0 - flipped.loc[rows, "y"]
    model, _ = fit(flipped, case, True, True)
    me = model.calculate_standardized_measures(stdz=["indirect"], reference="median")
    return {"flipped_provider": {"wald_zero_z": hexes(model.test()["z_raw"]),
                                 "indirect_ratio": hexes(me["indirect"]["indirect_ratio"])}}


def r_outputs(case_dir, case, function, rpprof=None):
    args = {"features": ",".join(features(case)), "intercept": "1" if case.get("intercept", True) else "0",
            "offset": "1" if case.get("offset") else "0", "points": ",".join(function["points"]),
            "sigma0": ",".join(function["sigma0"]),
            "rpprof": rpprof if (rpprof and case["id"] in R_PPROF_CASES) else ""}
    path_args = os.path.join(case_dir, "r_args.txt")
    with open(path_args, "w", encoding="utf-8") as handle:
        handle.write("".join(f"{k}\t{v}\n" for k, v in args.items()))
    subprocess.run(["Rscript", os.path.join(HERE, "logistic_re_r.R"), case_dir], check=True)
    os.remove(path_args)
    path = os.path.join(case_dir, "r_lme4.txt")
    result = {}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            key, values = line.rstrip("\n").split("\t")
            stage, kind, quantity = key.split(".")
            result.setdefault(stage, {}).setdefault(kind, {})[quantity] = hexes(
                [float.fromhex(v) for v in values.split(",")] if values else [])
    os.remove(path)
    return result


def generate(out, inputs, lock, write_json):
    """Writes out/logistic-re/<case>/{case.json, input.csv, pprof_py.json, r_lme4.json, inference_pprof_py.json}."""
    root = os.path.join(out, "logistic-re")
    shutil.rmtree(root, ignore_errors=True)
    rpprof = r_pprof_sources(lock, tempfile.mkdtemp(prefix="r_pprof_logistic_re_"))
    names = []
    for case in CASES:
        case_dir = os.path.join(root, case["id"])
        os.makedirs(case_dir)
        if inputs:
            shutil.copyfile(os.path.join(inputs, "logistic-re", case["id"], "input.csv"), os.path.join(case_dir, "input.csv"))
            df = pd.read_csv(os.path.join(case_dir, "input.csv"))
        else:
            df = make_data(case)
            df.to_csv(os.path.join(case_dir, "input.csv"), index=False)
        reference = {}
        tight_models = {}
        for name, stage2 in STAGES:
            default, caught = fit(df, case, stage2, False)
            tight, caught_tight = fit(df, case, stage2, True)
            tight_models[name] = tight
            reference[name] = {"default": record(default, df, case, caught), "tight": record(tight, df, case, caught_tight)}
        t1 = tight_models["nagq1"]
        intercept = case.get("intercept", True)
        centre = ([0.0] if intercept else []) + [float(df[f].mean()) for f in features(case)]
        function = function_level(t1, t1.sigma_["provider"], t1.coefficients_["beta"].to_numpy(),
                                  tight_models["nagq0"].sigma_["provider"], centre, intercept)
        reference["function"] = function
        reference["negative_controls"] = negative_controls(df, case)
        inferred = inference(t1, df, case)
        inferred["negative_controls"] = inference_controls(df, case)
        write_json(os.path.join(case_dir, "inference_pprof_py.json"), inferred)
        write_json(os.path.join(case_dir, "case.json"), dict(case, features=features(case), rows=len(df),
                                                            providers=len(case["sizes"]), text=bool(case.get("text"))))
        write_json(os.path.join(case_dir, "pprof_py.json"), reference)
        write_json(os.path.join(case_dir, "r_lme4.json"), r_outputs(case_dir, case, function, rpprof))
        names.append(case["id"])
        print("logistic-re", case["id"], "done", flush=True)
    return names
