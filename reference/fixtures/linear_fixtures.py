"""Linear fixed-effect reference fixtures (Phase 3a; ADR-0005, D-45).

The cases of docs/spec/linear/fixed-effect-estimation.md §11. Inputs are synthetic and exact in text:
features on a 1/64 grid (x3 is 0 or 1, scaled by 1/1024 in lin-shifted), outcomes on a 1/1024 grid.
pprof_py's outputs come from LinearFixedEffectModel with both variance options; R's from
stats::lm(y ~ 0 + factor(provider) + x1 + x2 + x3) (linear_r.R). generate.py calls
`generate(out, inputs, lock, write_json)`; the output is deterministic.
"""

import os
import shutil
import subprocess
import warnings

import numpy as np
import pandas as pd
from pprof_py.algorithms.linear.fixed_effect import GroupDemeaner, preprocess_groups
from pprof_py.models.linear import LinearFixedEffectModel

HERE = os.path.dirname(os.path.abspath(__file__))
FEATURES = ("x1", "x2", "x3")
TRUE_BETA = (0.8, -0.5, 0.3)
BASE_SIZES = [1, 2] + [3 + (17 * j) % 117 for j in range(38)]

CASES = (
    {"id": "lin-base", "seed": 31, "sizes": BASE_SIZES},
    {"id": "lin-singletons", "seed": 32, "sizes": [1] * 30 + [2] * 20 + [30] * 10},
    {"id": "lin-shifted", "seed": 31, "sizes": BASE_SIZES, "shift": [10000.0, -10000.0, 0.0], "scale_x3": 1.0 / 1024},
    {"id": "lin-text", "seed": 34, "sizes": [5 + (7 * j) % 40 for j in range(30)], "text": True},
    {"id": "lin-many", "seed": 35, "sizes": [3 + j % 6 for j in range(2000)]},
)

OPTIONS = {
    "pprof_py": "LinearFixedEffectModel(gamma_var_option='complete') and ('simplified'): fit(x_vars, y_var, provider_var)",
    "summary": "summary() (two_sided, level 0.95, null 0); summary(null=0.25, alternative='greater' and 'less')",
    "function_level": "GroupDemeaner (within transformation): QX'QX (packed), QX'Qy; preprocess_groups' provider means",
    "predict": "predict (DataFrame interface) for the first 100 rows; R^2 by score's formula from predict's training "
               "predictions (score's array path rejects text keys, X-034; equal to score on the numeric-key cases)",
    "negative_controls": "nudged_outcome: y + 1 in the first row of the largest provider; dropped_row: its last row removed",
    "lm": "lm(y ~ 0 + factor(provider) + x1 + x2 + x3), provider levels sorted in the C locale; sigma, vcov, logLik, AIC, BIC, "
          "summary's t, p = 2 pt(|t|, df, lower.tail = FALSE), confint; one-sided tests at null 0.25 by pt and qt; "
          "fitted values of the first 100 rows; R^2 = 1 - RSS / sum((y - mean(y))^2); the within cross-products of the "
          "provider-demeaned features (ave) and outcome",
}


def hexes(values):
    return [float(v).hex() for v in np.ravel(np.asarray(values, dtype=float))]


def packed(matrix):
    m = np.asarray(matrix, dtype=float)
    return [m[i, j] for i in range(m.shape[0]) for j in range(i, m.shape[0])]


def label(value):
    return value.item() if hasattr(value, "item") else value


def make_data(case):
    """Providers 1..m (or "prov-1".. "prov-m"); within each, rows in generation order."""
    rng = np.random.Generator(np.random.PCG64(case["seed"]))
    parts = []
    for j, size in enumerate(case["sizes"], start=1):
        g = 0.5 * rng.standard_normal()
        x1 = np.rint(rng.standard_normal(size) * 64) / 64
        x2 = np.rint((0.5 * g + rng.standard_normal(size)) * 64) / 64
        x3 = (rng.random(size) < 0.4).astype(float)
        y = g + TRUE_BETA[0] * x1 + TRUE_BETA[1] * x2 + TRUE_BETA[2] * x3 + rng.standard_normal(size)
        parts.append(pd.DataFrame({"provider": f"prov-{j}" if case.get("text") else j,
                                   "y": np.rint(y * 1024) / 1024, "x1": x1, "x2": x2, "x3": x3}))
    df = pd.concat(parts, ignore_index=True)
    for name, shift in zip(FEATURES, case.get("shift", (0.0, 0.0, 0.0))):
        df[name] = df[name] + shift
    if "scale_x3" in case:
        df["x3"] = df["x3"] * case["scale_x3"]
    df.insert(0, "id", np.arange(len(df)))
    return df


def fit(df, option="complete"):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        model = LinearFixedEffectModel(option).fit(df, x_vars=list(FEATURES), y_var="y", provider_var="provider")
    n, m, p = len(df), len(model.provider_ids_), len(FEATURES)
    rss = float(np.sum(model.residuals_ ** 2))
    loglik = -n / 2 * (np.log(2 * np.pi) + np.log(rss / n) + 1)
    return model, {
        "providers": [label(v) for v in model.provider_ids_], "sizes": [int(v) for v in model.provider_sizes_],
        "beta": hexes(model.coefficients_["beta"]), "gamma": hexes(model.coefficients_["gamma"]),
        "sigma": hexes([model.sigma_]), "rss": hexes([rss]), "df": n - m - p,
        "var_beta": hexes(packed(model.variances_["beta"])), "var_gamma": hexes(model.variances_["gamma"]),
        "loglik": hexes([loglik]), "aic": hexes([model.aic_]), "bic": hexes([model.bic_]),
        "warnings": [str(w.message) for w in caught],
    }


def summaries(model):
    out = {}
    for name, kwargs in (("two_sided", {}), ("greater", {"null": 0.25, "alternative": "greater"}),
                         ("less", {"null": 0.25, "alternative": "less"})):
        s = model.summary(**kwargs)
        out[name] = {column: hexes(s[column].to_numpy()) for column in s.columns}
    return out


def function_values(model, df):
    X = df[list(FEATURES)].to_numpy(dtype=float)
    y = df["y"].to_numpy(dtype=float)
    Q, y_means, X_means = preprocess_groups(X, y, model.provider_indices_, len(model.provider_ids_))
    QX, Qy = Q @ X, Q @ y
    return {"within_xx": hexes(packed(QX.T @ QX)), "within_xy": hexes(QX.T @ Qy),
            "x_means": hexes(X_means), "y_means": hexes(y_means)}


def r_squared(model, df, case):
    """pprof_py's `score` formula on the training predictions of `predict` (DataFrame interface): `score`
    takes arrays, and its array path rejects text provider keys (X-034); on numeric keys the two agree."""
    y = df["y"].to_numpy(dtype=float)
    predicted = model.predict(df, x_vars=list(FEATURES), provider_var="provider")
    r2 = 1 - np.sum((y - predicted) ** 2) / np.sum((y - np.mean(y)) ** 2)
    if not case.get("text"):
        score = model.score(df[list(FEATURES)].to_numpy(dtype=float), y, df["provider"].to_numpy())
        assert abs(r2 - score) <= 4e-16 * max(1.0, abs(score)), (r2, score)
    return r2


def negative_controls(df):
    largest = int(np.argmax(df.groupby("provider", sort=False).size().to_numpy()))
    key = df["provider"].drop_duplicates().iloc[largest]
    rows = np.flatnonzero(df["provider"].to_numpy() == key)
    nudged = df.copy()
    nudged.loc[rows[0], "y"] = nudged.loc[rows[0], "y"] + 1.0
    dropped = df.drop(index=rows[-1]).reset_index(drop=True)
    controls = {"nudged_outcome": fit(nudged)[1]}
    model, controls["dropped_row"] = fit(dropped)
    controls["dropped_row"]["function"] = function_values(model, dropped)
    return controls


def r_outputs(case_dir):
    subprocess.run(["Rscript", os.path.join(HERE, "linear_r.R"), case_dir], check=True)
    path = os.path.join(case_dir, "r_linear.txt")
    result = {}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            key, values = line.rstrip("\n").split("\t")
            parsed = [float.fromhex(v) for v in values.split(",")]
            result[key] = int(parsed[0]) if key == "df" else hexes(parsed)
    os.remove(path)
    return result


def generate(out, inputs, lock, write_json):
    """Writes out/linear/<case>/{case.json, input.csv, pprof_py.json, r_linear.json}."""
    root = os.path.join(out, "linear")
    shutil.rmtree(root, ignore_errors=True)
    names = []
    for case in CASES:
        case_dir = os.path.join(root, case["id"])
        os.makedirs(case_dir)
        if inputs:
            shutil.copyfile(os.path.join(inputs, "linear", case["id"], "input.csv"), os.path.join(case_dir, "input.csv"))
            df = pd.read_csv(os.path.join(case_dir, "input.csv"))
        else:
            df = make_data(case)
            df.to_csv(os.path.join(case_dir, "input.csv"), index=False)
        model, complete = fit(df)
        simplified = fit(df, "simplified")[1]
        X = df[list(FEATURES)]
        write_json(os.path.join(case_dir, "case.json"), dict(case, features=list(FEATURES), rows=len(df),
                                                            providers=len(model.provider_ids_), text=bool(case.get("text"))))
        write_json(os.path.join(case_dir, "pprof_py.json"), {
            "complete": complete, "simplified": {"var_gamma": simplified["var_gamma"]},
            "summary": summaries(model), "function": function_values(model, df),
            "predict": hexes(model.predict(df.head(100), x_vars=list(FEATURES), provider_var="provider")),
            "score": hexes([r_squared(model, df, case)]),
            "negative_controls": negative_controls(df)})
        write_json(os.path.join(case_dir, "r_linear.json"), r_outputs(case_dir))
        names.append(case["id"])
    return names
