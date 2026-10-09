#!/usr/bin/env python3
# Mirrored in CI by testkit's FixturesSuite; keep the two in step.
"""Calibrate the tolerance classes against the Cox and logistic reference fixtures (D-09, NN-9; ADR-0005).

For each case and tie method it reports, under the comparison rule of testkit's `Tolerance`
(element i passes when |a_i - e_i| <= atol + rtol * max(|e_i|, s), s = max_j |e_j|), the worst
ratio of observed difference to allowed difference:
  - pprof_py against R survival::coxph (two validated implementations): must be at most 1;
  - each negative control against pprof_py: must be at least MARGIN, so a real defect fails.
Writes docs/parity/cox-calibration.md, logistic-calibration.md and three-stage-calibration.md. Usage: python reference/fixtures/calibrate.py
"""

import json
import math
import os
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
MARGIN = 10.0

QUANTITIES = (("coef", "T-coef"), ("se", "T-var"), ("covariance", "T-var"), ("loglik", "T-fn"))
FUNCTION = (("loglik", "T-fn"), ("score", "T-fn"), ("information", "T-fn"))


def tolerances():
    values = {}
    with open(os.path.join(ROOT, "testkit", "src", "main", "resources", "tolerances.conf"), encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = (part.strip() for part in line.split("=", 1))
                values[key] = float(value)
    return values


def ratio(tol, cls, actual, expected):
    rtol, atol = tol[f"{cls}.rtol"], tol[f"{cls}.atol"]
    a = np.array([float.fromhex(v) for v in actual])
    e = np.array([float.fromhex(v) for v in expected])
    scale = float(np.max(np.abs(e)))
    allowed = atol + rtol * np.maximum(np.abs(e), scale)
    diff = np.abs(a - e)
    with np.errstate(divide="ignore", invalid="ignore"):
        r = np.where(diff == 0.0, 0.0, diff / allowed)
    return float(np.max(r))


LOGISTIC_GLM = (("beta", "T-coef"), ("gamma", "T-coef"), ("var_beta", "T-var"), ("var_gamma", "T-var"),
                ("var_case_mix", "T-var"), ("loglik", "T-fn"))
LOGISTIC_SERBIN = (("default", "beta"), ("default", "gamma"), ("tight", "beta"), ("tight", "gamma"))


def logistic(tol, manifest, fixtures):
    """pprof_py against stats::glm (tight fits) and R pprof's SerBIN (default and tight), and the negative
    controls against pprof_py's tight fit; writes docs/parity/logistic-calibration.md."""
    rows, failures, informational = [], [], []
    for case in manifest["cases"].get("logistic", []):
        py = json.load(open(os.path.join(fixtures, "logistic", case, "pprof_py.json"), encoding="utf-8"))
        r = json.load(open(os.path.join(fixtures, "logistic", case, "r_logistic.json"), encoding="utf-8"))
        checks = [(f"{q} vs glm (tight fits)", cls, py["tight"][q], r["glm"][q]) for q, cls in LOGISTIC_GLM if "glm" in r]
        checks += [(f"{q} vs R SerBIN ({fit})", "T-coef", py[fit][q], r[f"serbin_{fit}"][q]) for fit, q in LOGISTIC_SERBIN]
        for label, cls, a, e in checks:
            agreement = ratio(tol, cls, a, e)
            ok = agreement <= 1.0
            if not ok:
                failures.append((case, label))
            rows.append((case, label, cls, f"{agreement:.3g}", "yes" if ok else "**no**"))
        for q, cls in LOGISTIC_GLM:
            for name, control in py["negative_controls"].items():
                if name == "cluster_shifted":  # moves only the robust variances (checked below)
                    continue
                miss = ratio(tol, cls, control[q], py["tight"][q])
                ok = miss >= MARGIN
                if not ok:
                    failures.append((case, f"{name} {q}"))
                rows.append((case, f"negative control {name}: {q}", cls, f"{miss:.3g}", "yes" if ok else "**no**"))
        # Slice 2b (D-34): tests, AUC, predictions and robust variances against R; their controls.
        inference = py["inference"]["tight"]
        if "glm_tests" in r:
            t = r["glm_tests"]
            checks2b = [("LR vs glm", "T-test", inference["lr"]["stat"], t["lr"]),
                        ("score vs Rao", "T-test", inference["score"]["stat"], t["rao"]),
                        ("predictions vs glm", "T-meas", inference["predict"], t["predict"])]
            if "auc" in t:
                checks2b.append(("AUC vs Mann-Whitney", "T-meas", inference["auc"], t["auc"]))
            if "glm_robust" in r:
                checks2b += [(f"robust {q} vs vcovCL", "T-var", inference["robust"][q], r["glm_robust"][q])
                             for q in ("var_beta", "var_case_mix", "var_fixed_beta")]
            for label, cls, a, e in checks2b:
                agreement = ratio(tol, cls, a, e)
                ok = agreement <= 1.0
                if not ok:
                    failures.append((case, label))
                rows.append((case, label, cls, f"{agreement:.3g}", "yes" if ok else "**no**"))
        for name, control in py["negative_controls"].items():
            c = control["inference"]
            targets = [] if name == "cluster_shifted" else [
                ("lr stat", "T-test", c["lr"]["stat"], inference["lr"]["stat"]),
                ("score stat", "T-test", c["score"]["stat"], inference["score"]["stat"])]
            if name == "flipped_outcome" and inference["auc"]:
                targets.append(("AUC", "T-meas", c["auc"], inference["auc"]))
            if name == "cluster_shifted":
                targets += [(f"robust {q}", "T-var", c["robust"][q], inference["robust"][q])
                            for q in ("var_beta", "var_case_mix", "var_fixed_beta")]
            for label, cls, a, e in targets:
                miss = ratio(tol, cls, a, e)
                ok = miss >= MARGIN
                if not ok:
                    failures.append((case, f"{name} {label}"))
                rows.append((case, f"negative control {name}: {label}", cls, f"{miss:.3g}", "yes" if ok else "**no**"))
        # Slice 2c (D-35): pprof_py's provider tests against R pprof's test.logis_fe; their controls.
        tests = py["provider_tests"]
        for mine, theirs in (("exact_two_sided_median", "r_tests_exact_two_sided"), ("exact_greater_median", "r_tests_exact_greater"),
                             ("exact_less_median", "r_tests_exact_less"), ("score_two_sided_median", "r_tests_score_two_sided")):
            if theirs not in r:
                continue
            a, e = tests[mine], r[theirs]
            za = [float.fromhex(v) for v in a["z"]]
            ze = [float.fromhex(v) for v in e["z"]]
            finite = [k for k in range(len(ze)) if math.isfinite(ze[k])]
            # R does not cap z; pprof_py caps abs(z) at the normal quantile of 1e-300 (spec 2c §2).
            capped = all(math.copysign(1.0, za[k]) == math.copysign(1.0, ze[k]) and abs(za[k]) > 37.0
                         for k in range(len(ze)) if not math.isfinite(ze[k]))
            agreement = ratio(tol, "T-test", [a["z"][k] for k in finite], [e["z"][k] for k in finite])
            flags = [int(v) for v in a["flag"]] == [int(float.fromhex(v)) for v in e["flag"]]
            ok = agreement <= 1.0 and flags and capped
            if not ok:
                failures.append((case, f"{mine} vs R"))
            rows.append((case, f"{mine} z vs R test.logis_fe (flags {'equal' if flags else 'DIFFER'})", "T-test",
                         f"{agreement:.3g}", "yes" if ok else "**no**"))
        targets = [(f"control reference_shifted: exact z", tests["exact_two_sided_median"]["z"],
                    py["provider_test_controls"]["reference_shifted"]["exact_two_sided_median"]["z"]),
                   (f"control flipped_outcome: exact z", tests["exact_two_sided_median"]["z"],
                    py["provider_test_controls"]["flipped_outcome"]["exact_two_sided_median"]["z"]),
                   (f"control greater against less: exact z", tests["exact_greater_median"]["z"], tests["exact_less_median"]["z"])]
        for label, a, e in targets:
            miss = ratio(tol, "T-test", e, a)
            ok = miss >= MARGIN
            if not ok:
                failures.append((case, label))
            rows.append((case, f"negative {label}", "T-test", f"{miss:.3g}", "yes" if ok else "**no**"))
        # Slice 2d (D-36): pprof_py's standardized measures against R pprof's SM_output.logis_fe; controls.
        standard = py["standardization"]
        if "r_sm" in r:
            table = standard["measures"]["median"]
            for name, theirs in (("indirect_ratio", "indirect"), ("indirect_rate", "indirect"),
                                 ("direct_ratio", "direct"), ("direct_rate", "direct")):
                agreement = ratio(tol, "T-meas", table[theirs][name], r["r_sm"][name])
                ok = agreement <= 1.0
                if not ok:
                    failures.append((case, f"{name} vs R"))
                rows.append((case, f"{name} vs R SM_output.logis_fe", "T-meas", f"{agreement:.3g}", "yes" if ok else "**no**"))
        controls2d = py["standardization_controls"]
        targets = [("control reference_shifted: indirect expected", "T-meas",
                    controls2d["reference_shifted"]["indirect_expected"], standard["measures"]["median"]["indirect"]["expected"]),
                   ("control reference_shifted: direct-rate test z", "T-test",
                    controls2d["reference_shifted"]["direct_rate_z"], standard["tests"]["direct_rate_auto"]["z"]),
                   ("control flipped_outcome: direct-rate test z", "T-test",
                    controls2d["flipped_outcome"]["direct_rate_z"], standard["tests"]["direct_rate_auto"]["z"]),
                   ("control log scale for the logit: direct-rate test z", "T-test",
                    standard["tests"]["direct_rate_log"]["z"], standard["tests"]["direct_rate_auto"]["z"])]
        for label, cls, a, e in targets:
            miss = ratio(tol, cls, a, e)
            ok = miss >= MARGIN
            if not ok:
                failures.append((case, label))
            rows.append((case, f"negative {label}", cls, f"{miss:.3g}", "yes" if ok else "**no**"))
        if "glm_nondegenerate" in r:
            informational.append(f"- {case}: pprof_py's tight β̂ against `glm` without the degenerate providers is "
                                 f"{ratio(tol, 'T-coef', py['tight']['beta'], r['glm_nondegenerate']['beta']):.3g} of T-coef "
                                 "(X-018: the bound keeps their records in the β score; not a parity check)")
    lines = ["# Logistic fixed-effect tolerance calibration (D-09, D-30, D-34)", "",
             "Generated by `reference/fixtures/calibrate.py` from the fixtures of pprof_py "
             f"{manifest['reference']['pprof_py']['version']}, R {manifest['reference']['r']['version']} (`stats::glm`) "
             f"and R pprof {manifest['reference']['r_pprof']['version']} (`logis_BIN_fe_prov`). Ratios are observed "
             "difference over allowed difference under testkit's `Tolerance` rule: agreement must be at most 1 and "
             f"every negative control, compared with pprof_py's tight fit, at least {MARGIN:g}.", "",
             "| Case | Comparison | Class | Ratio | OK |", "|---|---|---|---|---|"]
    lines += [f"| {case} | {label} | {cls} | {value} | {ok} |" for case, label, cls, value, ok in rows]
    tails = json.load(open(os.path.join(fixtures, "logistic", "poibin-tails.json"), encoding="utf-8"))
    for name, vector in sorted(tails.items()):
        for row in vector["tails"]:
            exact = [float.fromhex(v) for v in row["mpmath"]]
            theirs = [float.fromhex(v) for v in row["pprof_py"]]
            for k, label in enumerate(("upper mid-p", "lower mid-p", "P(X >= o)", "P(X <= o)")):
                if exact[k] == 0.0:
                    continue
                error = abs(theirs[k] - exact[k]) / exact[k]
                region = "reliable" if k in (1, 3) or exact[k] >= 1e-7 else "X-024"
                informational.append(f"- poibin-tails {name}, o = {row['observed']}, {label}: mpmath {exact[k]:.3g}, "
                                     f"pprof_py relative error {error:.2g} ({region})")
    lines += [""] + informational + ["", f"Result: {'all checks pass' if not failures else f'{len(failures)} checks fail'}."]
    with open(os.path.join(ROOT, "docs", "parity", "logistic-calibration.md"), "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")
    print("logistic:", lines[-1])
    for failure in failures:
        print("FAIL", *failure)
    return failures


def three_stage(tol, manifest, fixtures):
    """Slice 2f-1 (D-39): pprof_py's preparation against R's glmm.data.prep and its stage 1 against R; the
    controls against pprof_py; writes docs/parity/three-stage-calibration.md."""
    rows, failures = [], []
    def record(case, label, cls, value, ok):
        rows.append((case, label, cls, value, "yes" if ok else "**no**"))
        if not ok:
            failures.append((case, label))
    for case in manifest["cases"].get("three-stage", []):
        py = json.load(open(os.path.join(fixtures, "three-stage", case, "pprof_py.json"), encoding="utf-8"))
        r = json.load(open(os.path.join(fixtures, "three-stage", case, "r_threestage.json"), encoding="utf-8"))
        prep, tight = py["preparation"], py["stage1"]["tight"]
        if "prep" in r:
            rp = r["prep"]
            same = prep["rid"] == rp["rid"] and prep["cell"] == rp["cell"] and prep["included"] == rp["included"]
            record(case, "kept records, cells and included vs R glmm.data.prep", "exact", "equal" if same else "differ", same)
            agreement = ratio(tol, "T-meas", prep["y_adj"], rp["y_adj"])
            record(case, "y_adj vs R glmm.data.prep", "T-meas", f"{agreement:.3g}", agreement <= 1.0)
            record(case, "cells present vs R n.fac.hosp", "exact", f"{prep['cells']} vs {rp['cells_present']}",
                   prep["cells"] == rp["cells_present"])
        agreement = ratio(tol, "T-coef", tight["beta"], r["glm_stage1"]["beta"])
        record(case, "stage 1 beta (tight) vs R glm", "T-coef", f"{agreement:.3g}", agreement <= 1.0)
        if "stage23" in r:
            agreement = ratio(tol, "T-coef", tight["beta"], r["stage23"]["beta"])
            record(case, "stage 1 beta (tight) vs R's stage 1 beta", "T-coef", f"{agreement:.3g}", agreement <= 1.0)
        controls = py["negative_controls"]
        moved = (controls["cutoff_11"]["kept"], controls["cutoff_11"]["included_cells"]) != (prep["kept"], prep["included_cells"])
        record(case, "negative control cutoff_11: kept records or included cells change", "exact",
               f"{controls['cutoff_11']['kept']}/{controls['cutoff_11']['included_cells']} vs {prep['kept']}/{prep['included_cells']}", moved)
        miss = ratio(tol, "T-coef", controls["flipped_outcome"]["beta"], tight["beta"])
        record(case, "negative control flipped_outcome: stage 1 beta", "T-coef", f"{miss:.3g}", miss >= MARGIN)
        if "stage3" in py:  # slice 2f-2 (D-40): reference-independent checks and controls
            s3 = py["stage3"]
            hx = lambda v: [float.fromhex(x) for x in v]
            score = max(abs(v) for v in hx(s3["function"]["tight"]["score"]))
            record(case, "stage 3: max abs(score) at the tight fit", "below 1e-9", f"{score:.2g}", score <= 1e-9)
            gap = max(abs(a - b) for a, b in zip(hx(s3["small_sigma"]["gamma"]), hx(s3["zero_sigma_limit"])))
            record(case, "stage 3: sigma 1e-4 against the sigma = 0 limit (X-005)", "below 100 sigma^2", f"{gap:.3g}", gap <= 1e-6)
            for name, gamma in s3["negative_controls"].items():
                miss = ratio(tol, "T-coef", gamma, s3["tight"]["gamma"])
                record(case, f"negative control {name}: stage 3 gamma", "T-coef", f"{miss:.3g}", miss >= MARGIN)
    for case in manifest["cases"].get("three-stage", []):  # slice 2f-3 (D-41, D-42)
        py = json.load(open(os.path.join(fixtures, "three-stage", case, "pprof_py.json"), encoding="utf-8"))
        if "stage2" not in py:
            continue
        r = json.load(open(os.path.join(fixtures, "three-stage", case, "r_threestage.json"), encoding="utf-8"))["glmer_stage2"]
        s2, tight = py["stage2"], py["stage2"]["tight"]
        hx = lambda v: [float.fromhex(x) for x in v]
        fractional = float.fromhex(r["saturated"][0]) != 0.0
        for q in ("sigma", "intercept", "blup_providers", "blup_clusters"):
            agreement = ratio(tol, "T-opt", tight[q], r[q])
            if fractional:  # lme4's binomial likelihood differs for non-integer y_adj (spec 2f-3 §7)
                record(case, f"stage 2 {q}: pprof_py (tight) vs glmer, informational (fractional y_adj)", "T-opt",
                       f"{agreement:.3g}", True)
            else:
                record(case, f"stage 2 {q}: pprof_py (tight) vs glmer", "T-opt", f"{agreement:.3g}", agreement <= 1.0)
            default = ratio(tol, "T-opt", s2["default"][q], tight[q])
            record(case, f"stage 2 {q}: pprof_py default vs tight", "T-opt", f"{default:.3g}", default <= 1.0)
        devfun = hx(r["devfun"])
        saturated = float.fromhex(r["saturated"][0])
        for k, name in enumerate(("optimum", "fixed", "boundary")):
            gap = devfun[k] - saturated - hx(s2["function"][name]["deviance"])[0]
            record(case, f"stage 2 deviance at the {name} point: lme4's devfun minus pprof_py's, informational", "—",
                   f"{gap:.3g}", True)
        for name, control in s2["negative_controls"].items():
            miss = ratio(tol, "T-opt", control["sigma"] + control["intercept"], tight["sigma"] + tight["intercept"])
            record(case, f"negative control {name}: stage 2 sigma and intercept", "T-opt", f"{miss:.3g}", miss >= MARGIN)
    for case in manifest["cases"].get("three-stage", []):  # slice 2f-4a (D-43)
        py = json.load(open(os.path.join(fixtures, "three-stage", case, "pprof_py.json"), encoding="utf-8"))
        if "inference" not in py:
            continue
        inf = py["inference"]
        hx = lambda v: [float.fromhex(x) for x in v]
        (e95, lo95, hi95), (e90, lo90, hi90) = hx(inf["profile"]["0.95"]), hx(inf["profile"]["0.9"])
        nested = lo95 <= lo90 <= e90 <= hi90 <= hi95 and e95 == e90
        record(case, "profile interval of sigma_c: 0.9 inside 0.95, both around the estimate", "exact",
               f"[{lo90:.4g}, {hi90:.4g}] in [{lo95:.4g}, {hi95:.4g}]", nested)
        if "sigma" in inf["sensitivity"]:
            same = hx(inf["sensitivity"]["sigma"]) == [lo95, e95, hi95]
            record(case, "sigma_sensitivity's sigmas equal the 0.95 profile interval", "exact", "equal" if same else "differ", same)
        else:
            record(case, f"sigma_sensitivity raised {inf['sensitivity']['error']} (X-005); interval lower end {lo95:.3g}", "—",
                   "informational", True)
        for name, control in inf["negative_controls"].items():
            miss = ratio(tol, "T-test", control["z"], inf["tests"]["exact_two_sided"]["z"])
            record(case, f"negative control {name}: exact test z", "T-test", f"{miss:.3g}", miss >= MARGIN)
    rules = json.load(open(os.path.join(fixtures, "three-stage", "gauss-hermite.json"), encoding="utf-8"))
    for n, rule in sorted(rules.items(), key=lambda item: int(item[0])):
        for part in ("nodes", "weights"):
            agreement = ratio(tol, "T-fn", rule[f"numpy_{part}"], rule[part])
            record("gauss-hermite", f"{n}-node {part}: numpy against mpmath", "T-fn", f"{agreement:.3g}", agreement <= 1.0)
    lines = ["# Three-stage tolerance calibration: preparation and stages 1 to 3 (D-39, D-40, D-41, D-42)", "",
             "Generated by `reference/fixtures/calibrate.py` from the three-stage fixtures (pprof_py "
             f"{manifest['reference']['pprof_py']['version']}, R {manifest['reference']['r']['version']}, and the R outputs in "
             "pprof_py's golden data). Agreement ratios must be at most 1; negative controls at least "
             f"{MARGIN:g} or, for counts, different.", "",
             "| Case | Check | Class | Value | Pass |", "|---|---|---|---|---|"]
    lines += [f"| {c} | {label} | {cls} | {value} | {ok} |" for c, label, cls, value, ok in rows]
    lines += ["", f"Result: {'all checks pass' if not failures else f'{len(failures)} checks fail'}."]
    with open(os.path.join(ROOT, "docs", "parity", "three-stage-calibration.md"), "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")
    print("three-stage:", lines[-1])
    for failure in failures:
        print("FAIL", *failure)
    return failures


def main():
    tol = tolerances()
    fixtures = os.path.join(ROOT, "fixtures")
    manifest = json.load(open(os.path.join(fixtures, "manifest.json"), encoding="utf-8"))
    rows, failures = [], []
    for case in manifest["cases"]["cox"]:
        py = json.load(open(os.path.join(fixtures, "cox", case, "pprof_py.json"), encoding="utf-8"))
        r = json.load(open(os.path.join(fixtures, "cox", case, "r_survival.json"), encoding="utf-8"))
        for ties in ("breslow", "efron"):
            checks = [(f"{q} (tight fit)", cls, py[ties]["tight"][q], r[ties]["tight"][q]) for q, cls in QUANTITIES]
            checks += [(f"{q} at {b}", cls, py[ties][b][q], r[ties][b][q]) for b in ("beta_zero", "beta_fixed") for q, cls in FUNCTION]
            for label, cls, a, e in checks:
                agreement = ratio(tol, cls, a, e)
                controls = {name: ratio(tol, cls, fit[label.split(" ")[0]], py[ties]["tight"][label.split(" ")[0]])
                            for name, fit in py[ties]["negative_controls"].items()} if "tight" in label else {}
                weakest = min(controls.values()) if controls else None
                ok = agreement <= 1.0 and (weakest is None or weakest >= MARGIN)
                if not ok:
                    failures.append((case, ties, label))
                rows.append((case, ties, label, cls, agreement, weakest, ok))
            rows.append((case, ties, "iterations, default control", "exact",
                         abs(py[ties]["default"]["iterations"] - r[ties]["default"]["iterations"]), None,
                         py[ties]["default"]["iterations"] == r[ties]["default"]["iterations"]))

    lines = ["# Cox tolerance calibration (D-09)", "",
             "Generated by `reference/fixtures/calibrate.py` from the fixtures of pprof_py "
             f"{manifest['reference']['pprof_py']['version']} and R {manifest['reference']['r']['version']} "
             f"with survival {manifest['reference']['r']['survival']}. Ratios are observed difference over allowed "
             "difference under testkit's `Tolerance` rule. Agreement between the two validated implementations "
             f"must be at most 1; every negative control must be at least {MARGIN:g}.", "",
             "| Case | Ties | Quantity | Class | pprof_py vs R | Weakest negative control | OK |",
             "|---|---|---|---|---|---|---|"]
    for case, ties, label, cls, agreement, weakest, ok in rows:
        agreement_text = f"{agreement:d}" if isinstance(agreement, int) else f"{agreement:.3g}"
        weakest_text = "—" if weakest is None else f"{weakest:.3g}"
        lines.append(f"| {case} | {ties} | {label} | {cls} | {agreement_text} | {weakest_text} | {'yes' if ok else '**no**'} |")
    lines += ["", f"Result: {'all checks pass' if not failures else f'{len(failures)} checks fail'}."]
    os.makedirs(os.path.join(ROOT, "docs", "parity"), exist_ok=True)
    with open(os.path.join(ROOT, "docs", "parity", "cox-calibration.md"), "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")
    print("\n".join(lines[-1:]))
    for failure in failures:
        print("FAIL", *failure)
    failures += logistic(tol, manifest, fixtures)
    failures += three_stage(tol, manifest, fixtures)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
