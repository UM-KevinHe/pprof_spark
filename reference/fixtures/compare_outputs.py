#!/usr/bin/env python3
"""Compare regenerated fixture outputs with the committed ones (fixtures.yml; ADR-0005).

Inputs must be identical byte for byte. Outputs may differ in the last bits between machines,
because numpy's BLAS and numba compile for the host CPU, so they are compared under T-part (the
platform-invariance class, R2) with testkit's element-wise rule; the linear random-effect family's
optimizer-limited values take the looser classes of `tolerance`.
Usage: python reference/fixtures/compare_outputs.py <regenerated-dir> [<committed-dir>]
"""

import filecmp
import json
import os
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
RTOL = 1e-10  # T-part.parameters


def tolerance(family, key):
    """(rtol, atol) for one quantity. The linear random-effect family's default runs stop where pprof_py's Powell and
    lme4's BOBYQA happen to stop, so a last-bit platform difference moves them by up to 1e-7 (round 102): T-opt, as
    D-53 treats them; its tight optima are roots of central differences, which amplify such differences to about
    1e-10, so the rest of the family is compared at 1e-8 relative. Every other family keeps T-part."""
    if family == "linear-re":
        return (2e-4, 1e-6) if ("default" in key or "rpprof" in key) else (1e-8, 0.0)
    if family == "logistic-re":  # every fit is optimizer-limited (D-57); values at fixed parameters are not
        if "function" in key:
            return (1e-8, 0.0)
        return (2e-2, 1e-3) if "resampling" in key else (2e-4, 1e-6)  # Monte Carlo draws move with the platform
    return RTOL, 0.0


def values(node, path=()):
    if isinstance(node, dict):
        for key, value in node.items():
            yield from values(value, path + (key,))
    elif isinstance(node, list) and node and isinstance(node[0], str):
        try:
            yield path, np.array([float.fromhex(v) for v in node])
        except ValueError:  # lists of keys (three-stage cells, providers): compared exactly
            yield path, list(node)


def main(regenerated, committed):
    manifest = json.load(open(os.path.join(committed, "manifest.json"), encoding="utf-8"))
    worst, failures = 0.0, []
    families = (("cox", ("pprof_py.json", "r_survival.json")), ("logistic", ("pprof_py.json", "r_logistic.json")), ("three-stage", ("pprof_py.json", "r_threestage.json")),
                ("linear", ("pprof_py.json", "r_linear.json", "inference_pprof_py.json", "inference_r.json")),
                ("linear-re", ("pprof_py.json", "r_lme4.json", "inference_pprof_py.json")),
                ("logistic-re", ("pprof_py.json", "r_lme4.json", "inference_pprof_py.json")))
    cases = [(f, c, n) for f, n in families for c in manifest["cases"].get(f, [])]
    # Family-level reference files (logistic/poibin-tails.json, slice 2c) have no input.csv.
    shared = [(f, None, tuple(sorted(n for n in os.listdir(os.path.join(committed, f)) if n.endswith(".json"))))
              for f, _ in families if os.path.isdir(os.path.join(committed, f))]
    for family, case, names in cases + shared:
        base = os.path.join(family, case) if case else family
        if case and not filecmp.cmp(os.path.join(regenerated, base, "input.csv"), os.path.join(committed, base, "input.csv"), shallow=False):
            failures.append(f"{base}/input.csv differs")
        for name in names:
            new = dict(values(json.load(open(os.path.join(regenerated, base, name), encoding="utf-8"))))
            old = dict(values(json.load(open(os.path.join(committed, base, name), encoding="utf-8"))))
            if new.keys() != old.keys():
                failures.append(f"{base}/{name}: different quantities")
                continue
            for key, expected in old.items():
                actual = new[key]
                if isinstance(expected, list):
                    if actual != expected:
                        failures.append(f"{base}/{name} {'.'.join(key)}: keys differ")
                    continue
                scale = float(np.max(np.abs(expected[np.isfinite(expected)]), initial=0.0))
                rtol, atol = tolerance(family, key)
                allowed = atol + rtol * np.maximum(np.abs(expected), scale)
                with np.errstate(invalid="ignore"):  # equal infinities
                    diff = np.where(actual == expected, 0.0, np.abs(actual - expected))
                ratio = float(np.max(np.where(diff == 0.0, 0.0, diff / np.where(allowed == 0.0, 1e-300, allowed))))
                worst = max(worst, ratio)
                if ratio > 1.0:
                    failures.append(f"{base}/{name} {'.'.join(key)}: {ratio:.3g} times its class")
    print(f"worst output difference: {worst:.3g} of its class (T-part, {RTOL:g} relative, but for linear-re's)")
    for failure in failures:
        print("FAIL", failure)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else os.path.join(ROOT, "fixtures")))
