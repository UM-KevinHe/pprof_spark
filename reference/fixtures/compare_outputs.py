#!/usr/bin/env python3
"""Compare regenerated fixture outputs with the committed ones (fixtures.yml; ADR-0005).

Inputs must be identical byte for byte. Outputs may differ in the last bits between machines,
because numpy's BLAS and numba compile for the host CPU, so they are compared under T-part (the
platform-invariance class, R2) with testkit's element-wise rule.
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


def values(node, path=()):
    if isinstance(node, dict):
        for key, value in node.items():
            yield from values(value, path + (key,))
    elif isinstance(node, list) and node and isinstance(node[0], str):
        yield path, np.array([float.fromhex(v) for v in node])


def main(regenerated, committed):
    manifest = json.load(open(os.path.join(committed, "manifest.json"), encoding="utf-8"))
    worst, failures = 0.0, []
    families = (("cox", ("pprof_py.json", "r_survival.json")), ("logistic", ("pprof_py.json", "r_logistic.json")))
    for family, case, names in ((f, c, n) for f, n in families for c in manifest["cases"].get(f, [])):
        base = os.path.join(family, case)
        if not filecmp.cmp(os.path.join(regenerated, base, "input.csv"), os.path.join(committed, base, "input.csv"), shallow=False):
            failures.append(f"{base}/input.csv differs")
        for name in names:
            new = dict(values(json.load(open(os.path.join(regenerated, base, name), encoding="utf-8"))))
            old = dict(values(json.load(open(os.path.join(committed, base, name), encoding="utf-8"))))
            if new.keys() != old.keys():
                failures.append(f"{base}/{name}: different quantities")
                continue
            for key, expected in old.items():
                actual = new[key]
                scale = float(np.max(np.abs(expected[np.isfinite(expected)]), initial=0.0))
                allowed = RTOL * np.maximum(np.abs(expected), scale)
                diff = np.where(actual == expected, 0.0, np.abs(actual - expected))  # equal infinities
                ratio = float(np.max(np.where(diff == 0.0, 0.0, diff / np.where(allowed == 0.0, 1e-300, allowed))))
                worst = max(worst, ratio)
                if ratio > 1.0:
                    failures.append(f"{base}/{name} {'.'.join(key)}: {ratio:.3g} times T-part")
    print(f"worst output difference: {worst:.3g} of T-part ({RTOL:g} relative)")
    for failure in failures:
        print("FAIL", failure)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else os.path.join(ROOT, "fixtures")))
