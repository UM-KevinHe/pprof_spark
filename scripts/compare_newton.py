#!/usr/bin/env python3
"""Step 5's exit (plan §4; docs/spec/logistic/profile-newton.md §9, D-83): compares PerfJob reports of logistic fits
by SerBIN and by the profile-likelihood Newton (`--newton profile`) on the same points.

Usage:
    python3 scripts/compare_newton.py REPORTS... [--baseline REPORTS...]
    python3 scripts/compare_newton.py --self-test

REPORTS are text files holding report lines, one JSON object per line (other lines, such as the rest of
03_perf's output, are skipped), or PerfJob report folders, searched for `report/part-*.txt`. Reports given after
`--baseline` are step 2's exit runs, listed beside the others.

For each point the script lists every fit: its method, seconds, iterations, the Newton passes and their executor CPU.
Fit times are compared only when every compared fit ran on one fixed executor count, the same for all: with
autoscaling, the first fit of a pair waits for workers.
Where both methods ran, it then compares the profile method's medians with SerBIN's, and the log-likelihoods. It
checks the profile estimates against SerBIN's by the T-coef rule, and the standard errors by the T-var rule, with the
values of testkit/src/main/resources/tolerances.conf. Step 5's exit asks for T-coef: a worst ratio of at most 1.
Reports from builds before round 199 carry no estimates, so that check is skipped for them.
"""
import glob
import json
import math
import os
import statistics
import sys

PASSES = ("pprof.logistic.pass-a", "pprof.logistic.pass-b", "pprof.logistic.profile")
CONF = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "testkit", "src", "main", "resources",
                    "tolerances.conf")


def tolerances(path=CONF):
    """The `<class>.rtol` and `<class>.atol` entries of tolerances.conf."""
    values = {}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.split("#", 1)[0].strip()
            if "=" in line:
                key, value = (part.strip() for part in line.split("=", 1))
                values[key] = float(value)
    return values


def worst_ratio(actual, expected, rtol, atol):
    """D-09's rule, as testkit's Tolerance.worstRatio: element i passes when |a - e| <= atol + rtol * max(|e|, s),
    s the largest finite magnitude in `expected`; the worst ratio of observed to allowed difference, at most 1 to
    pass. A NaN, or an infinity not matched exactly, gives infinity."""
    if len(actual) != len(expected):
        raise ValueError(f"{len(actual)} values against {len(expected)}")
    scale = max((abs(e) for e in expected if math.isfinite(e)), default=0.0)
    worst = 0.0
    for a, e in zip(actual, expected):
        if not (math.isfinite(a) and math.isfinite(e)):
            ratio = 0.0 if a == e else math.inf
        else:
            allowed = atol + rtol * max(abs(e), scale)
            difference = abs(a - e)
            ratio = (difference / allowed) if allowed > 0 else (0.0 if difference == 0 else math.inf)
        worst = max(worst, ratio)
    return worst


def read(paths):
    """Every report line in the given files and folders."""
    files = []
    for path in paths:
        if os.path.isdir(path):
            files += sorted(glob.glob(os.path.join(path, "**", "report", "part-*.txt"), recursive=True))
        else:
            files.append(path)
    reports = []
    for name in files:
        with open(name, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line.startswith("{") and '"runner"' in line:
                    reports.append(json.loads(line))
    return reports


def summary(report, group):
    """One fit's figures."""
    fit = report["fit"]
    sites = fit.get("recording", {}).get("sites", [])
    executions = {tag: sum(s["executions"] for s in sites if s["tags"] == tag) for tag in PASSES}
    cpu = sum(s["cpu_ms"] for s in sites if s["tags"] in PASSES)
    passes = sum(executions.values())
    rows = report["point"]["n"]
    return {
        "group": group,
        "point": report["point"]["name"],
        "seconds": fit["seconds"],
        "succeeded": fit["succeeded"],
        "iterations": fit.get("iterations"),
        "converged": fit.get("converged"),
        "loglik": fit.get("loglik"),
        "passes": passes,
        "executions": executions,
        "cpu_ms": cpu,
        "us_per_row_pass": 1000.0 * cpu / (rows * passes) if passes else math.nan,
        "estimates": fit.get("estimates"),
        "errors": fit.get("standard_errors"),
        "storage": fit.get("storage_level_used", ""),
        "executors": (fit.get("recording", {}).get("executors_min"), fit.get("recording", {}).get("executors_max")),
    }


def compare(current, baseline=(), out=print, conf=None):
    """Prints the comparison; returns the verdicts as {point: True/False/None} (None: no estimates to check)."""
    conf = conf or tolerances()
    fits = [summary(r, r["options"].get("newton", "serbin")) for r in current]
    fits += [summary(r, "step 2") for r in baseline]
    verdicts = {}
    for point in sorted({f["point"] for f in fits}):
        here = [f for f in fits if f["point"] == point]
        out(f"\n## {point}\n")
        out("| Fit | Seconds | Executors | Iterations | Passes | Pass CPU (s) | µs per row per pass | ℓ |")
        out("|---|---|---|---|---|---|---|---|")
        for f in here:
            passes = ", ".join(f"{tag.rsplit('.', 1)[1]} {n}" for tag, n in f["executions"].items() if n)
            low, high = f["executors"]
            out(f"| {f['group']}{'' if f['succeeded'] else ' (failed)'} | {f['seconds']:.1f} | "
                f"{'?' if low is None else (low if low == high else f'{low} to {high}')} | {f['iterations']} | "
                f"{f['passes']} ({passes}) | {f['cpu_ms'] / 1000:.1f} | {f['us_per_row_pass']:.2f} | {f['loglik']!r} |")
        groups = {g: [f for f in here if f["group"] == g and f["succeeded"]] for g in ("serbin", "profile", "step 2")}
        if not (groups["serbin"] and groups["profile"]):
            verdicts[point] = None
            out("\nBoth methods are needed for the comparison.")
            continue

        def median(group, key):
            return statistics.median(f[key] for f in groups[group])

        s_cpu, p_cpu = median("serbin", "cpu_ms"), median("profile", "cpu_ms")
        s_sec, p_sec = median("serbin", "seconds"), median("profile", "seconds")
        out(f"\n- Passes: SerBIN {median('serbin', 'passes'):g}, profile {median('profile', 'passes'):g}"
            + (f", step 2 {median('step 2', 'passes'):g}" if groups["step 2"] else "") + ".")
        out(f"- Pass CPU: SerBIN {s_cpu / 1000:.1f} s, profile {p_cpu / 1000:.1f} s, {s_cpu / p_cpu:.2f} times less"
            + (f"; step 2 {median('step 2', 'cpu_ms') / 1000:.1f} s" if groups["step 2"] else "") + ".")
        # Fit times compare only on a fixed cluster: with autoscaling, the first fit of a pair waits for workers
        # (round 202). Every compared fit must have run on one executor count, the same for all.
        compared = groups["serbin"] + groups["profile"]
        fixed = all(f["executors"][0] is not None and f["executors"][0] == f["executors"][1] for f in compared) \
            and len({f["executors"] for f in compared}) == 1
        if fixed:
            change = 100.0 * (1.0 - p_sec / s_sec)
            out(f"- Fit: SerBIN {s_sec:.1f} s, profile {p_sec:.1f} s, "
                f"{abs(change):.0f}% {'shorter' if change >= 0 else 'longer'}"
                + (f"; step 2 {median('step 2', 'seconds'):.1f} s" if groups["step 2"] else "") + ".")
        else:
            ranges = ", ".join(f"{f['group']} {f['executors'][0]} to {f['executors'][1]}" for f in compared)
            out(f"- Fit time: not compared, because the executors changed during a fit or differed between fits "
                f"({ranges}). Fix the cluster's size to compare it (perf-runbook).")
        serbin, profile = groups["serbin"][0], groups["profile"][0]
        relative = abs(profile["loglik"] - serbin["loglik"]) / abs(serbin["loglik"])
        out(f"- ℓ: relative difference {relative:.3g}.")
        if serbin["estimates"] is None or profile["estimates"] is None:
            verdicts[point] = None
            out("- Estimates: not in these reports (round 199's build or later writes them); not checked.")
            continue
        coef = worst_ratio(profile["estimates"], serbin["estimates"], conf["T-coef.rtol"], conf["T-coef.atol"])
        var = worst_ratio(profile["errors"], serbin["errors"], conf["T-var.rtol"], conf["T-var.atol"])
        shift = max(abs(p - s) / e for p, s, e in zip(profile["estimates"], serbin["estimates"], serbin["errors"]))
        verdicts[point] = coef <= 1.0
        out(f"- Estimates: T-coef worst ratio {coef:.3g} ({'passes' if coef <= 1.0 else 'FAILS'}); "
            f"largest |Δβ|/SE {shift:.3g}. Standard errors: T-var worst ratio {var:.3g}.")
    return verdicts


def self_test():
    conf = {"T-coef.rtol": 1e-8, "T-coef.atol": 1e-10, "T-var.rtol": 1e-7, "T-var.atol": 0.0}
    assert worst_ratio([1.0, 2.0], [1.0, 2.0], 1e-8, 1e-10) == 0.0
    assert abs(worst_ratio([1.0 + 2e-8], [1.0], 1e-8, 0.0) - 2.0) < 1e-6
    # The scale is the largest magnitude: a near-zero element is judged against it.
    assert worst_ratio([1e-9, 10.0], [0.0, 10.0], 1e-8, 0.0) < 1.0
    assert worst_ratio([math.nan], [1.0], 1e-8, 1e-10) == math.inf

    def report(newton, passes, estimates, executors=(4, 4)):
        sites = [{"tags": tag, "executions": n, "cpu_ms": ms} for tag, n, ms in passes]
        options = {"model": "logistic"} if newton is None else {"model": "logistic", "newton": newton}
        return {"runner": "PerfJob", "point": {"name": "p", "n": 1000}, "options": options,
                "fit": {"seconds": 10.0, "succeeded": True, "iterations": 5, "converged": True, "loglik": -500.0,
                        "estimates": estimates, "standard_errors": [0.1, 0.2],
                        "recording": {"sites": sites, "executors_min": executors[0], "executors_max": executors[1]}}}

    serbin = report(None, [("pprof.logistic.pass-a", 5, 5000), ("pprof.logistic.pass-b", 5, 1000)], [0.5, -0.3])
    close = report("profile", [("pprof.logistic.profile", 5, 4000)], [0.5 + 1e-12, -0.3])
    far = report("profile", [("pprof.logistic.profile", 5, 4000)], [0.5 + 1e-6, -0.3])
    lines = []
    assert compare([serbin, close], out=lines.append, conf=conf) == {"p": True}
    assert any("Passes: SerBIN 10, profile 5" in line for line in lines), lines
    assert any("6.0 s, profile 4.0 s, 1.50 times less" in line for line in lines), lines
    assert compare([serbin, far], out=lambda _: None, conf=conf) == {"p": False}
    assert compare([serbin], out=lambda _: None, conf=conf) == {"p": None}
    assert any("Fit: SerBIN 10.0 s, profile 10.0 s" in line for line in lines), lines
    scaling = report(None, [("pprof.logistic.pass-a", 5, 5000), ("pprof.logistic.pass-b", 5, 1000)], [0.5, -0.3],
                     executors=(3, 4))
    fixed = report("profile", [("pprof.logistic.profile", 5, 4000)], [0.5, -0.3], executors=(8, 8))
    lines = []
    compare([scaling, fixed], out=lines.append, conf=conf)
    assert any("Fit time: not compared" in line and "serbin 3 to 4, profile 8 to 8" in line for line in lines), lines
    assert not any(line.startswith("- Fit: ") for line in lines), lines
    print("self-test: OK")


def main(args):
    if args == ["--self-test"]:
        self_test()
        return 0
    if not args or args[0].startswith("--"):
        print(__doc__)
        return 2
    current, baseline = (args[:args.index("--baseline")], args[args.index("--baseline") + 1:]) \
        if "--baseline" in args else (args, [])
    verdicts = compare(read(current), read(baseline))
    checked = [v for v in verdicts.values() if v is not None]
    print(f"\nT-coef checked at {len(checked)} of {len(verdicts)} points; "
          f"{'all pass' if checked and all(checked) else ('none checked' if not checked else 'NOT ALL PASS')}.")
    return 0 if all(checked) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
