"""Providers at the bound: does a profile-likelihood method reach pprof_py's SerBIN estimate?
Synthetic data with many providers that have no events. Three fits, all to a tight tolerance:
  S: pprof_py's SerbinAlgorithm;
  A: intercepts solved exactly, bounded providers held at the bound, beta solving the plain score equation;
  B: intercepts solved exactly, bounded providers held at the bound, beta stepping as SerBIN's reduced system."""
import os, sys, json
os.environ.setdefault("OMP_NUM_THREADS", "1")
import numpy as np
from scipy.special import expit
sys.path.insert(0, "/home/claude/pprof_py")
from pprof_py.algorithms.logistic.fixed_effect import SerbinAlgorithm, AlgorithmOptions

rng = np.random.default_rng(11)
sizes = np.r_[np.full(500, 55), np.full(100, 25)]; m = len(sizes); n = int(sizes.sum()); p = 10
prov = np.repeat(np.arange(m), sizes)
X = np.empty((n, p)); X[:, ::2] = rng.random((n, 5)) < 0.3; X[:, 1::2] = (np.floor(rng.random((n, 5)) * 129) - 64) / 64
X = X - X.mean(0)
beta_true = np.where(np.arange(p) % 2 == 0, -1.0, 1.0) * 0.5 / np.sqrt(p)
y = (rng.random(n) < expit((-3.0 + 0.5 * rng.standard_normal(m))[prov] + X @ beta_true)).astype(float)
ev = np.bincount(prov, weights=y, minlength=m)
bound, TOL = 10.0, 1e-12
out = {"n": n, "m": m, "p": p, "providers_without_events": int((ev == 0).sum()), "providers_with_only_events": int((ev == sizes).sum())}

ybar = y.mean()
alg = SerbinAlgorithm(np.column_stack([y, prov]).astype(float), 0, X.copy(), 1, np.bincount(prov),
                      np.full(m, np.log(ybar / (1 - ybar))), np.zeros(p), AlgorithmOptions(backtrack=True, max_iter=10000, bound=bound, tol=TOL))
res = alg.fit(); bS, gS = res["beta"].copy(), res["gamma"].copy()
med = np.median(gS)
out["serbin_iterations"] = int(alg.iter)
out["zero_event_gamma_minus_(median-bound)"] = float(np.max(np.abs(gS[ev == 0] - (med - bound)))) if (ev == 0).any() else None

def terms(beta, gamma):
    pi = expit(gamma[prov] + X @ beta); q = np.maximum(pi * (1 - pi), 1e-20); r = y - pi
    g = np.bincount(prov, weights=r, minlength=m); h = np.bincount(prov, weights=q, minlength=m)
    qX = q[:, None] * X
    B = np.vstack([np.bincount(prov, weights=qX[:, k], minlength=m) for k in range(p)]).T      # m x p
    return X.T @ r, X.T @ qX, g, h, B

gb, C, g, h, B = terms(bS, gS)
S = C - B.T @ (B / h[:, None])
se = np.sqrt(np.diag(np.linalg.inv(S)))
out["at_serbin_estimate"] = {"max_abs_score_beta": float(np.max(np.abs(gb))), "max_abs_reduced_rhs": float(np.max(np.abs(gb - B.T @ (g / h)))),
                             "max_abs_score_gamma_interior": float(np.max(np.abs(g[(ev > 0) & (ev < sizes)])))}

def solve_gamma(beta, gamma):
    g_ = gamma.copy()
    for _ in range(500):
        pi = expit(g_[prov] + X @ beta)
        step = np.bincount(prov, weights=y - pi, minlength=m) / np.maximum(np.bincount(prov, weights=pi * (1 - pi), minlength=m), 1e-20)
        new = g_ + step; md = np.median(new); new = np.clip(new, md - bound, md + bound)     # pprof_py's clamp
        if np.max(np.abs(new - g_)) < 1e-13: g_ = new; break
        g_ = new
    return g_

def fit(variant):
    beta = np.zeros(p); gamma = np.full(m, np.log(ybar / (1 - ybar))); it = 0
    while it < 200:
        it += 1
        gamma = solve_gamma(beta, gamma)
        gb, C, g, h, B = terms(beta, gamma)
        S = C - B.T @ (B / h[:, None])
        if variant == "B":
            step = np.linalg.solve(S, gb - B.T @ (g / h))
        else:                                   # A: bounded providers are constants, so they are not centred out
            free = np.abs(g) < 1e-9
            SA = C - B[free].T @ (B[free] / h[free][:, None])
            step = np.linalg.solve(SA, gb)
        beta = beta + step
        if np.max(np.abs(step)) < TOL: break
    return beta, solve_gamma(beta, gamma), it

for v in ("A", "B"):
    b, gm, it = fit(v)
    out[f"profile_{v}"] = {"outer_iterations": it, "max_abs_beta_diff": float(np.max(np.abs(b - bS))), "max_diff_in_se": float(np.max(np.abs(b - bS) / se)),
                           "max_abs_gamma_diff": float(np.max(np.abs(gm - gS)))}
out["tolerance_class_T_coef"] = "atol 1e-10, rtol 1e-8"
print(json.dumps(out, indent=1))
