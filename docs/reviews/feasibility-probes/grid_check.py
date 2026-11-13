"""An unstratified Cox evaluation from per-time aggregates: no sort, any row order, two passes.
Pass 1 sums r and r*x by distinct time; the driver takes suffix sums over the time grid; pass 2 forms the
weighted Gram matrix. Compared with the sorted sweep."""
import json, numpy as np
from scipy import sparse
import feas

n, p = 60000, 180
prov, X, beta_true, effect, rng = feas.design(n, 96, p, "real", 79)
hazard = 0.2 * np.exp(X @ beta_true); ev = -np.log(rng.random(n)) / hazard; ce = 5.0 * rng.random(n)
tm = np.maximum(1.0, np.ceil(365.0 * np.minimum(ev, np.minimum(ce, 5.0)))); d = (ev <= np.minimum(ce, 5.0)).astype(float)
beta = 0.6 * beta_true
ref = feas.Cox(X, tm, d, np.zeros(n, dtype=int)); rv, rg, rH = ref.fgh(beta)

perm = rng.permutation(n)                       # rows in arbitrary order, as partitions would hold them
Xp, tp, dp = X[perm], tm[perm], d[perm]
times, tix = np.unique(tp, return_inverse=True); K = len(times)
eta = Xp @ beta; r = np.exp(eta)
by_time = sparse.csr_matrix((np.ones(n), (tix, np.arange(n))), shape=(K, n))
R0 = by_time @ r; R1 = by_time @ (r[:, None] * Xp); dt = by_time @ dp          # pass 1: K x (p + 2) numbers
S0 = np.cumsum(R0[::-1])[::-1]; S1 = np.cumsum(R1[::-1], axis=0)[::-1]           # driver: suffix sums over the grid
xbar = S1 / S0[:, None]
value = dp @ eta - dt @ np.log(S0)
score = Xp.T @ dp - xbar.T @ dt
c = np.cumsum(dt / S0)                                                            # hazard increments up to each time
w = r * c[tix]
info = Xp.T @ (w[:, None] * Xp) - xbar.T @ (dt[:, None] * xbar)                   # pass 2: one weighted Gram matrix
print(json.dumps({"n": n, "p": p, "distinct_times": int(K), "bytes_to_driver_pass_1": int(K * (p + 2) * 8),
                  "bytes_for_S2_per_time_instead": int(K * p * (p + 1) // 2 * 8),
                  "value_rel_diff": abs(value - rv) / abs(rv), "score_scaled_diff": float(np.max(np.abs(score - rg)) / np.max(np.abs(rg))),
                  "information_scaled_diff": float(np.max(np.abs(info - rH)) / np.max(np.abs(rH)))}))
