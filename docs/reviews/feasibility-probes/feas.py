"""Numerical experiments for the algorithmic feasibility study: iteration and pass counts of candidate
algorithms for the provider fixed-effect logistic model and the stratified Cox model, and the distance of
approximate estimators from the exact one. Synthetic data only."""
import os, sys, time, json
os.environ.setdefault("OMP_NUM_THREADS", "1")
import numpy as np
from scipy import sparse
from scipy.special import expit
from scipy.optimize import minimize
from scipy.linalg import cho_factor, cho_solve

sys.path.insert(0, "/home/claude/pprof_py")


def design(n, m, p, kind, seed):
    rng = np.random.default_rng(seed)
    prov = np.repeat(np.arange(m), n // m)
    cols = np.arange(p)
    if kind == "simple":            # as databricks/02_scale.py: binary and gridded uniform, independent
        X = np.empty((n, p)); b = cols % 2 == 0
        X[:, b] = rng.random((n, int(b.sum()))) < 0.3
        X[:, ~b] = (np.floor(rng.random((n, int((~b).sum()))) * 129) - 64) / 64
        beta = np.where((cols + 1) % 2 == 0, 1.0, -1.0) * 0.5 / np.sqrt(p)
    else:                           # correlated indicators (some rare) and continuous covariates on mixed scales
        k = p // 3; kc = p - 2 * k
        f = rng.standard_normal((n, 6))
        load = rng.uniform(0.3, 0.8, (6, 2 * k)) * (rng.random((6, 2 * k)) < 0.35)
        z = f @ load + rng.standard_normal((n, 2 * k))
        prev = np.concatenate([rng.uniform(0.01, 0.08, k), rng.uniform(0.1, 0.5, k)])
        B = np.empty((n, 2 * k))
        for j in range(2 * k):
            B[:, j] = z[:, j] > np.quantile(z[:, j], 1 - prev[j])
        del z
        g = f[:, :3] @ rng.uniform(-0.8, 0.8, (3, kc)) + rng.standard_normal((n, kc))
        g = (g - g.mean(0)) / g.std(0)
        C = rng.uniform(0, 65, kc) + 10 ** rng.uniform(-1, 1.7, kc) * g
        X = np.hstack([B, C])
        beta = rng.choice([-1.0, 1.0], p) * 0.5 / np.sqrt(p) / X.std(0)
    X = X - X.mean(0)               # centred once, as pprof_spark centres internally
    effect = -1.0 + 0.5 * rng.standard_normal(m)
    return prov, X, beta, effect, rng


class Logistic:
    """Profile log-likelihood of beta: each provider's intercept solved exactly for the given beta."""
    def __init__(self, X, y, prov):
        self.X, self.y = X, y
        self.codes, self.prov = np.unique(prov, return_inverse=True)[0], np.unique(prov, return_inverse=True)[1]
        self.m = len(self.codes); self.n, self.p = X.shape
        self.ind = sparse.csr_matrix((np.ones(self.n), (self.prov, np.arange(self.n))), shape=(self.m, self.n))
        ybar = np.clip(np.bincount(self.prov, weights=y, minlength=self.m) / np.bincount(self.prov, minlength=self.m), 1e-6, 1 - 1e-6)
        self.gamma = np.log(ybar / (1 - ybar))
        self.count = {"grad": 0, "hess": 0, "hv": 0, "inner": 0}

    def _solve(self, eta0):
        g = self.gamma.copy()
        for _ in range(200):
            pi = expit(g[self.prov] + eta0)
            step = np.bincount(self.prov, weights=self.y - pi, minlength=self.m) / np.maximum(np.bincount(self.prov, weights=pi * (1 - pi), minlength=self.m), 1e-20)
            g += np.clip(step, -5, 5); self.count["inner"] += 1
            if np.max(np.abs(step)) < 1e-11: break
        self.gamma = g
        eta = g[self.prov] + eta0
        return eta, expit(eta)

    def fg(self, beta, kind="grad"):
        eta, pi = self._solve(self.X @ beta)
        self.count[kind] += 1
        self._pi = pi
        return float(np.sum(self.y * eta - np.logaddexp(0, eta))), self.X.T @ (self.y - pi)

    def _curv(self):
        q = self._pi * (1 - self._pi)
        qX = q[:, None] * self.X
        Bm = self.ind @ qX
        h = np.bincount(self.prov, weights=q, minlength=self.m)
        return q, qX, Bm, h

    def fgh(self, beta):
        val, grad = self.fg(beta, "hess")
        q, qX, Bm, h = self._curv()
        return val, grad, self.X.T @ qX - Bm.T @ (Bm / h[:, None])

    def fg_hvprep(self, beta):
        val, grad = self.fg(beta)
        q, qX, Bm, h = self._curv()
        self._hv = (q, Bm, h)
        self.diag = (qX * self.X).sum(0) - ((Bm ** 2) / h[:, None]).sum(0)
        return val, grad

    def hv(self, v):
        q, Bm, h = self._hv; self.count["hv"] += 1
        return self.X.T @ (q * (self.X @ v)) - Bm.T @ ((Bm @ v) / h)


class Cox:
    """Stratified Cox partial likelihood with Breslow ties, right-censored rows; the information in the
    weighted-Gram form X'diag(r c)X - sum_t d_t xbar_t xbar_t'."""
    def __init__(self, X, time, event, strata):
        order = np.lexsort((-event, -time, strata))
        self.X = np.ascontiguousarray(X[order]); t = time[order]; self.d = event[order].astype(float); s = strata[order]
        n = len(t); self.n, self.p = self.X.shape
        new_group = np.r_[True, (s[1:] != s[:-1]) | (t[1:] != t[:-1])]
        gid = np.cumsum(new_group) - 1
        gstart = np.flatnonzero(new_group); gend = np.r_[gstart[1:] - 1, n - 1]
        new_str = np.r_[True, s[1:] != s[:-1]]
        sid = np.cumsum(new_str) - 1
        sstart = np.flatnonzero(new_str); send = np.r_[sstart[1:] - 1, n - 1]
        dg = np.bincount(gid, weights=self.d, minlength=gid[-1] + 1)
        ev = dg > 0
        self.eg_end, self.eg_start, self.eg_d = gend[ev], gstart[ev], dg[ev]
        self.eg_sstart = sstart[sid[gstart[ev]]]
        self.row_gstart = gstart[gid]; self.row_send = send[sid]
        self.sumx = self.X.T @ self.d
        self.K = len(self.eg_d)
        self.count = {"grad": 0, "hess": 0, "hv": 0}

    def fg(self, beta, kind="grad"):
        eta = self.X @ beta; r = np.exp(eta)
        C0 = np.concatenate([[0.0], np.cumsum(r)])
        S0 = C0[self.eg_end + 1] - C0[self.eg_sstart]
        Xbar = np.empty((self.K, self.p))
        for a in range(0, self.p, 30):
            C1 = np.cumsum(r[:, None] * self.X[:, a:a + 30], axis=0)
            C1 = np.vstack([np.zeros((1, C1.shape[1])), C1])
            Xbar[:, a:a + 30] = (C1[self.eg_end + 1] - C1[self.eg_sstart]) / S0[:, None]
        self._state = (r, S0, Xbar); self.count[kind] += 1
        return float(self.d @ eta - self.eg_d @ np.log(S0)), self.sumx - Xbar.T @ self.eg_d

    def _weights(self):
        r, S0, _ = self._state
        inc = np.zeros(self.n + 1); inc[self.eg_start] = self.eg_d / S0
        suf = np.cumsum(inc[::-1])[::-1]
        return r * (suf[self.row_gstart] - suf[self.row_send + 1])

    def fgh(self, beta):
        val, grad = self.fg(beta, "hess")
        w = self._weights(); Xbar = self._state[2]
        return val, grad, self.X.T @ (w[:, None] * self.X) - Xbar.T @ (self.eg_d[:, None] * Xbar)

    def fg_hvprep(self, beta):
        val, grad = self.fg(beta)
        self._w = self._weights(); Xbar = self._state[2]
        self.diag = self._w @ (self.X ** 2) - self.eg_d @ (Xbar ** 2)
        return val, grad

    def hv(self, v):
        Xbar = self._state[2]; self.count["hv"] += 1
        return self.X.T @ (self._w * (self.X @ v)) - Xbar.T @ (self.eg_d * (Xbar @ v))


def rule_for(model):
    if model == "logistic":   # SerBIN's rule: the accepted beta step below 1e-8 in the largest coordinate
        return lambda b0, b1, v0, v1: np.max(np.abs(b1 - b0)) < 1e-8
    return lambda b0, b1, v0, v1: abs(v1 - v0) / (abs(v0) if v0 != 0 else 1.0) < 1e-9   # the Cox rule


def line(obj, beta, step, val, evaluate):
    t = 1.0
    while True:
        out = evaluate(beta + t * step)
        if np.isfinite(out[0]) and out[0] >= val - 1e-10 * abs(val): return t, out
        t *= 0.5
        if t < 1e-8: return t, out


def newton(obj, rule, beta0, max_iter=60, extra=0):
    beta = beta0.copy(); val, grad, H = obj.fgh(beta); iters = 0; path = []; after = None
    while iters < max_iter:
        iters += 1
        step = np.linalg.solve(H, grad)
        t, (nv, ng, nH) = line(obj, beta, step, val, obj.fgh)
        done = rule(beta, beta + t * step, val, nv)
        beta = beta + t * step; val, grad, H = nv, ng, nH; path.append(beta.copy())
        if done and after is None: after = iters
        if after is not None and iters >= after + extra: break
    return beta, H, after if after is not None else iters, path


def chord(obj, rule, p, refresh, max_iter=300):
    beta = np.zeros(p); val, grad, H = obj.fgh(beta); c = cho_factor(H); iters = 0
    while iters < max_iter:
        iters += 1
        step = cho_solve(c, grad)
        ev = obj.fgh if iters in refresh else obj.fg
        t, out = line(obj, beta, step, val, ev)
        done = rule(beta, beta + t * step, val, out[0])
        beta = beta + t * step; val, grad = out[0], out[1]
        if len(out) == 3: c = cho_factor(out[2])
        if done: break
    return beta, iters


def lbfgs_trace(obj, p, scale, maxfun=500):
    log = []
    def f(b):
        beta = b / scale
        val, grad = obj.fg(beta); log.append(beta.copy())
        return -val / obj.n, -(grad / scale) / obj.n
    minimize(f, np.zeros(p), jac=True, method="L-BFGS-B",
             options=dict(maxcor=10, maxfun=maxfun, maxiter=maxfun, ftol=1e-16, gtol=1e-13))
    return log


def newton_cg(obj, rule, p, max_outer=80):
    beta = np.zeros(p); val, grad = obj.fg_hvprep(beta); g0 = np.linalg.norm(grad); outer = 0
    while outer < max_outer:
        outer += 1
        gn = np.linalg.norm(grad); tol = min(0.1, np.sqrt(gn / g0)) * gn
        d = np.zeros(p); r = grad.copy(); z = r / obj.diag; q = z.copy(); rz = r @ z
        for _ in range(4 * p):                      # preconditioned conjugate gradients on H d = grad
            Hq = obj.hv(q); a = rz / (q @ Hq)
            d += a * q; r -= a * Hq
            if np.linalg.norm(r) <= tol: break
            z = r / obj.diag; rz1 = r @ z; q = z + (rz1 / rz) * q; rz = rz1
        t, (nv, ng) = line(obj, beta, d, val, obj.fg_hvprep)
        done = rule(beta, beta + t * d, val, nv)
        beta = beta + t * d; val, grad = nv, ng
        if done: break
    return beta, outer


def first_below(devs, thr):
    hits = [i + 1 for i, d in enumerate(devs) if d < thr]
    return hits[0] if hits else None


def run(model, kind, n, m, p, seed=79):
    t_start = time.time()
    prov, X, beta_true, effect, rng = design(n, m, p, kind, seed)
    lin = X @ beta_true
    if model == "logistic":
        y = (rng.random(n) < expit(effect[prov] + lin)).astype(float)
        make = lambda idx: Logistic(X[idx], y[idx], prov[idx])
    else:
        hazard = 0.2 * np.exp(0.5 * (effect[prov] + 1.0) + lin)
        ev = -np.log(rng.random(n)) / hazard; ce = 5.0 * rng.random(n)
        tm = np.maximum(1.0, np.ceil(365.0 * np.minimum(ev, np.minimum(ce, 5.0)))); event = (ev <= np.minimum(ce, 5.0)).astype(float)
        make = lambda idx: Cox(X[idx], tm[idx], event[idx], prov[idx])
    everything = np.arange(n)
    rule = rule_for(model)
    out = {"model": model, "design": kind, "n": n, "m": m, "p": p}

    # exact Newton, to the model's own stopping rule and then three more steps for a reference optimum
    obj = make(everything)
    bhat, H, iters, path = newton(obj, rule, np.zeros(p), extra=3)
    se = np.sqrt(np.diag(np.linalg.inv(H)))
    dev = lambda b: float(np.max(np.abs(b - bhat) / se))
    out["newton"] = {"iterations": iters, "hessian_passes": iters + 1, "dev_at_stop": dev(path[iters - 1]),
                     "cond_information": float(np.linalg.cond(H)), "max_abs_z_truth": float(np.max(np.abs(bhat - beta_true) / se))}
    if model == "logistic": out["newton"]["inner_solves_per_pass"] = obj.count["inner"] / obj.count["hess"]
    else: out["events"] = int(obj.d.sum()); out["event_times"] = int(obj.K)

    # chord: the Hessian of the start (and optionally of the first iterate) reused
    for name, refresh in (("chord_H0", ()), ("chord_H0_H1", (1,)), ("chord_H0_H2", (2,))):
        o = make(everything); b, it = chord(o, rule, p, refresh)
        out[name] = {"iterations": it, "hessian_passes": o.count["hess"], "gradient_passes": o.count["grad"], "dev_at_stop": dev(b)}

    # L-BFGS on the same objective, raw and standardized coordinates
    sd = X.std(0)
    for name, scale in (("lbfgs_raw", np.ones(p)), ("lbfgs_standardized", 1.0 / sd)):
        o = make(everything); log = lbfgs_trace(o, p, scale)
        devs = [dev(b) for b in log]
        out[name] = {"evaluations_run": len(log), "to_1e-2": first_below(devs, 1e-2), "to_1e-4": first_below(devs, 1e-4),
                     "to_1e-6": first_below(devs, 1e-6), "final_dev": devs[-1]}

    # truncated Newton: conjugate gradients on Hessian-vector products, Jacobi preconditioner
    o = make(everything); b, outer = newton_cg(o, rule, p)
    out["newton_cg"] = {"outer_iterations": outer, "gradient_passes": o.count["grad"], "hv_passes": o.count["hv"], "dev_at_stop": dev(b)}

    # pilot on a tenth of the providers, then Newton on everything
    groups = np.unique(prov)
    pilot_groups = groups[rng.permutation(len(groups))[: max(len(groups) // 10, 4)]]
    o = make(np.flatnonzero(np.isin(prov, pilot_groups)))
    btil, _, pit, _ = newton(o, rule, np.zeros(p))
    o = make(everything); _, _, pol, ppath = newton(o, rule, btil)
    out["pilot_then_newton"] = {"pilot_iterations": pit, "pilot_dev": dev(btil), "full_iterations": pol, "full_hessian_passes": pol + 1,
                                "dev_after_step": [dev(b) for b in ppath[:3]]}

    # divide and conquer over 30 groups of providers; one round of the surrogate likelihood
    K = 30 if len(groups) >= 90 else 6
    part = rng.permutation(len(groups)) % K
    bs, Hs = [], []
    for k in range(K):
        idx = np.flatnonzero(np.isin(prov, groups[part == k]))
        o = make(idx); b, Hk, _, _ = newton(o, rule, np.zeros(p)); bs.append(b); Hs.append(Hk)
    simple = np.mean(bs, axis=0)
    weighted = np.linalg.solve(sum(Hs), sum(Hk @ b for Hk, b in zip(Hs, bs)))
    out["divide_and_conquer"] = {"K": K, "simple_average_dev": dev(simple), "information_weighted_dev": dev(weighted)}
    full = make(everything); _, gfull = full.fg(weighted)
    idx = np.flatnonzero(np.isin(prov, groups[part == 0])); loc = make(idx); _, gloc = loc.fg(weighted)
    shift = gfull / n - gloc / len(idx)
    b = weighted.copy()
    for _ in range(30):                              # maximise the surrogate with the local Hessian
        v, g, Hk = loc.fgh(b); step = np.linalg.solve(Hk / len(idx), g / len(idx) + shift); b = b + step
        if np.max(np.abs(step)) < 1e-10: break
    out["surrogate_likelihood_one_round"] = {"dev": dev(b)}
    onestep = weighted + np.linalg.solve(*full.fgh(weighted)[2:0:-1])
    out["one_step_from_weighted_average"] = {"dev": dev(onestep)}

    if model == "logistic":
        from pprof_py.algorithms.logistic.fixed_effect import SerbinAlgorithm, BanAlgorithm, AlgorithmOptions
        ybar = y.mean()
        for name, cls, cap in (("pprof_py_serbin", SerbinAlgorithm, 10000), ("pprof_py_ban", BanAlgorithm, 3000)):
            data = np.column_stack([y, prov]).astype(float)
            alg = cls(data, 0, X.copy(), 1, np.bincount(prov), np.full(m, np.log(ybar / (1 - ybar))), np.zeros(p),
                      AlgorithmOptions(backtrack=True, max_iter=cap, bound=10.0, tol=1e-8))
            t0 = time.time(); res = alg.fit()
            out[name] = {"iterations": int(alg.iter), "seconds": round(time.time() - t0, 1), "dev": dev(res["beta"])}
    else:
        # mini-batch stochastic gradient on batches of 64 rows of one stratum, Adam steps, iterate averaging
        Xs = X / sd; order = np.argsort(prov, kind="stable"); best = {}
        for lr in (0.003, 0.01, 0.03):
            b = np.zeros(p); m1 = np.zeros(p); m2 = np.zeros(p); k = 0; r2 = np.random.default_rng(1)
            for epoch in range(1, 21):
                avg = np.zeros(p); cnt = 0
                idx = order.reshape(m, -1)[:, :]; idx = r2.permuted(idx, axis=1)
                batches = idx.reshape(-1, 64) if idx.shape[1] % 64 == 0 else idx[:, : idx.shape[1] // 64 * 64].reshape(-1, 64)
                for bt in batches[r2.permutation(len(batches))]:
                    tt = tm[bt]; o2 = np.argsort(-tt, kind="stable"); xb = Xs[bt][o2]; tb = tt[o2]; db = event[bt][o2]
                    if db.sum() == 0: continue
                    rr = np.exp(xb @ b); ends = np.searchsorted(-tb, -tb, side="right") - 1
                    S0 = np.cumsum(rr)[ends]; S1 = np.cumsum(rr[:, None] * xb, axis=0)[ends]
                    g = -(db @ (xb - S1 / S0[:, None])) / db.sum()
                    k += 1; m1 = 0.9 * m1 + 0.1 * g; m2 = 0.999 * m2 + 0.001 * g * g
                    b = b - lr * (m1 / (1 - 0.9 ** k)) / (np.sqrt(m2 / (1 - 0.999 ** k)) + 1e-8)
                    avg += b; cnt += 1
                if epoch in (1, 5, 20):
                    d_ = dev((avg / cnt) / sd)
                    best[epoch] = min(best.get(epoch, np.inf), d_)
        out["minibatch_sgd"] = {f"dev_after_{e}_epochs": v for e, v in best.items()}
    out["seconds"] = round(time.time() - t_start, 1)
    return out


def check_cox():
    from pprof_py.algorithms.survival.cox_likelihood import cox_partial_likelihood
    prov, X, beta, effect, rng = design(6000, 12, 8, "real", 3)
    hz = 0.2 * np.exp(X @ beta); ev = -np.log(rng.random(6000)) / hz; ce = 5 * rng.random(6000)
    tm = np.maximum(1.0, np.ceil(365 * np.minimum(ev, ce))); d = (ev <= ce).astype(float)
    o = Cox(X, tm, d, prov); v, g, H = o.fgh(beta * 0.7)
    rv, rg, rH = cox_partial_likelihood(X, np.zeros(6000), tm, d, beta * 0.7, strata=prov, ties="breslow")
    return {"value": abs(v - rv) / abs(rv), "score": float(np.max(np.abs(g - rg)) / np.max(np.abs(rg))), "information": float(np.max(np.abs(H - rH)) / np.max(np.abs(rH)))}


if __name__ == "__main__":
    if sys.argv[1] == "check":
        print(json.dumps(check_cox()))
    else:
        res = run(sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]))
        print(json.dumps(res, indent=1, default=float))
