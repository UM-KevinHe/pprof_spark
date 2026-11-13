"""L-BFGS in standardized coordinates (the first run scaled the wrong way), and L-BFGS on the joint
(beta, gamma) logistic likelihood."""
import sys, json
import numpy as np
from scipy.special import expit
from scipy.optimize import minimize
import feas

out = {}
for model in ("logistic", "cox"):
    for kind in ("simple", "real"):
        n, m, p = 60000, 96, 180
        prov, X, beta_true, effect, rng = feas.design(n, m, p, kind, 79)
        lin = X @ beta_true
        if model == "logistic":
            y = (rng.random(n) < expit(effect[prov] + lin)).astype(float)
            make = lambda: feas.Logistic(X, y, prov)
        else:
            hazard = 0.2 * np.exp(0.5 * (effect[prov] + 1.0) + lin)
            ev = -np.log(rng.random(n)) / hazard; ce = 5.0 * rng.random(n)
            tm = np.maximum(1.0, np.ceil(365.0 * np.minimum(ev, np.minimum(ce, 5.0)))); event = (ev <= np.minimum(ce, 5.0)).astype(float)
            make = lambda: feas.Cox(X, tm, event, prov)
        bhat, H, iters, _ = feas.newton(make(), feas.rule_for(model), np.zeros(p), extra=3)
        se = np.sqrt(np.diag(np.linalg.inv(H))); sd = X.std(0)
        dev = lambda b: float(np.max(np.abs(b - bhat) / se))
        res = {"cond_information": float(np.linalg.cond(H)), "cond_standardized": float(np.linalg.cond(H / np.outer(sd, sd)))}
        log = feas.lbfgs_trace(make(), p, sd, maxfun=600)          # beta = b / sd: standardized features
        devs = [dev(b) for b in log]
        res["lbfgs_standardized"] = {"evaluations_run": len(log), "to_1e-2": feas.first_below(devs, 1e-2), "to_1e-4": feas.first_below(devs, 1e-4),
                                     "to_1e-6": feas.first_below(devs, 1e-6), "final_dev": devs[-1]}
        if model == "logistic":
            trace = []
            ybar = y.mean()
            def f(theta):
                b, g = theta[:p], theta[p:]
                beta = b / sd; eta = g[prov] + X @ beta; pi = expit(eta); trace.append(beta.copy())
                val = np.sum(y * eta - np.logaddexp(0, eta))
                return -val / n, -np.concatenate([(X.T @ (y - pi)) / sd, np.bincount(prov, weights=y - pi, minlength=m)]) / n
            minimize(f, np.concatenate([np.zeros(p), np.full(m, np.log(ybar / (1 - ybar)))]), jac=True, method="L-BFGS-B",
                     options=dict(maxcor=10, maxfun=600, maxiter=600, ftol=1e-16, gtol=1e-13))
            devs = [dev(b) for b in trace]
            res["lbfgs_joint_standardized"] = {"evaluations_run": len(trace), "to_1e-2": feas.first_below(devs, 1e-2), "to_1e-4": feas.first_below(devs, 1e-4),
                                               "to_1e-6": feas.first_below(devs, 1e-6), "final_dev": devs[-1]}
        out[f"{model}-{kind}"] = res
        print(model, kind, json.dumps(res, default=float), flush=True)
json.dump(out, open("res-lbfgs-fix.json", "w"), indent=1, default=float)
