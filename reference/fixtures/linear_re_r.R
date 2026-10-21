# lme4 references for the linear random-effect fixtures (linear_re_fixtures.py, D-52).
# Usage: Rscript linear_re_r.R <case_dir>; reads <case_dir>/r_args.txt, writes <case_dir>/r_lme4.txt
# (criterion.kind.quantity<TAB>hex values).
suppressMessages(library(lme4))
invisible(Sys.setlocale("LC_COLLATE", "C"))
dir <- commandArgs(trailingOnly = TRUE)[1]
a <- strsplit(readLines(file.path(dir, "r_args.txt")), "\t"); a <- setNames(lapply(a, `[`, 2), vapply(a, `[`, "", 1))
num <- function(s) as.numeric(strsplit(s, ",")[[1]])
d <- read.csv(file.path(dir, "input.csv"), stringsAsFactors = FALSE)
d$provider <- factor(d$provider, levels = sort(unique(d$provider)))
rhs <- c(if (a$intercept == "0") "0", strsplit(a$features, ",")[[1]], if (a$offset == "1") "offset(o)", "(1 | provider)")
f <- as.formula(paste("y ~", paste(rhs, collapse = " + ")))
thetas <- num(a$thetas); taus <- num(a$taus); wtilde <- num(a$wtilde)
packed <- function(A) unlist(lapply(seq_len(nrow(A)), function(i) A[i, i:ncol(A)]))
out <- list()
put <- function(key, v) out[[key]] <<- as.numeric(v)
record <- function(prefix, fit) {
  re <- ranef(fit, condVar = TRUE)$provider; cmp <- getME(fit, "devcomp")$cmp; k <- min(100, nrow(d))
  put(paste0(prefix, ".theta"), getME(fit, "theta")); put(paste0(prefix, ".sigma"), sigma(fit))
  put(paste0(prefix, ".sigma_alpha"), getME(fit, "theta") * sigma(fit))
  put(paste0(prefix, ".beta"), fixef(fit)); put(paste0(prefix, ".var_beta"), packed(as.matrix(vcov(fit))))
  put(paste0(prefix, ".blups"), re[, 1]); put(paste0(prefix, ".condvar"), attr(re, "postVar")[1, 1, ])
  put(paste0(prefix, ".deviance"), if (isREML(fit)) REMLcrit(fit) else deviance(fit))
  put(paste0(prefix, ".loglik"), as.numeric(logLik(fit))); put(paste0(prefix, ".aic"), AIC(fit)); put(paste0(prefix, ".bic"), BIC(fit))
  put(paste0(prefix, ".pwrss"), cmp[["pwrss"]]); put(paste0(prefix, ".ldL2"), cmp[["ldL2"]]); put(paste0(prefix, ".ussq"), cmp[["ussq"]])
  put(paste0(prefix, ".fitted"), fitted(fit)[1:k])
  put(paste0(prefix, ".predict_fixed"), predict(fit, newdata = d[1:k, ], re.form = NA))
  put(paste0(prefix, ".predict_re"), predict(fit, newdata = d[1:k, ]))
}
slope <- function(dev, t) { h <- 3e-3 * t
  (4 * (dev(t + h) - dev(t - h)) / (2 * h) - (dev(t + 2 * h) - dev(t - 2 * h)) / (4 * h)) / 3 }
for (crit in c("reml", "ml")) {
  reml <- crit == "reml"
  fit <- if (a$weights == "1") lmer(f, d, weights = w, REML = reml) else lmer(f, d, REML = reml)
  record(paste0(crit, ".default"), fit)
  lf <- if (a$weights == "1") lFormula(f, d, REML = reml, weights = w) else lFormula(f, d, REML = reml)
  devfun <- do.call(mkLmerDevfun, lf); dev <- function(t) devfun(sqrt(t))
  put(paste0(crit, ".function.deviance"), vapply(thetas, devfun, 0))
  put(paste0(crit, ".function.slope"), vapply(taus, function(t) slope(dev, t), 0))
  tau0 <- getME(fit, "theta")^2; h0 <- 1e-6 / wtilde
  if (dev(h0) > dev(0) && dev(0) <= dev(max(tau0, h0))) tstar <- 0 else {
    lo <- tau0 * (1 - 1e-3); hi <- tau0 * (1 + 1e-3); k <- 0
    while (!(slope(dev, lo) < 0 && slope(dev, hi) > 0)) { lo <- lo / 2; hi <- hi * 2; k <- k + 1; if (k > 40) stop("no bracket") }
    tstar <- uniroot(function(t) slope(dev, t), c(lo, hi), tol = 1e-14 * tau0, maxiter = 500)$root
  }
  opt <- optimizeLmer(devfun)
  opt$par <- sqrt(tstar); opt$fval <- devfun(sqrt(tstar))  # leaves the deviance function's state at the tight theta
  record(paste0(crit, ".tight"), mkMerMod(environment(devfun), opt, lf$reTrms, fr = lf$fr))
  put(paste0(crit, ".tight.tau"), tstar)
}
lines <- vapply(names(out), function(k) paste0(k, "\t", paste(sprintf("%a", out[[k]]), collapse = ",")), "")
writeLines(lines, file.path(dir, "r_lme4.txt"))
