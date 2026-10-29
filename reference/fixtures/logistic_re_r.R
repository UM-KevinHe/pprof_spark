# lme4 references for the logistic random-effect fixtures (logistic_re_fixtures.py, D-57).
# Usage: Rscript logistic_re_r.R <case_dir>; reads <case_dir>/r_args.txt, writes <case_dir>/r_lme4.txt
# (stage.kind.quantity<TAB>hex values).
suppressMessages(library(lme4))
invisible(Sys.setlocale("LC_COLLATE", "C"))
dir <- commandArgs(trailingOnly = TRUE)[1]
a <- strsplit(readLines(file.path(dir, "r_args.txt")), "\t"); a <- setNames(lapply(a, `[`, 2), vapply(a, `[`, "", 1))
num <- function(s) as.numeric(strsplit(s, ",")[[1]])
d <- read.csv(file.path(dir, "input.csv"), stringsAsFactors = FALSE)
d$provider <- factor(d$provider, levels = sort(unique(d$provider)))
rhs <- c(if (a$intercept == "0") "0", strsplit(a$features, ",")[[1]], if (a$offset == "1") "offset(o)", "(1 | provider)")
f <- as.formula(paste("y ~", paste(rhs, collapse = " + ")))
packed <- function(A) unlist(lapply(seq_len(nrow(A)), function(i) A[i, i:ncol(A)]))
out <- list()
put <- function(key, v) { v <- as.numeric(v); v[is.na(v)] <- NaN; out[[key]] <<- v }
tight <- glmerControl(optimizer = "bobyqa", optCtrl = list(rhoend = 1e-12, maxfun = 1e6), tolPwrss = 1e-13)
quiet <- function(expr) suppressWarnings(suppressMessages(expr))
record <- function(prefix, fit) {
  re <- ranef(fit, condVar = TRUE)$provider; cmp <- getME(fit, "devcomp")$cmp; k <- min(100, nrow(d)); nd <- d[1:k, ]
  put(paste0(prefix, ".sigma"), getME(fit, "theta")); put(paste0(prefix, ".beta"), fixef(fit))
  put(paste0(prefix, ".var_beta"), packed(as.matrix(quiet(vcov(fit, use.hessian = FALSE)))))
  put(paste0(prefix, ".var_beta_hessian"), packed(as.matrix(quiet(vcov(fit)))))
  put(paste0(prefix, ".blups"), re[, 1]); put(paste0(prefix, ".condvar"), attr(re, "postVar")[1, 1, ])
  put(paste0(prefix, ".deviance"), -2 * as.numeric(logLik(fit))); put(paste0(prefix, ".loglik"), as.numeric(logLik(fit)))
  put(paste0(prefix, ".aic"), AIC(fit)); put(paste0(prefix, ".bic"), BIC(fit))
  put(paste0(prefix, ".pwrss"), cmp[["drsum"]] + cmp[["ussq"]]); put(paste0(prefix, ".ldL2"), cmp[["ldL2"]])
  put(paste0(prefix, ".ussq"), cmp[["ussq"]])
  put(paste0(prefix, ".fitted"), fitted(fit)[1:k])
  put(paste0(prefix, ".predict_fixed"), predict(fit, newdata = nd, re.form = NA, type = "response"))
  put(paste0(prefix, ".predict_re"), predict(fit, newdata = nd, type = "response"))
  put(paste0(prefix, ".predict_link_re"), predict(fit, newdata = nd, type = "link"))
  put(paste0(prefix, ".pearson"), residuals(fit, type = "pearson")[1:k])
  put(paste0(prefix, ".deviance_residuals"), residuals(fit, type = "deviance")[1:k])
}
for (stage in c("nagq1", "nagq0")) {
  ag <- if (stage == "nagq1") 1 else 0
  record(paste0(stage, ".default"), quiet(glmer(f, d, family = binomial, nAGQ = ag)))
  fitted_tight <- quiet(glmer(f, d, family = binomial, nAGQ = ag, control = tight))
  record(paste0(stage, ".tight"), fitted_tight)
  if (stage == "nagq1") laplace_tight <- fitted_tight
}
# lme4's profile interval for sigma on the tightened Laplace fit (spec 4b-2 §6, D-60), on a grid of zeta steps of
# 0.02 rather than the default cutoff/8, since confint interpolates the profile by splines (round 116)
profiled <- quiet(profile(laplace_tight, which = ".sig01", delta = 0.02, maxpts = 1000))
put("profile.lme4.limits", quiet(confint(profiled, parm = ".sig01", level = 0.95)))
# R pprof's logis_re family on its own default glmer fit (spec 4b-2 §10, X-047)
if (!is.null(a$rpprof) && !is.na(a$rpprof) && nzchar(a$rpprof)) {  # an empty field reads as NA
  for (file in c("logis_re.R", "test.logis_re.R", "SM_output.logis_re.R", "confint.logis_re.R", "summary.logis_re.R",
                 "data_check.R")) source(file.path(a$rpprof, file))
  SM_output <- function(fit, ...) SM_output.logis_re(fit, ...)
  computeDirectExp <- function(alpha, Z_beta, threads) vapply(as.numeric(alpha), function(v) sum(plogis(v + Z_beta)), 0)
  dd <- read.csv(file.path(dir, "input.csv"), stringsAsFactors = FALSE)
  # on glmer's tightened fit: its default stops too early on lgre-small-sigma and lgre-many (round 116)
  rfit <- quiet(logis_re(data = dd, Y.char = "y", Z.char = strsplit(a$features, ",")[[1]], ProvID.char = "provider",
                         control = tight))
  w <- test.logis_re(rfit)
  put("rpprof.wald.z", w$stat); put("rpprof.wald.p", w[["p value"]]); put("rpprof.wald.flag", as.numeric(as.character(w$flag)))
  put("rpprof.wald.se", w$Std.Error)
  ac <- confint.logis_re(rfit, option = "alpha")
  put("rpprof.alpha.lower", ac$alpha.Lower); put("rpprof.alpha.upper", ac$alpha.Upper)
  sm <- SM_output.logis_re(rfit, stdz = c("indirect", "direct"), measure = c("ratio", "rate"))
  put("rpprof.measures.direct_ratio", sm$direct.ratio[, 1]); put("rpprof.measures.direct_rate", sm$direct.rate[, 1])
  put("rpprof.measures.indirect_ratio", sm$indirect.ratio[, 1]); put("rpprof.measures.indirect_rate", sm$indirect.rate[, 1])
  for (alt in c("two.sided", "greater", "less")) {
    key <- paste0("rpprof.sm_", gsub(".", "_", alt, fixed = TRUE))
    ci <- quiet(confint.logis_re(rfit, option = "SM", stdz = c("indirect", "direct"), measure = c("ratio", "rate"),
                                 alternative = alt))
    put(paste0(key, ".direct_ratio_lower"), ci$CI.direct_ratio[, 2]); put(paste0(key, ".direct_ratio_upper"), ci$CI.direct_ratio[, 3])
    put(paste0(key, ".direct_rate_lower"), ci$CI.direct_rate[, 2]); put(paste0(key, ".direct_rate_upper"), ci$CI.direct_rate[, 3])
    put(paste0(key, ".indirect_ratio_lower"), ci$CI.indirect_ratio[, 2]); put(paste0(key, ".indirect_ratio_upper"), ci$CI.indirect_ratio[, 3])
  }
}
points <- matrix(num(a$points), nrow = 4, byrow = TRUE)
dev1 <- quiet(glmer(f, d, family = binomial, nAGQ = 1, devFunOnly = TRUE, control = tight))
put("function.lme4.deviance", apply(points, 1, function(x) dev1(x)))
dev0 <- quiet(glmer(f, d, family = binomial, nAGQ = 0, devFunOnly = TRUE, control = tight))
put("function.lme4.deviance0", vapply(num(a$sigma0), dev0, 0))
lines <- vapply(names(out), function(k) paste0(k, "\t", paste(sprintf("%a", out[[k]]), collapse = ",")), "")
writeLines(lines, file.path(dir, "r_lme4.txt"))
