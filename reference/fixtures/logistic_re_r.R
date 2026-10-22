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
  record(paste0(stage, ".tight"), quiet(glmer(f, d, family = binomial, nAGQ = ag, control = tight)))
}
points <- matrix(num(a$points), nrow = 4, byrow = TRUE)
dev1 <- quiet(glmer(f, d, family = binomial, nAGQ = 1, devFunOnly = TRUE, control = tight))
put("function.lme4.deviance", apply(points, 1, function(x) dev1(x)))
dev0 <- quiet(glmer(f, d, family = binomial, nAGQ = 0, devFunOnly = TRUE, control = tight))
put("function.lme4.deviance0", vapply(num(a$sigma0), dev0, 0))
lines <- vapply(names(out), function(k) paste0(k, "\t", paste(sprintf("%a", out[[k]]), collapse = ",")), "")
writeLines(lines, file.path(dir, "r_lme4.txt"))
