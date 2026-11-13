# lme4's lmer with crossed intercepts for one fixtures/linear-re-clusters case (slice 4d-1, D-69): REML and ML with tight
# optimizer tolerances. Usage: Rscript linear_re_cluster_r.R <case_dir>; writes <case_dir>/r_lme4.json.
suppressMessages({library(lme4); library(jsonlite)})
dir <- commandArgs(trailingOnly = TRUE)[1]
case <- fromJSON(file.path(dir, "case.json"))
d <- read.csv(file.path(dir, "input.csv"))
d$provider <- factor(d$provider)
for (cv in case$cluster_vars) d[[cv]] <- factor(d[[cv]])
hex <- function(x) sprintf("%a", as.numeric(x))
terms <- paste(c("x1 + x2", "(1 | provider)", sprintf("(1 | %s)", case$cluster_vars),
                 if (!is.null(case$offset) && isTRUE(case$offset)) "offset(o)"), collapse = " + ")
f <- as.formula(paste("y ~", terms))
control <- lmerControl(optimizer = "nloptwrap", calc.derivs = FALSE,
                       optCtrl = list(xtol_abs = 1e-14, ftol_abs = 1e-14, xtol_rel = 0, ftol_rel = 0, maxeval = 1e5))
out <- list()
for (reml in c(TRUE, FALSE)) {
  fit <- if (!is.null(case$weights) && isTRUE(case$weights)) lmer(f, d, weights = w, REML = reml, control = control) else
    lmer(f, d, REML = reml, control = control)
  re <- ranef(fit)
  theta <- getME(fit, "theta")  # named by factor, in lme4's order
  effects <- lapply(c("provider", case$cluster_vars), function(v) list(keys = rownames(re[[v]]), blup = hex(re[[v]][, 1])))
  names(effects) <- c("provider", case$cluster_vars)
  out[[if (reml) "reml" else "ml"]] <- list(
    beta = hex(fixef(fit)), theta = hex(theta), theta_names = names(theta), sigma = hex(sigma(fit)),
    criterion = hex(if (reml) REMLcrit(fit) else deviance(fit)), loglik = hex(as.numeric(logLik(fit))),
    se = hex(sqrt(diag(as.matrix(vcov(fit))))), random_effects = effects,
    fitted = hex(fitted(fit)[seq_len(min(100, nrow(d)))]))
}
writeLines(toJSON(out, auto_unbox = TRUE, digits = NA), file.path(dir, "r_lme4.json"))
