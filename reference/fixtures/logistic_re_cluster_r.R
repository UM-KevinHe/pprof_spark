# lme4's glmer (Laplace) with crossed intercepts for one fixtures/logistic-re-clusters case (slice 4d-2, D-71), with
# tight optimizer tolerances. Usage: Rscript logistic_re_cluster_r.R <case_dir>; writes <case_dir>/r_lme4.json.
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
tight <- glmerControl(optimizer = "bobyqa", optCtrl = list(rhoend = 1e-12, maxfun = 1e6), tolPwrss = 1e-13)
fit <- suppressMessages(suppressWarnings(glmer(f, d, family = binomial, nAGQ = 1, control = tight)))
re <- ranef(fit, condVar = TRUE)
theta <- getME(fit, "theta")
effects <- lapply(c("provider", case$cluster_vars), function(v) {
  list(keys = rownames(re[[v]]), blup = hex(re[[v]][, 1]), se = hex(sqrt(attr(re[[v]], "postVar")[1, 1, ])))
})
names(effects) <- c("provider", case$cluster_vars)
out <- list(beta = hex(fixef(fit)), sigma = hex(theta), sigma_names = names(theta), deviance = hex(deviance(fit)),
            loglik = hex(as.numeric(logLik(fit))), se = hex(sqrt(diag(as.matrix(vcov(fit))))),
            random_effects = effects, fitted = hex(fitted(fit)[seq_len(min(100, nrow(d)))]))
writeLines(toJSON(out, auto_unbox = TRUE, digits = NA), file.path(dir, "r_lme4.json"))
