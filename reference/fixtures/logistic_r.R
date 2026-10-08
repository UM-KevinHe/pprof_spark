# Logistic fixed-effect reference outputs from R for one fixture case (ADR-0005, D-30).
# Usage: Rscript logistic_r.R <case-dir> <binomial> <r-pprof-src-dir> <excluded> <degenerate>
# <excluded> and <degenerate> are comma-separated provider IDs (possibly empty): the providers
# pprof_py screened out, and those with no events or only events. Writes <case-dir>/r_logistic.txt:
# one line per quantity, "label.quantity<TAB>comma-separated %a values" (exact hexadecimal).
#   glm (or glm_nondegenerate, fitted without the degenerate providers; X-018): the maximum-likelihood
#     estimate by stats::glm, its model-based variances and the log-likelihood without sum(lchoose(n, y)),
#     which pprof_py omits.
#   serbin_default, serbin_tight: R pprof's logis_BIN_fe_prov (src/Fixed_effect.cpp at the commit
#     pinned in REFERENCE.lock) at tol 1e-8 and 1e-13; binomial rows are expanded to Bernoulli rows.
args <- commandArgs(trailingOnly = TRUE)
case_dir <- args[[1]]
binomial <- args[[2]] == "true"
src <- args[[3]]
ids <- function(text) if (nzchar(text)) as.integer(strsplit(text, ",")[[1]]) else integer()
excluded <- ids(if (length(args) >= 4) args[[4]] else "")
degenerate <- ids(if (length(args) >= 5) args[[5]] else "")
features <- c("x1", "x2", "x3")

d <- read.csv(file.path(case_dir, "input.csv"))
if (!binomial) d$n <- 1L
d <- d[!(d$provider %in% excluded), ]

hexes <- function(v) paste(sprintf("%a", as.numeric(v)), collapse = ",")
packed <- function(m) unlist(lapply(seq_len(nrow(m)), function(i) m[i, i:ncol(m)]))
out <- character()
emit <- function(key, v) out <<- c(out, paste0(key, "\t", hexes(v)))

glm_outputs <- function(dd, label) {
  dd$provider <- factor(dd$provider)
  f <- glm(cbind(y, n - y) ~ 0 + provider + x1 + x2 + x3, family = binomial(), data = dd,
           control = glm.control(epsilon = 1e-15, maxit = 200))
  k <- nlevels(dd$provider)
  b <- (k + 1):(k + length(features))
  V <- vcov(f)
  xbar <- colSums(dd$n * as.matrix(dd[, features])) / sum(dd$n)
  case_mix <- sapply(seq_len(k), function(j)
    V[j, j] + 2 * sum(xbar * V[b, j]) + drop(t(xbar) %*% V[b, b] %*% xbar))
  emit(paste0(label, ".beta"), coef(f)[b])
  emit(paste0(label, ".gamma"), coef(f)[seq_len(k)])
  emit(paste0(label, ".var_beta"), packed(V[b, b]))
  emit(paste0(label, ".var_gamma"), diag(V)[seq_len(k)])
  emit(paste0(label, ".var_case_mix"), case_mix)
  emit(paste0(label, ".loglik"), as.numeric(logLik(f)) - sum(lchoose(dd$n, dd$y)))
  emit(paste0(label, ".iterations"), f$iter)
}
clustered <- length(args) >= 6 && args[[6]] == "true"
inference_outputs <- function(dd) {
  # Slice 2b: LR and Rao score tests, AUC (Bernoulli rows), predictions, cluster-robust variances.
  dd$provider <- factor(dd$provider)
  ctl <- glm.control(epsilon = 1e-15, maxit = 200)
  full <- glm(cbind(y, n - y) ~ 0 + provider + x1 + x2 + x3, family = binomial(), data = dd, control = ctl)
  reduced <- lapply(features, function(v) update(full, as.formula(paste(". ~ . -", v))))
  emit("glm_tests.lr", sapply(reduced, function(r) 2 * (as.numeric(logLik(full)) - as.numeric(logLik(r)))))
  emit("glm_tests.rao", sapply(reduced, function(r) anova(r, full, test = "Rao")$Rao[2]))
  if (all(dd$n == 1)) {
    ranks <- rank(fitted(full))
    positive <- dd$y == 1
    emit("glm_tests.auc", (sum(ranks[positive]) - sum(positive) * (sum(positive) + 1) / 2) /
           (sum(positive) * sum(!positive)))
  }
  emit("glm_tests.predict", unname(fitted(full)[seq_len(min(100, nrow(dd)))]))
  if (clustered) {
    k <- nlevels(dd$provider)
    b <- (k + 1):(k + length(features))
    V <- sandwich::vcovCL(full, cluster = interaction(dd$provider, dd$patient, drop = TRUE),
                          type = "HC0", cadjust = FALSE)
    xbar <- colSums(dd$n * as.matrix(dd[, features])) / sum(dd$n)
    emit("glm_robust.var_beta", packed(V[b, b]))
    emit("glm_robust.var_case_mix", sapply(seq_len(k), function(j)
      V[j, j] + 2 * sum(xbar * V[b, j]) + drop(t(xbar) %*% V[b, b] %*% xbar)))
    p <- fitted(full)
    residual <- dd$y - dd$n * p
    information <- dd$n * p * (1 - p)
    emit("glm_robust.var_fixed_beta", sapply(levels(dd$provider), function(j) {
      rows <- dd$provider == j
      sum(tapply(residual[rows], dd$patient[rows], sum)^2) / sum(information[rows])^2
    }))
  }
}
if (length(degenerate)) {
  glm_outputs(d[!(d$provider %in% degenerate), ], "glm_nondegenerate")
} else {
  glm_outputs(d, "glm")
  inference_outputs(d)
}

suppressMessages(Rcpp::sourceCpp(file.path(src, "Fixed_effect.cpp"), cacheDir = file.path(src, "cache"),
                                 showOutput = FALSE))
e <- d[order(d$provider), ]
if (binomial) {
  y <- unlist(lapply(seq_len(nrow(e)), function(i) c(rep(1, e$y[i]), rep(0, e$n[i] - e$y[i]))))
  e <- e[rep(seq_len(nrow(e)), e$n), ]
  e$y <- y
}
y <- e$y
n_prov <- as.vector(table(e$provider))
Z <- as.matrix(e[, features])
start <- rep(log(mean(y) / (1 - mean(y))), length(n_prov))
for (label in c("default", "tight")) {
  s <- logis_BIN_fe_prov(as.matrix(y), Z, n_prov, start, rep(0, length(features)), threads = 1,
                         tol = if (label == "default") 1e-8 else 1e-13, max_iter = 10000, bound = 10,
                         message = FALSE, backtrack = TRUE, stop = "beta")
  emit(paste0("serbin_", label, ".beta"), s$beta)
  emit(paste0("serbin_", label, ".gamma"), s$gamma)
  if (label == "default") serbin_default <- s
}
if (!binomial) {
  # Slice 2c: R pprof's test.logis_fe on the R SerBIN fit (Bernoulli data only, as R's test).
  source(file.path(src, "test.logis_fe.R"))
  fit_object <- structure(list(
    char_list = list(Y.char = "y", Z.char = features, ProvID.char = "provider"),
    data_include = e[, c("y", features, "provider")],
    coefficient = list(gamma = as.vector(serbin_default$gamma), beta = as.vector(serbin_default$beta)),
    variance = list(gamma = rep(NA_real_, length(n_prov)))), class = "logis_fe")
  runs <- list(exact_two_sided = list("exact.poisbinom", "two.sided"),
               exact_greater = list("exact.poisbinom", "greater"),
               exact_less = list("exact.poisbinom", "less"),
               score_two_sided = list("score", "two.sided"))
  for (name in names(runs)) {
    r <- test.logis_fe(fit_object, test = runs[[name]][[1]], null = "median", alternative = runs[[name]][[2]])
    emit(paste0("r_tests_", name, ".z"), r$stat)
    emit(paste0("r_tests_", name, ".p"), r[["p value"]])
    emit(paste0("r_tests_", name, ".flag"), as.numeric(as.character(r$flag)))
  }
}
writeLines(out, file.path(case_dir, "r_logistic.txt"))
