# Cox reference outputs from R survival::coxph for one fixture case (ADR-0005).
# Usage: Rscript cox_survival.R <case-dir> <p> <stratified> <weighted> <offset> [<truncated>]
# Writes <case-dir>/r_survival.txt: one line per quantity, "key<TAB>comma-separated %a values".
# Hexadecimal floating point (C's %a) is exact; Python and Java parse it back bit for bit.
# robust = FALSE throughout: with non-integer case weights coxph reports a robust (sandwich)
# variance by default, while pprof_py's default, and these fixtures, are model-based (X-009).
args <- commandArgs(trailingOnly = TRUE)
case_dir <- args[[1]]
p <- as.integer(args[[2]])
stratified <- args[[3]] == "true"
weighted <- args[[4]] == "true"
with_offset <- args[[5]] == "true"
truncated <- length(args) >= 6 && args[[6]] == "true"
suppressMessages(library(survival))

d <- read.csv(file.path(case_dir, "input.csv"))
rhs <- paste(paste0("x", seq_len(p)), collapse = " + ")
if (stratified) rhs <- paste(rhs, "+ strata(stratum)")
if (with_offset) rhs <- paste(rhs, "+ offset(offset)")
f <- as.formula(paste(if (truncated) "Surv(entry, time, event) ~" else "Surv(time, event) ~", rhs))
w <- if (weighted) d$weight else rep(1, nrow(d))
fixed_beta <- c(0.375, -0.1875, 0.0625)[seq_len(p)]

hexes <- function(v) paste(sprintf("%a", as.numeric(v)), collapse = ",")
packed <- function(m) {
  m <- as.matrix(m)
  unlist(lapply(seq_len(nrow(m)), function(i) m[i, i:ncol(m)]))
}
out <- character()
emit <- function(key, v) out <<- c(out, paste0(key, "\t", hexes(v)))

for (ties in c("breslow", "efron")) {
  for (label in c("default", "tight")) {
    control <- if (label == "tight") coxph.control(eps = 1e-11, iter.max = 100) else coxph.control()
    fit <- coxph(f, data = d, weights = w, ties = ties, robust = FALSE, control = control)
    key <- paste(ties, label, sep = ".")
    emit(paste0(key, ".coef"), coef(fit))
    emit(paste0(key, ".se"), sqrt(diag(vcov(fit))))
    emit(paste0(key, ".covariance"), packed(vcov(fit)))
    emit(paste0(key, ".loglik"), fit$loglik[2])
    emit(paste0(key, ".loglik_null"), fit$loglik[1])
    emit(paste0(key, ".iterations"), fit$iter)
    if (label == "tight") {
      # Cumulative hazard at x = 0 times exp(weighted mean offset), at every distinct time (X-014).
      bh <- basehaz(fit, centered = FALSE)
      emit(paste0(key, ".basehaz_time"), bh$time)
      emit(paste0(key, ".basehaz_hazard"), bh$hazard)
      if (stratified) emit(paste0(key, ".basehaz_stratum"), as.numeric(sub("^stratum=", "", as.character(bh$strata))))
    }
  }
  for (name in c("beta_zero", "beta_fixed")) {
    beta <- if (name == "beta_zero") rep(0, p) else fixed_beta
    fit0 <- suppressWarnings(coxph(f, data = d, weights = w, ties = ties, init = beta, robust = FALSE,
                                   control = coxph.control(iter.max = 0)))
    key <- paste(ties, name, sep = ".")
    emit(paste0(key, ".loglik"), fit0$loglik[1])
    emit(paste0(key, ".score"), colSums(as.matrix(residuals(fit0, type = "score")) * w))
    emit(paste0(key, ".information"), packed(solve(fit0$var)))
  }
}
writeLines(out, file.path(case_dir, "r_survival.txt"))
