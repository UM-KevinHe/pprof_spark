# R references for the linear provider inference fixtures (linear_fixtures.py, D-46): base R tails, quantiles and sums,
# computed as pprof_py defines each quantity, and R pprof 1.0.3's test.linear_fe and SM_output.linear_fe with the full
# variance (X-036). Usage: Rscript linear_inference_r.R <case_dir> <r_pprof_dir> <reduced: 0 or 1>;
# writes <case_dir>/r_inference.txt (quantity<TAB>hex values).
invisible(Sys.setlocale("LC_COLLATE", "C"))
args <- commandArgs(trailingOnly = TRUE)
reduced <- args[3] == "1"
suppressMessages({
  library(Matrix)
  for (f in c("linear_fe.R", "test.linear_fe.R", "SM_output.linear_fe.R")) source(file.path(args[2], f))
})
d <- read.csv(file.path(args[1], "input.csv"), stringsAsFactors = FALSE)
d$provider <- factor(d$provider, levels = sort(unique(d$provider)))
x <- c("x1", "x2", "x3")
f <- lm(y ~ 0 + provider + x1 + x2 + x3, data = d)
m <- nlevels(d$provider); n <- nrow(d); df <- f$df.residual
g <- unname(coef(f)[seq_len(m)]); se <- unname(sqrt(diag(vcov(f))[seq_len(m)]))
np <- as.numeric(table(d$provider))
xb <- as.numeric(as.matrix(d[, x]) %*% coef(f)[x])
out <- list(df = df, ref_median = median(g), ref_mean = sum(np * g) / n)
logtail <- function(t) pt(abs(t), df, lower.tail = FALSE, log.p = TRUE)
test <- function(name, ref, alt = "two_sided", critical = NA) {
  t <- (g - ref) / se
  z <- sign(t) * qnorm(logtail(t), lower.tail = FALSE, log.p = TRUE)
  cv <- if (!is.na(critical)) critical else if (alt == "two_sided") qnorm(0.975) else qnorm(0.95)
  q <- qt(pnorm(cv), df)
  if (alt == "two_sided") {
    p <- 2 * exp(logtail(t)); flag <- ifelse(z > cv, 1, ifelse(z < -cv, -1, 0)); lower <- g - q * se; upper <- g + q * se
  } else if (alt == "greater") {
    p <- pt(t, df, lower.tail = FALSE); flag <- ifelse(z > cv, 1, 0); lower <- g - q * se; upper <- rep(Inf, m)
  } else {
    p <- pt(t, df); flag <- ifelse(z < -cv, -1, 0); lower <- rep(-Inf, m); upper <- g + q * se
  }
  res <- list(z = z, p = p, flag = flag, lower = lower, upper = upper)
  for (k in names(res)) out[[paste0("test_", name, "_", k)]] <<- res[[k]]
}
test("two_sided", out$ref_median)
if (!reduced) {
  test("greater", out$ref_median, "greater"); test("less", out$ref_median, "less")
  test("mean", out$ref_mean); test("number", 0.25); test("critical", out$ref_median, critical = 2.5)
}
q2 <- qt(0.975, df)
out$gamma_lower <- g - q2 * se; out$gamma_upper <- g + q2 * se
sums <- function(v) as.numeric(tapply(v, d$provider, sum))
sm <- function(alt, ref) {
  q <- if (alt == "two_sided") q2 else qt(0.95, df)
  L <- if (alt == "less") rep(-Inf, m) else g - q * se
  U <- if (alt == "greater") rep(Inf, m) else g + q * se
  E <- sums(ref + xb); row <- as.integer(d$provider)
  out[[paste0("sm_", alt, "_indirect_lower")]] <<- (sums(L[row] + xb) - E) / np
  out[[paste0("sm_", alt, "_indirect_upper")]] <<- (sums(U[row] + xb) - E) / np
  out[[paste0("sm_", alt, "_direct_lower")]] <<- L - ref
  out[[paste0("sm_", alt, "_direct_upper")]] <<- U - ref
}
for (alt in if (reduced) "two_sided" else c("two_sided", "greater", "less")) sm(alt, out$ref_median)
measures <- function(name, ref) {
  E <- sums(ref + xb); O <- sums(d$y); obs <- sum(ref + xb); expected <- n * g + sum(xb)
  res <- list(indirect_difference = (O - E) / np, indirect_observed = O, indirect_expected = E,
              direct_difference = (expected - obs) / n, direct_observed = obs, direct_expected = expected)
  for (k in names(res)) out[[paste0("measures_", name, "_", k)]] <<- res[[k]]
}
measures("median", out$ref_median)
if (!reduced) measures("mean", out$ref_mean)
rf <- suppressMessages(linear_fe(data = data.frame(y = d$y, provider = d$provider, d[, x]), Y.char = "y", Z.char = x,
                                 ProvID.char = "provider", option.gamma.var = "full"))
rt <- test.linear_fe(rf, null = "median")
out$rpprof_stat <- rt$stat; out$rpprof_p <- rt$`p value`; out$rpprof_flag <- as.numeric(as.character(rt$flag))
rs <- SM_output.linear_fe(rf, stdz = c("indirect", "direct"), null = "median")
out$rpprof_indirect_difference <- rs$indirect.difference
out$rpprof_indirect_observed <- rs$OE$OE_indirect$Obs; out$rpprof_indirect_expected <- rs$OE$OE_indirect$Exp
out$rpprof_direct_difference <- rs$direct.difference; out$rpprof_direct_expected <- rs$OE$OE_direct$Exp
lines <- vapply(names(out), function(k) paste0(k, "\t", paste(sprintf("%a", as.numeric(out[[k]])), collapse = ",")), "")
writeLines(lines, file.path(args[1], "r_inference.txt"))
