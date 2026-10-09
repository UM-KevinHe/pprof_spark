# R references for the linear fixed-effect fixtures (linear_fixtures.py, D-45): stats::lm with a provider factor.
# Usage: Rscript linear_r.R <case_dir>; writes <case_dir>/r_linear.txt (quantity<TAB>hex values).
invisible(Sys.setlocale("LC_COLLATE", "C"))
args <- commandArgs(trailingOnly = TRUE)
d <- read.csv(file.path(args[1], "input.csv"), stringsAsFactors = FALSE)
d$provider <- factor(d$provider, levels = sort(unique(d$provider)))
f <- lm(y ~ 0 + provider + x1 + x2 + x3, data = d)
x <- c("x1", "x2", "x3"); m <- nlevels(d$provider); s <- summary(f); df <- f$df.residual
b <- coef(f)[x]; se <- sqrt(diag(vcov(f))[x]); V <- vcov(f)
packed <- function(A) unlist(lapply(seq_len(nrow(A)), function(i) A[i, i:ncol(A)]))
t <- b / se; tg <- (b - 0.25) / se; q <- qt(0.95, df)
X <- as.matrix(d[, x]); Xt <- X - apply(X, 2, function(v) ave(v, d$provider)); yt <- d$y - ave(d$y, d$provider)
out <- list(
  beta = b, gamma = coef(f)[seq_len(m)], sigma = sigma(f), rss = sum(residuals(f)^2), df = df,
  var_beta = packed(V[x, x]), var_gamma = diag(V)[seq_len(m)], var_gamma_simplified = sigma(f)^2 / as.numeric(table(d$provider)),
  loglik = as.numeric(logLik(f)), aic = AIC(f), bic = BIC(f),
  t = s$coefficients[x, 3], p = 2 * pt(abs(t), df, lower.tail = FALSE),
  ci_lower = confint(f, x, level = 0.95)[, 1], ci_upper = confint(f, x, level = 0.95)[, 2],
  greater_stat = tg, greater_p = pt(tg, df, lower.tail = FALSE), greater_lower = b - q * se,
  less_stat = tg, less_p = pt(tg, df), less_upper = b + q * se,
  predict = fitted(f)[1:min(100, nrow(d))], r2 = 1 - sum(residuals(f)^2) / sum((d$y - mean(d$y))^2),
  within_xx = packed(crossprod(Xt)), within_xy = as.numeric(crossprod(Xt, yt)))
lines <- vapply(names(out), function(k) paste0(k, "\t", paste(sprintf("%a", as.numeric(out[[k]])), collapse = ",")), "")
writeLines(lines, file.path(args[1], "r_linear.txt"))
