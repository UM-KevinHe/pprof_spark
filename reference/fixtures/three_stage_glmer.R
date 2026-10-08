# Stage 2 of the three-stage model by lme4 (slice 2f-3 fixtures): glmer's optimum at a tight bobyqa
# setting and its deviance function at given points. Arguments: the prepared records (fac, hosp, y_adj,
# stage1_offset) as CSV, the points (sigma_fac, sigma_hosp, intercept as hexadecimal doubles, one per
# line), the output file. Factor levels are ordered as pprof_py orders them (numeric keys numerically).
suppressMessages(library(lme4))
args <- commandArgs(trailingOnly = TRUE)
d <- read.csv(args[[1]], colClasses = c(fac = "character", hosp = "character"))
order_levels <- function(x) {
  u <- unique(x)
  if (all(grepl("^-?[0-9]+$", u))) u[order(as.numeric(u))] else sort(u, method = "radix")
}
d$fac <- factor(d$fac, levels = order_levels(d$fac))
d$hosp <- factor(d$hosp, levels = order_levels(d$hosp))
f <- y_adj ~ 1 + (1 | fac) + (1 | hosp) + offset(stage1_offset)
fit <- suppressWarnings(glmer(f, family = binomial, data = d, nAGQ = 1,
                              control = glmerControl(optimizer = "bobyqa", optCtrl = list(rhoend = 1e-12, maxfun = 1e5))))
dev <- suppressWarnings(glmer(f, family = binomial, data = d, nAGQ = 1, devFunOnly = TRUE))
theta <- getME(fit, "theta")
re <- ranef(fit)
points <- lapply(readLines(args[[2]]), function(line) as.numeric(strsplit(line, ",")[[1]]))
y <- d$y_adj
saturated <- 2 * sum(ifelse(y > 0, y * log(y), 0) + ifelse(y < 1, (1 - y) * log(1 - y), 0))
hex <- function(v) paste(sprintf("%a", v), collapse = ",")
lines <- c(
  paste0("sigma\t", hex(c(theta[["fac.(Intercept)"]], theta[["hosp.(Intercept)"]]))),
  paste0("intercept\t", hex(fixef(fit)[[1]])),
  paste0("blup_providers\t", hex(re$fac[levels(d$fac), 1])),
  paste0("blup_clusters\t", hex(re$hosp[levels(d$hosp), 1])),
  paste0("devfun\t", hex(sapply(points, function(p) dev(p)))),
  paste0("saturated\t", hex(saturated)),
  paste0("provider_levels\t", paste(levels(d$fac), collapse = ",")),
  paste0("cluster_levels\t", paste(levels(d$hosp), collapse = ","))
)
writeLines(lines, args[[3]])
