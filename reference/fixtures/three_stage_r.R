# Stage 1 of the three-stage model by glm (slice 2f-1 fixtures): fixed effects for the included cells.
# Arguments: the included records (cell, Y, features) as CSV, the feature names, the output file.
args <- commandArgs(trailingOnly = TRUE)
d <- read.csv(args[[1]], colClasses = c(cell = "character"))
features <- strsplit(args[[2]], ",")[[1]]
d$cell <- factor(d$cell)
f <- as.formula(paste("Y ~ 0 + cell +", paste(features, collapse = " + ")))
fit <- glm(f, family = binomial(), data = d, control = glm.control(epsilon = 1e-15, maxit = 200))
writeLines(paste(sprintf("%a", unname(coef(fit)[features])), collapse = ","), args[[3]])
