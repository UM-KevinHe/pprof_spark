# Evidence log: algorithmic feasibility study

> Committed in round 149 with the study (`algorithmic-feasibility-2026-11.md`), unchanged apart from trailing spaces.

Raw output of the experiments, 2026-11-13. Assistant sandbox: 1 vCPU (Intel Xeon 2.1 GHz, AVX-512), OpenJDK 21.0.12.1, Scala 2.13.17 and Spark 4.1.0 jars from PySpark 4.1.0, NumPy 2.5.4 with one BLAS thread, SciPy 1.18.1, numba 0.68.1.
pprof_spark at a28a224; pprof_py at 94b5476. Synthetic data only. Not run: a cluster, sbt, CI.

## The prototype Cox objective against pprof_py's kernel (feas.py check)

```
{"value": 4.46220845702038e-16, "score": 2.34134414568065e-14, "information": 1.7352348622209788e-15}
```

## feas.py logistic-simple 60000 96 180

```
{
 "model": "logistic",
 "design": "simple",
 "n": 60000,
 "m": 96,
 "p": 180,
 "newton": {
  "iterations": 4,
  "hessian_passes": 5,
  "dev_at_stop": 9.381458223866915e-15,
  "cond_information": 1.8991757092597479,
  "max_abs_z_truth": 2.2237016776846725,
  "inner_solves_per_pass": 1.75
 },
 "chord_H0": {
  "iterations": 7,
  "hessian_passes": 1,
  "gradient_passes": 7,
  "dev_at_stop": 2.5361196640544265e-09
 },
 "chord_H0_H1": {
  "iterations": 4,
  "hessian_passes": 2,
  "gradient_passes": 3,
  "dev_at_stop": 3.508458388112532e-10
 },
 "chord_H0_H2": {
  "iterations": 4,
  "hessian_passes": 2,
  "gradient_passes": 3,
  "dev_at_stop": 2.9034776082766005e-11
 },
 "lbfgs_raw": {
  "evaluations_run": 11,
  "to_1e-2": 6,
  "to_1e-4": 8,
  "to_1e-6": 10,
  "final_dev": 2.2509732486260102e-07
 },
 "lbfgs_standardized": {
  "evaluations_run": 31,
  "to_1e-2": 7,
  "to_1e-4": 11,
  "to_1e-6": 15,
  "final_dev": 7.941622351734446e-08
 },
 "newton_cg": {
  "outer_iterations": 5,
  "gradient_passes": 6,
  "hv_passes": 11,
  "dev_at_stop": 1.3004094493874094e-13
 },
 "pilot_then_newton": {
  "pilot_iterations": 4,
  "pilot_dev": 8.589896241397055,
  "full_iterations": 4,
  "full_hessian_passes": 5,
  "dev_after_step": [
   0.7051520241799807,
   0.00173751755478583,
   2.0451558827073325e-08
  ]
 },
 "divide_and_conquer": {
  "K": 30,
  "simple_average_dev": 1.5961093224296674,
  "information_weighted_dev": 0.47927294637843243
 },
 "surrogate_likelihood_one_round": {
  "dev": 0.18761002042444277
 },
 "one_step_from_weighted_average": {
  "dev": 0.0022951103537608656
 },
 "pprof_py_serbin": {
  "iterations": 5,
  "seconds": 0.9,
  "dev": 1.678129192364583e-14
 },
 "pprof_py_ban": {
  "iterations": 7,
  "seconds": 1.3,
  "dev": 2.372062549252414e-11
 },
 "seconds": 9.8
}
```

## feas.py logistic-real 60000 96 180

```
{
 "model": "logistic",
 "design": "real",
 "n": 60000,
 "m": 96,
 "p": 180,
 "newton": {
  "iterations": 4,
  "hessian_passes": 5,
  "dev_at_stop": 2.3740076445181965e-14,
  "cond_information": 365809.6445668526,
  "max_abs_z_truth": 2.5152538114872383,
  "inner_solves_per_pass": 2.125
 },
 "chord_H0": {
  "iterations": 10,
  "hessian_passes": 1,
  "gradient_passes": 10,
  "dev_at_stop": 1.2741438815005752e-08
 },
 "chord_H0_H1": {
  "iterations": 6,
  "hessian_passes": 2,
  "gradient_passes": 5,
  "dev_at_stop": 6.949166249610153e-11
 },
 "chord_H0_H2": {
  "iterations": 5,
  "hessian_passes": 2,
  "gradient_passes": 4,
  "dev_at_stop": 2.601142984034449e-10
 },
 "lbfgs_raw": {
  "evaluations_run": 501,
  "to_1e-2": null,
  "to_1e-4": null,
  "to_1e-6": null,
  "final_dev": 1.9267471858812228
 },
 "lbfgs_standardized": {
  "evaluations_run": 10,
  "to_1e-2": null,
  "to_1e-4": null,
  "to_1e-6": null,
  "final_dev": 5.480672609448557
 },
 "newton_cg": {
  "outer_iterations": 6,
  "gradient_passes": 7,
  "hv_passes": 57,
  "dev_at_stop": 1.3240586898184298e-14
 },
 "pilot_then_newton": {
  "pilot_iterations": 5,
  "pilot_dev": 7.624475070359325,
  "full_iterations": 5,
  "full_hessian_passes": 6,
  "dev_after_step": [
   0.7298601944864138,
   0.008212577418707874,
   1.2632458383474894e-06
  ]
 },
 "divide_and_conquer": {
  "K": 30,
  "simple_average_dev": 2.0848172793890227,
  "information_weighted_dev": 0.8013547141217856
 },
 "surrogate_likelihood_one_round": {
  "dev": 0.5902236272445156
 },
 "one_step_from_weighted_average": {
  "dev": 0.015302739780529909
 },
 "pprof_py_serbin": {
  "iterations": 5,
  "seconds": 0.9,
  "dev": 1.2755577909667577e-14
 },
 "pprof_py_ban": {
  "iterations": 8,
  "seconds": 1.5,
  "dev": 6.799424557135599e-11
 },
 "seconds": 23.0
}
```

## feas.py cox-simple 60000 96 180

```
{
 "model": "cox",
 "design": "simple",
 "n": 60000,
 "m": 96,
 "p": 180,
 "newton": {
  "iterations": 3,
  "hessian_passes": 4,
  "dev_at_stop": 1.3534230117264135e-11,
  "cond_information": 1.9963204413266256,
  "max_abs_z_truth": 2.2728543849125438
 },
 "events": 22248,
 "event_times": 19975,
 "chord_H0": {
  "iterations": 4,
  "hessian_passes": 1,
  "gradient_passes": 4,
  "dev_at_stop": 1.2110604885389535e-05
 },
 "chord_H0_H1": {
  "iterations": 3,
  "hessian_passes": 2,
  "gradient_passes": 2,
  "dev_at_stop": 6.229033408178066e-08
 },
 "chord_H0_H2": {
  "iterations": 4,
  "hessian_passes": 2,
  "gradient_passes": 3,
  "dev_at_stop": 7.2231798448098785e-12
 },
 "lbfgs_raw": {
  "evaluations_run": 46,
  "to_1e-2": 7,
  "to_1e-4": 9,
  "to_1e-6": 13,
  "final_dev": 7.877481997217818e-08
 },
 "lbfgs_standardized": {
  "evaluations_run": 30,
  "to_1e-2": 8,
  "to_1e-4": 12,
  "to_1e-6": 16,
  "final_dev": 1.2270898755802052e-06
 },
 "newton_cg": {
  "outer_iterations": 4,
  "gradient_passes": 5,
  "hv_passes": 8,
  "dev_at_stop": 8.066173293120526e-09
 },
 "pilot_then_newton": {
  "pilot_iterations": 3,
  "pilot_dev": 8.879750591957034,
  "full_iterations": 3,
  "full_hessian_passes": 4,
  "dev_after_step": [
   0.22913587271414274,
   0.00018079670839945967,
   1.0148171222860332e-10
  ]
 },
 "divide_and_conquer": {
  "K": 30,
  "simple_average_dev": 2.0882267982305462,
  "information_weighted_dev": 0.49825094820135046
 },
 "surrogate_likelihood_one_round": {
  "dev": 0.1578768370356085
 },
 "one_step_from_weighted_average": {
  "dev": 0.000617239392494144
 },
 "minibatch_sgd": {
  "dev_after_1_epochs": 1.6133327557326036,
  "dev_after_5_epochs": 1.4291460158603462,
  "dev_after_20_epochs": 1.2641285470299488
 },
 "seconds": 38.0
}
```

## feas.py cox-real 60000 96 180

```
{
 "model": "cox",
 "design": "real",
 "n": 60000,
 "m": 96,
 "p": 180,
 "newton": {
  "iterations": 5,
  "hessian_passes": 6,
  "dev_at_stop": 5.84470029104902e-11,
  "cond_information": 370377.0000088486,
  "max_abs_z_truth": 2.950996214574164
 },
 "events": 22943,
 "event_times": 20598,
 "chord_H0": {
  "iterations": 9,
  "hessian_passes": 1,
  "gradient_passes": 11,
  "dev_at_stop": 0.029625976046665715
 },
 "chord_H0_H1": {
  "iterations": 8,
  "hessian_passes": 2,
  "gradient_passes": 7,
  "dev_at_stop": 0.001140010921222371
 },
 "chord_H0_H2": {
  "iterations": 5,
  "hessian_passes": 3,
  "gradient_passes": 4,
  "dev_at_stop": 0.0012683784914629972
 },
 "lbfgs_raw": {
  "evaluations_run": 19,
  "to_1e-2": null,
  "to_1e-4": null,
  "to_1e-6": null,
  "final_dev": 33390576977785.3
 },
 "lbfgs_standardized": {
  "evaluations_run": 21,
  "to_1e-2": null,
  "to_1e-4": null,
  "to_1e-6": null,
  "final_dev": 3.13305842035973e+16
 },
 "newton_cg": {
  "outer_iterations": 6,
  "gradient_passes": 7,
  "hv_passes": 42,
  "dev_at_stop": 8.614932149946632e-08
 },
 "pilot_then_newton": {
  "pilot_iterations": 6,
  "pilot_dev": 8.42686933110372,
  "full_iterations": 4,
  "full_hessian_passes": 5,
  "dev_after_step": [
   1.0492816251543318,
   0.04774115791166964,
   7.184028065730378e-05
  ]
 },
 "divide_and_conquer": {
  "K": 30,
  "simple_average_dev": 2.2975829554664196,
  "information_weighted_dev": 0.8197175280417142
 },
 "surrogate_likelihood_one_round": {
  "dev": 0.7931778709980003
 },
 "one_step_from_weighted_average": {
  "dev": 0.06274433174408926
 },
 "minibatch_sgd": {
  "dev_after_1_epochs": 1.9319187079358975,
  "dev_after_5_epochs": 1.6542476332914422,
  "dev_after_20_epochs": 1.7172986025359416
 },
 "seconds": 36.1
}
```

The `lbfgs_standardized` entries above scaled the coordinates the wrong way (beta = b x sd); the corrected runs follow. `lbfgs_raw` is unaffected.

## lbfgs_fix.py: L-BFGS in standardized coordinates, and on (beta, gamma) jointly

```
logistic simple {"cond_information": 1.8991757092597479, "cond_standardized": 1.2423270672545061, "lbfgs_standardized": {"evaluations_run": 14, "to_1e-2": 5, "to_1e-4": 6, "to_1e-6": 8, "final_dev": 3.632316356372615e-07}, "lbfgs_joint_standardized": {"evaluations_run": 101, "to_1e-2": 27, "to_1e-4": 76, "to_1e-6": 100, "final_dev": 2.308037391135736e-07}}
logistic real {"cond_information": 365809.6445668526, "cond_standardized": 34.130514723865296, "lbfgs_standardized": {"evaluations_run": 49, "to_1e-2": 21, "to_1e-4": 32, "to_1e-6": 49, "final_dev": 7.34329731018786e-07}, "lbfgs_joint_standardized": {"evaluations_run": 266, "to_1e-2": 102, "to_1e-4": 172, "to_1e-6": 256, "final_dev": 1.7839585029883655e-06}}
cox simple {"cond_information": 1.9963204413266256, "cond_standardized": 1.3149500921836477, "lbfgs_standardized": {"evaluations_run": 37, "to_1e-2": 5, "to_1e-4": 7, "to_1e-6": 9, "final_dev": 8.611360755513441e-08}}
cox real {"cond_information": 370377.0000088486, "cond_standardized": 36.865001221706464, "lbfgs_standardized": {"evaluations_run": 65, "to_1e-2": 27, "to_1e-4": 42, "to_1e-6": null, "final_dev": 2.7642912880997903e-06}}
```

## grid_check.py: an unstratified Cox evaluation from per-time aggregates

```
{"n": 60000, "p": 180, "distinct_times": 1825, "bytes_to_driver_pass_1": 2657200, "bytes_for_S2_per_time_instead": 237834000, "value_rel_diff": 7.491575760052272e-16, "score_scaled_diff": 8.347881030167005e-15, "information_scaled_diff": 5.69562094456146e-15}
```

## GradBench: passes without p x p work, in the JVM

```
n=20000 p=180 java=21.0.12.1
cox: log-likelihood and score only                 14.9 ms    0.75 us/row
cox: score and one Hessian-vector product          32.0 ms    1.60 us/row
logistic: profile likelihood and gradient          13.1 ms    0.65 us/row
  inner Newton steps per provider, from the common start: 5.8
n=60000 p=50 java=21.0.12.1
cox: log-likelihood and score only                 13.4 ms    0.22 us/row
cox: score and one Hessian-vector product          63.2 ms    1.05 us/row
logistic: profile likelihood and gradient          15.2 ms    0.25 us/row
  inner Newton steps per provider, from the common start: 4.8
n=100000 p=20 java=21.0.12.1
cox: log-likelihood and score only                 11.0 ms    0.11 us/row
cox: score and one Hessian-vector product          30.9 ms    0.31 us/row
logistic: profile likelihood and gradient          14.8 ms    0.15 us/row
  inner Newton steps per provider, from the common start: 4.4
```

## KernelBench (fresh JVM per chunk size): information passes, in the JVM

```
n=20000 p=180 groups=32 perGroup=625 events=3669 distinctEventTimes=3503 java=21.0.12.1
cox production (Neumaier)             1029.1 ms     51.46 us/row
cox same sweep, plain sums             134.3 ms      6.72 us/row  x7.7  dInfo 4.0e-15 dScore 2.5e-15 dValue 5.1e-16
cox weighted Gram, chunk 256 + Neumaier     107.6 ms      5.38 us/row  x9.6  dInfo 3.6e-16 dScore 7.8e-17 dValue 0.0e+00
logistic pass A production             598.9 ms     29.94 us/row
logistic pass B production              54.1 ms      2.71 us/row
logistic pass A, chunk 256 + Neumaier      96.9 ms      4.85 us/row  x6.2  dS 1.4e-16 dRhs 0.0e+00 dLoglik 0.0e+00
summary co-moments (Moments.centred)     648.7 ms     32.43 us/row
n=60000 p=50 groups=96 perGroup=625 events=15638 distinctEventTimes=14559 java=21.0.12.1
cox production (Neumaier)              280.4 ms      4.67 us/row
cox same sweep, plain sums              51.0 ms      0.85 us/row  x5.5  dInfo 6.9e-15 dScore 7.0e-15 dValue 2.9e-15
cox weighted Gram, chunk 256 + Neumaier      46.3 ms      0.77 us/row  x6.1  dInfo 1.7e-16 dScore 9.2e-17 dValue 0.0e+00
logistic pass A production             159.1 ms      2.65 us/row
logistic pass B production              49.2 ms      0.82 us/row
logistic pass A, chunk 256 + Neumaier      46.5 ms      0.78 us/row  x3.4  dS 1.1e-16 dRhs 0.0e+00 dLoglik 0.0e+00
summary co-moments (Moments.centred)     153.8 ms      2.56 us/row
```

## HybridBench: the weighted-Gram prototype on the engine's own blocks

```
n=60000 m=96 p=180 shufflePartitions=8 inputPartitions=4 master=local[1] java=21.0.12.1
  prototype against the engine: log-likelihood (relative)            0.000
  prototype against the engine: score (scaled)                   1.50e-16
  prototype against the engine: information (scaled)             1.29e-16
  today: Dataset cache, engine kernel, partials collected            3.474
  Dataset cache, prototype kernel, partials collected                0.904
  RDD object cache, prototype kernel, treeAggregate                  0.651
  blocks                                                            22.000
```

## bound_check.py: providers at the bound (tolerance 1e-12)

```
{
 "n": 30000,
 "m": 600,
 "p": 10,
 "providers_without_events": 73,
 "providers_with_only_events": 0,
 "serbin_iterations": 13,
 "zero_event_gamma_minus_(median-bound)": 0.0,
 "at_serbin_estimate": {
  "max_abs_score_beta": 0.00040604540992385913,
  "max_abs_reduced_rhs": 2.4755750834687396e-14,
  "max_abs_score_gamma_interior": 1.8318679906315083e-15
 },
 "profile_A": {
  "outer_iterations": 5,
  "max_abs_beta_diff": 1.3693712877516084e-06,
  "max_diff_in_se": 2.2271226667976436e-05,
  "max_abs_gamma_diff": 7.534802626274484e-07
 },
 "profile_B": {
  "outer_iterations": 5,
  "max_abs_beta_diff": 9.020562075079397e-17,
  "max_diff_in_se": 1.830308359031933e-15,
  "max_abs_gamma_diff": 8.881784197001252e-16
 },
 "tolerance_class_T_coef": "atol 1e-10, rtol 1e-8"
}
```

## bound_check.py at pprof_py's default tolerance (1e-8)

```
{
 "n": 30000,
 "m": 600,
 "p": 10,
 "providers_without_events": 73,
 "providers_with_only_events": 0,
 "serbin_iterations": 8,
 "zero_event_gamma_minus_(median-bound)": 2.366568749446758,
 "at_serbin_estimate": {
  "max_abs_score_beta": 0.003701834758295665,
  "max_abs_reduced_rhs": 4.978539340735368e-07,
  "max_abs_score_gamma_interior": 2.525757381022231e-15
 },
 "profile_A": {
  "outer_iterations": 4,
  "max_abs_beta_diff": 1.3678345816559023e-06,
  "max_diff_in_se": 2.2246655615765284e-05,
  "max_abs_gamma_diff": 2.3665685002576833
 },
 "profile_B": {
  "outer_iterations": 4,
  "max_abs_beta_diff": 1.536702598503581e-09,
  "max_diff_in_se": 2.6166038534478888e-08,
  "max_abs_gamma_diff": 2.366568749716702
 },
 "tolerance_class_T_coef": "atol 1e-10, rtol 1e-8"
}
```
