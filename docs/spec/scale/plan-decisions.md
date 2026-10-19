# Scale-test plan: the decisions of §12

- Status: a memo for the maintainer's decision (round 90). Approval records D-50 (D-49 went to the
  production-validation fixes), and the round that records it amends [the plan](plan.md).
- Since the plan was drafted in round 80:
  - The production validation ([review](../../reviews/production-validation-2026-10.md)) ran at about 5 million
    rows and 60 to 142 features, wider than the plan's widest point and §2.3's envelope (p = 100).
  - It found pprof_spark about 4.7 times slower than pprof_py at 4.8 million rows and p = 57 (C4, OI-76), which
    the plan does not yet target.
  - Rounds 83 to 87 added budgets and options that the grid should exercise: block growth (L5),
    `maxStratumBytes` (C3) and composite row keys (C2).
  - The production fixes took the round numbers of §11.

| # | Decision | Recommendation |
|---|---|---|
| 1 | Approve the plan | Approve it as D-50, with the amendments below |
| 2 | Cluster budget | One worker type throughout; you state the largest number of workers. Below 32, C3 moves to P3 and strong scaling runs on 4, 8 and 16 workers |
| 3 | Tiers | Confirm them, with two production-shaped points in P1 (W4 at p = 150 in place of W3; X4 at C4's shape) and composite row keys at C2 |
| 4 | Three-stage SRR | A second wave, planned from Stage A's report and sized to measure stage 2's cost (OI-59) |
| 5 | Targets | Numbers from Stage A's report, as D-04 intended, with their form fixed now |
| 6 | Results | Aggregate tables under `docs/parity/scale/`, exported by the kit without paths or identifiers |

## 1. Approve the plan

**Recommendation: approve it as D-50, with the amendments below.** Nothing in the production findings argues
against the plan's synthetic data and measurement-only approach. Its guards (§3) already include the budgets
that rounds 83 to 87 introduced. The round that records D-50 makes these amendments:

- **§5:** add W4 and X4 (decision 3) and composite row keys at C2.
- **§8:** add OI-76 with this rule: attribute X4's time to phases. Where pprof_spark on 16 workers is slower
  than pprof_py on the driver, propose the remedy for whichever takes the largest share: OI-68 (rebuilt working
  sets), OI-70 (job overhead) or OI-38 (kernel sums).
- **§11 and the status line:** renumber the kit's rounds (see Next) and name D-50.

## 2. Cluster budget

**Recommendation: use one worker type throughout Stage A and Stage B, with a driver of the same type.**
Choose 8 or more cores, at least 32 GB and a local SSD for shuffle (for example the m6id or r6id families).
One type keeps every time comparable. The driver holds partials and provider tables but nothing as large as
the data.

What the plan needs from you is the largest number of workers you can use:

| Workers available | Consequence |
|---|---|
| 64 or more | The grid as planned: C2's strong scaling on 4, 16 and 64 workers; C3 in P2 |
| 32 to 63 | Strong scaling on 4, 16 and 32 workers; C3 stays in P2 |
| Fewer than 32 | Strong scaling on 4, 8 and 16 workers (or the largest three doublings available); C3 moves to P3 |

Your dedicated cluster serves if it can be resized to these shapes. Otherwise, use job clusters in dedicated
(single-user) access mode, one per shape: they also give the Classic session that §6's listener needs. The
kit's last round extrapolates node-hours per point from its local dry run, and you confirm P1 against that
estimate before Stage A.

## 3. Tiers

**Recommendation: confirm the tiers, with three changes that follow the production validation.**

- **W4 (P1): Cox and logistic at n = 10⁸, m = 10⁵, p = 150, in place of W3 (p = 100), which moves to P2.**
  Production reached 142 features. W4 tests block growth (L5) and the driver budget (OI-02) beyond §2.3's
  p = 100, and its result decides whether to widen §2.3.
- **X4 (P1): Cox and logistic at n = 5 × 10⁶, m = 5 × 10³, p = 60.** It runs on 1, 4 and 16 workers against
  pprof_py on the driver: the shape of finding C4, on synthetic data.
- **Composite row keys at C2 (Cox):** this measures the cost of the hash and its distinct count at 10⁸ rows.
  - Not at C3: synthetic data carry a row identifier.
  - At 10⁹ rows, a 64-bit hash collides with probability about 3% (n²/2⁶⁵), and validation would report that
    as a failure.

P1 then holds C1, C2, W4, S1, U1, L1 and X1 to X4.

## 4. The three-stage SRR model

**Recommendation: a second wave, as proposed, planned from Stage A's report.**

- The production validation found no three-stage issue.
- The model's cost drivers differ: stage 2's dense Schur complement (OI-59) grows with the number of levels,
  not rows.
- Its generator needs provider × cluster cells.

The second wave adds that generator and a few points sized to measure OI-59, on Stage A's cluster shapes.

## 5. Targets

**Recommendation: set the numbers from Stage A's report, as D-04 intended, but fix their form now.** That way
the numbers can follow from what the pipelines need, not only from what was measured. Each target is one of:

- The wall time of a fit and of each downstream operation at C2, on a stated cluster shape.
- The driver's peak heap, below a stated share of its memory.
- Every check of §3 passing.
- At X4, the time on 4 and 16 workers relative to pprof_py on the driver.

A feature that meets its targets in Stage B becomes scale-verified (§9).

## 6. Results

**Recommendation: aggregate tables under `docs/parity/scale/`, as proposed, which the kit exports in a form fit
for a public repository.**

- The export keeps the instance type, the number of workers, the Databricks Runtime build and the Spark
  settings.
- It leaves out workspace paths, storage locations, and cluster and workspace identifiers.
- The raw results table stays in your workspace.
- Data stay synthetic, as §2 requires.

## Next

On your decisions:

1. Round 91 records D-50 and amends the plan.
2. Rounds 92 to 94 build the kit:
   - generators;
   - `ScaleJob` with Python access;
   - the kernel benchmarks, `02_scale` extended to the grid, and a local dry run with node-hour estimates.
3. Stage A runs on your workspace.
