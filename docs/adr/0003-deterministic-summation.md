# ADR-0003: Deterministic summation

- Status: Accepted (delegated; D-15)
- Date: 2026-10-05

## Context
§6.4 and §6.8 require blocked (pairwise) summation within a block, Neumaier compensation across
blocks, and reduction in block-identifier order, so that results are bitwise reproducible (NN-4,
level R0). The exact algorithms become part of every result's bits, so they are fixed here.

## Decision
- `Summation.pairwise`: cascade summation. A range of at most 32 values is summed sequentially,
  left to right, starting from its first value (an empty range sums to +0.0); a longer range is
  split at `from + n / 2` and the sums of the two halves are added.
- `NeumaierSum`, `Summation.neumaier` and `NeumaierVector`: Neumaier's compensated summation in
  the order given. Once the running sum is infinite or NaN, compensation stops and IEEE
  arithmetic decides the result.
- Callers fix the order: within a block by canonical row order (DIST-6), across blocks by block
  identifier (§6.8).
- Main code of `numerics` and `engine` computes transcendental functions with `StrictMath`;
  `scripts/check-engine-api.sh` enforces this (§8.1).

## Evidence (assistant sandbox, 2026-10-05; CI to confirm)
- An independent Python implementation, reference/numerics/summation_reference.py, reproduces
  the Scala results bit for bit on a 10,000-value data set spanning 2^-41 to 2^40; the bits are
  pinned in SummationSuite.
- On that set the Neumaier sum equals the exactly rounded sum (`math.fsum`). The pairwise error
  is 7.9e-4, 0.25% of its first-order bound (32 + depth) * u * sum|x|. Sequential summation and
  a 16-value leaf each give different bits (negative control).
- SummationSuite passes on OpenJDK 17.0.20 and 21.0.12 with scalac 2.13.16 and the build's flags.

## Consequences
- Changing the leaf size, the split rule or the compensation formula changes results. Such a
  change is behavioral: it needs R0 evidence and a negative control (NUM-2), and the pinned
  constants in SummationSuite fail until they are regenerated with the reference script.
- Neumaier costs about four extra floating-point operations per addend, so it is used where
  partials are combined across blocks, not in per-row inner loops.
