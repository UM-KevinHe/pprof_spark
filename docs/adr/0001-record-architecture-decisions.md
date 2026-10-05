# ADR-0001: Record architecture decisions

- Status: Accepted
- Date: 2026-10-03
- Decision: required by PROJECT_CONTEXT §0 and §11.6

## Context
PROJECT_CONTEXT requires architecture decisions, including every Phase 0 spike, to be recorded
as architecture decision records alongside the numbered register in DECISIONS.md.

## Decision
Architecture decisions are recorded in `docs/adr/NNNN-title.md` using `0000-template.md`,
numbered sequentially and never renumbered. Each ADR links its D-number and S-number, and an ADR
that replaces another marks the old one superseded instead of editing its decision.

## Evidence
Not applicable.

## Consequences
Every spike round ends with an ADR, and Phase 0 cannot close without one per spike.
