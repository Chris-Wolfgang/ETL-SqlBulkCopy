# 0001. Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-07-10

## Context

`Wolfgang.Etl.SqlBulkCopy` makes several design choices that are not obvious
from reading the code alone: an SDK is wrapped behind interfaces that look like
indirection-for-its-own-sake, synchronous BCL APIs are banned by an analyzer,
property access goes through compiled expression trees instead of plain
reflection, and the assembly version is deliberately decoupled from the package
version. Without a record of *why*, each of these reads as an accident a future
contributor might "clean up" and regress.

## Decision

We keep a log of Architecture Decision Records in `docs/adr/`, one immutable
markdown file per decision, using the lightweight MADR format described in
[README.md](README.md).

## Consequences

- Non-obvious decisions carry their rationale next to the code, versioned with
  it, reviewable in the PR that makes the decision.
- Reversing a decision is explicit: a new ADR supersedes the old one rather than
  the old record being silently edited, so the history of the reasoning
  survives.
- There is a small ongoing cost: a decision worth an ADR needs a few minutes to
  write one. The alternative — reconstructing intent from git archaeology — is
  more expensive and lossy.
