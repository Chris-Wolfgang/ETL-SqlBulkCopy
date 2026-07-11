# Migration Guides

When a major-version release of `Wolfgang.Etl.SqlBulkCopy` contains breaking
changes, consumers need a written upgrade path: what changed, why, and the
before/after code to move from the previous major to the new one.

This folder holds one guide per major-version transition. The convention is
established **proactively** — the template exists before the first breaking
release so a guide is authored *during* that release's prep, not retrofitted
after consumers hit the break.

## Convention

- One file per transition, named `vX-to-vY.md` (e.g. `v0-to-v1.md`,
  `v1-to-v2.md`), created from [`TEMPLATE-major-version-migration.md`](TEMPLATE-major-version-migration.md).
- The guide is written as part of the release PR that introduces the breaking
  change — it is reviewed alongside the code, not bolted on later.
- The GitHub Release notes for the new major link to its guide.
- Breaking changes and their rationale should already be captured in an
  [ADR](../adr/index.md); the migration guide is the consumer-facing *how to
  upgrade*, the ADR is the *why we changed it*.

## Guides

_None yet — the package is at v0.1.0 with no breaking transition. The first
guide lands with the first major release that removes or changes public API._

## Relationship to SemVer

Per [ADR-0005](../adr/0005-decouple-assemblyversion-from-package-version.md),
the package is pre-1.0. A 0.x → 0.(x+1) bump may still carry breaking changes
(SemVer permits this in 0.x); when it does, it gets a guide here too. From 1.0
onward, only `MAJOR` bumps carry breaking changes and each gets a guide.
