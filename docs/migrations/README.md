# Migration Guides

When a release of `Wolfgang.Etl.SqlBulkCopy` contains **breaking changes**,
consumers need a written upgrade path: what changed, why, and the before/after
code to move from the previous version to the new one.

Under [SemVer](https://semver.org/) a breaking change means a `MAJOR` bump once
the package is at 1.0+, but during the pre-1.0 `0.x` line a `0.x → 0.(x+1)` bump
is also permitted to break (see
[ADR-0005](../adr/0005-decouple-assemblyversion-from-package-version.md)).
Either way, a breaking release gets a guide. This folder holds one guide per
breaking transition. The convention is established **proactively** — the
template exists before the first breaking release so a guide is authored
*during* that release's prep, not retrofitted after consumers hit the break.

## Convention

- One file per breaking transition, named `vX-to-vY.md` — e.g. `v0.1-to-v0.2.md`
  for a breaking pre-1.0 bump or `v1-to-v2.md` for a major — created from
  [`TEMPLATE-major-version-migration.md`](TEMPLATE-major-version-migration.md).
- The guide is written as part of the release PR that introduces the breaking
  change — it is reviewed alongside the code, not bolted on later.
- The GitHub Release notes for the breaking release link to its guide.
- Breaking changes and their rationale should already be captured in an
  [ADR](../adr/index.md); the migration guide is the consumer-facing *how to
  upgrade*, the ADR is the *why we changed it*.

## Guides

_None yet — no release has introduced a breaking change. The first guide lands
with the first release that removes or changes public API (a breaking `0.x`
bump or the eventual `1.0`), whichever comes first._
