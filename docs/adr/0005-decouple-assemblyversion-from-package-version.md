# 0005. Decouple AssemblyVersion from the package version

- **Status:** Accepted
- **Date:** 2026-07-10

## Context

Three version numbers travel with a NuGet package: `Version` (the package
version, e.g. `0.1.0`, which consumers see and range against), `FileVersion`
(informational, shown in file properties), and `AssemblyVersion` (part of the
assembly's strong identity, used by the runtime for binding). During pre-1.0
development the package version changes on nearly every release, but the
*binding* identity ideally changes only on a genuinely binary-incompatible
release — otherwise every patch bump forces a binding-redirect churn on
consumers.

An earlier state of the csproj had `AssemblyVersion` pinned at a stale
`1.0.0` while `Version` was already `0.1.0`, which is simply inconsistent: it
advertises a 1.x binding identity for a 0.x package.

## Decision

For the 0.x pre-release line we keep `AssemblyVersion` and `FileVersion` in step
with `Version` (all three were `0.1.0` when this ADR was written, and move
together on each 0.x release). Once the library reaches 1.0 and stability
matters, `AssemblyVersion` may be intentionally held at `MAJOR.0.0.0` and moved
only on breaking releases, while `Version`/`FileVersion` continue to track each
release.

## Consequences

- The three versions agree, removing the stale-`1.0.0` inconsistency and
  making the assembly identity honestly reflect a 0.x package.
- The door is left open to the standard post-1.0 practice of pinning
  `AssemblyVersion` to the major, without committing to it while the API is
  still moving.
- Whoever cuts the 1.0 release must make the `AssemblyVersion` policy an
  explicit decision (a follow-up ADR), not an accident of the csproj.
