# 0007. Derive AssemblyVersion from the minor version

- **Status:** Accepted (0.9.0)
- **Date:** 2026-09-22

## Context

[ADR-0005](0005-decouple-assemblyversion-from-package-version.md) kept
`AssemblyVersion` in step with the package `Version` for the 0.x line, so every
release, patch releases included, moved the assembly's binding identity. On
.NET Framework `AssemblyVersion` is part of bind-time identity, so a patch
release that changes it forces binding redirects on consumers for no
behavioural reason. Keeping it in step by hand also relied on each
release-prep PR remembering to edit it.

Before 1.0, SemVer puts breaking changes in the minor position. The ETL family
adopted a shared policy for this (ADR-0011 in ETL-Abstractions).

## Decision

`AssemblyVersion` is derived from `<Version>` in the csproj as
`0.{Minor}.0.0`. It moves on every minor release and stays fixed across patch
releases. `FileVersion` tracks the full `MAJOR.MINOR.PATCH.0` of each release.

## Consequences

- Patch releases keep the same binding identity, so they need no binding
  redirects. Minor releases, which may break pre-1.0, change it.
- A release-prep PR cannot forget the bump, because the value is computed.
- At 1.0 the formula needs a deliberate revisit (for example `MAJOR.0.0.0`),
  recorded as a new ADR that supersedes this one.
