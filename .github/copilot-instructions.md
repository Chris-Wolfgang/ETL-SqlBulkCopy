# Copilot Coding Agent Instructions

## Repository Summary

`Wolfgang.Etl.SqlBulkCopy` is a **shipping .NET library** (published to NuGet) that
loads an `IAsyncEnumerable<TRecord>` into Microsoft SQL Server via `SqlBulkCopy`.
Schema, table and column mapping are derived from the record type's attributes, so
consumers write no manual `ColumnMappings`. It builds on
[Wolfgang.Etl.Abstractions](https://github.com/Chris-Wolfgang/ETL-Abstractions).

**Repository Type**: Library (ships one NuGet package)
**Package ID**: `Wolfgang.Etl.SqlBulkCopy`
**Target Frameworks**: `net462;net481;netstandard2.0;net8.0;net10.0`
**Primary Language**: C#
**Solution**: `ETL-SqlBulkCopy.slnx` (XML solution format — there is no `.sln`)

## Build and Validation Instructions

### Prerequisites

- **.NET 10 SDK or later.** Required — the projects target `net10.0`, and older
  SDKs cannot load the csproj.
- ReportGenerator (`dotnet tool install -g dotnet-reportgenerator-globaltool`)
- DevSkim CLI (`dotnet tool install --global Microsoft.CST.DevSkim.CLI`)

### Build Process

Always restore first. Build and test in **Release** — several analyzers
(`TreatWarningsAsErrors`) only run in Release, so a Debug build can pass while CI fails.

```powershell
dotnet restore
dotnet build --no-restore --configuration Release
dotnet test --configuration Release
```

Before pushing, run the local mirror of the PR pipeline:

```powershell
pwsh ./scripts/build-pr.ps1
```

Formatting is enforced (`dotnet format --verify-no-changes` in CI):

```powershell
pwsh ./scripts/format.ps1
```

## Repository Layout

| Path | Contents |
|---|---|
| `src/Wolfgang.Etl.SqlBulkCopy/` | The library. |
| `src/Wolfgang.Etl.SqlBulkCopy.SourceGenerator/` | Roslyn incremental generator emitting compile-time property accessors for `[BulkCopyable]` types. Packed **into the same NuGet package** as an analyzer — it is not shipped separately. |
| `tests/…Tests.Unit/` | Main unit suite; gates the coverage threshold. |
| `tests/…Tests.Integration/` | Testcontainers + real SQL Server. Runs on the **Linux** CI stage only (Windows/macOS runners can't run Linux containers). |
| `tests/…Tests.DocExamples/` | Compiles every XML-doc `<example><code>` block via Roslyn — guards against doc rot. |
| `tests/…Tests.Fuzz/` | CsCheck property-based tests (weekly schedule). |
| `tests/…Tests.Concurrency/` | Coyote systematic-concurrency tests (weekly schedule). |
| `tests/…AotSmoke/` | A `net10.0` **executable** (not a test project) published Native-AOT to prove the mapping path is AOT-clean. |
| `benchmarks/` | BenchmarkDotNet suite. |
| `samples/`, `examples/`, `tools/` | Supporting projects, not shipped. |
| `docfx_project/` | DocFX source for the published docs site. |
| `docs/` | Hand-written docs: ADRs, migration guides, workflow/security notes. **Not** generated output — DocFX emits to `docfx_project/_site`. |
| `scripts/` | `build-pr.ps1`, `format.ps1`, `Fix-BranchRuleset.ps1`, `Setup-Labels.ps1`, `Validate-DocsDeploy.sh`. |

## Code Standards

Repository-wide conventions (see `CONTRIBUTING.md` and the root `.editorconfig`):

- **Allman braces**, 4-space indent, file-scoped namespaces.
- **3 blank lines** between members (constructors, methods, properties).
- Multi-line argument lists put the opening paren on the same line, each argument
  indented on its own line, and the closing paren on its own line.
- Test names follow `MethodUnderTest_when_condition_expected_result`.
- **Async-only**: `BannedSymbols.txt` blocks synchronous I/O, `Task.Wait()`,
  `Task.Result` and `Thread.Sleep`. Every `await` in shippable code carries
  `ConfigureAwait(false)` — including `await foreach`, which `CA2007` does not flag.
- Public API changes must be reflected in `PublicAPI.Shipped.txt` /
  `PublicAPI.Unshipped.txt` or `PublicApiAnalyzers` fails the build (RS0016/RS0017).

Eight analyzer sets are active: the SDK's built-in **Microsoft.CodeAnalysis.NetAnalyzers**
plus seven `PackageReference`s in `Directory.Build.props` — Roslynator, AsyncFixer,
Microsoft.VisualStudio.Threading.Analyzers, Microsoft.CodeAnalysis.BannedApiAnalyzers,
Meziantou.Analyzer, SonarAnalyzer.CSharp, and Microsoft.CodeAnalysis.PublicApiAnalyzers.

## CI/CD

`.github/workflows/` holds **19** workflows. The ones that gate day-to-day work:

- **`pr.yaml`** — the main gate. Three sequential stages: Stage 1 Linux (tests +
  coverage gate + the Docker-backed integration tests), Stage 2 Windows (.NET
  Framework + modern TFMs), Stage 3 macOS. Also runs gitleaks, DevSkim, InspectCode
  and a **protected-configuration-file guard** that fails any PR touching
  `.editorconfig`, `Directory.Build.props`, `BannedSymbols.txt`, `*.globalconfig`,
  `*.ruleset` or `.github/workflows/*` — those need a separate maintainer-reviewed PR.
- **`release.yaml`** — triggered on `release: published`; packs, validates, publishes
  to NuGet via **Trusted Publishing (OIDC — no API-key secret)**, attests SLSA
  provenance, and calls `docfx.yaml`. It runs from the **tag's** commit, not `main`.
- Scheduled/specialised: `codeql`, `scorecard`, `stryker` (mutation), `fuzz`,
  `concurrency`, `benchmarks`, `pr-benchmarks`, `aot-smoke`, `sourcelink`,
  `reproducible-build`, `cross-platform-differential`, `license-audit`,
  `workflow-security`, `build-all-versions`, `gc-profile`, `shadow`, `docfx`.

### Branch flow

Work merges into a per-release-cycle **`vNext`** integration branch, and `vNext`
merges to `main` when the release is cut. Do not merge directly to `main`.

## Guidance for Agents

- **Verify before asserting.** These instructions can drift; the repository is the
  source of truth. If something here contradicts what you observe in the tree,
  trust the tree and fix this file.
- Build and test in **Release**, not Debug.
- Do not edit protected configuration files as part of an unrelated change — it
  trips the PR guard and forces an admin bypass.
- When changing the public surface, update the `PublicAPI.*.txt` files in the same
  commit.
- When adding an `<example>` to an XML doc, make sure it compiles — `Tests.DocExamples`
  compiles every one of them.
