# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.0] - 2026-08-14

### Changed

- Adopted **ETL core 0.22.0** — `Wolfgang.Etl.Abstractions` 0.21.0 -> 0.22.0, along with the
  test-only `Wolfgang.Etl.TestKit` / `Wolfgang.Etl.TestKit.Xunit` references. 0.22.0 is the release in
  which the TestKit packages were folded into the ETL-Abstractions repository and now build and ship
  from there. The public API of all four core packages is unchanged.
- Inherited from Abstractions 0.22.0: the `await foreach` sites in `ExtractorBase` and
  `TransformerBase` now use `ConfigureAwait(false)`, removing a sync-over-async deadlock risk for
  consumers on the `net462` and `netstandard2.0` targets that drive the pipeline from a
  synchronization context. No behavioural change on the modern targets.

## [0.6.0] - 2026-08-12

### Fixed

- **Source generator: `[BulkCopyable]` on a type with a non-public property getter
  produced a package that threw on first use.** The descriptor filter accepted any
  readable property, while the accessor emitter additionally required the getter be
  reachable from generated code. A `private`/`protected` getter therefore landed in
  the generated descriptor as a column with **no registered accessor**, and the first
  load threw `InvalidOperationException: No source-generated getter is registered for
  '<Type>.<Property>'. This indicates a source-generator defect.` Opting a working
  type into `[BulkCopyable]` turned it into a hard runtime failure. Such a type now
  falls back to the reflection map — the same path every other ineligible type
  already takes — and the mapping is unchanged.
- **Source generator: two types whose names differed only in `.` vs `_` silently
  disabled generation for the entire compilation.** The identifier-mangling step
  replaced every non-alphanumeric character with `_`, so `Ns.A_B` and `Ns_A.B`
  collapsed to the same name. The generator then added two sources under the same
  hint name and threw, surfacing as
  `warning CS8785: Generator 'BulkCopyAccessorGenerator' failed to generate source`.
  Because a generator that throws contributes **nothing**, *every* `[BulkCopyable]`
  type in that compilation quietly lost its generated accessors and descriptor and
  fell back to reflection — no error, just the AOT-clean path disappearing. Mangled
  names now carry a stable hash suffix. (The hash is FNV-1a rather than
  `string.GetHashCode()`, which is randomized per process on .NET Core and would make
  generated output differ between builds.)
- **Source generator: the same accessor gap applied to nested-table properties.** A
  nested collection property with a non-public getter produced a nested descriptor
  entry with no registered accessor, failing the same way through `NestedTableMap`.
- `ColumnMap`'s descriptor constructor validated nothing, so a null `clrType` slipped
  past the field assignments and surfaced as a `NullReferenceException` from a
  half-constructed instance. Arguments are now validated first and name the offending
  parameter.

### Changed

- **Raised the minimum `Wolfgang.Etl.Abstractions` version from 0.16.0 to 0.21.0.**
  Consumers that pin `Wolfgang.Etl.Abstractions` directly will be moved forward five
  minor versions by this upgrade. Because Abstractions is still pre-1.0, its minor
  releases are permitted to contain breaking changes — review its changelog for
  0.17.0 through 0.21.0 before upgrading if you use those types directly. This
  package's own public API is unchanged.
- Raised the minimum `Microsoft.Bcl.AsyncInterfaces` and
  `Microsoft.Extensions.Logging.Abstractions` versions from 10.0.10 to 10.0.11.
- **Performance (per-row hot path):** `IsDBNull` no longer re-invokes the compiled
  property getter for a cell that `GetValue` already read. `SqlBulkCopy` calls both
  for every nullable column of every row, so the getter ran twice per cell — and value
  types boxed twice. A single-cell memo keyed on `(row, ordinal)` removes the duplicate
  call; advancing the row invalidates it implicitly.
- **Performance:** `TypeMap.QualifiedTableName` is built once during construction
  instead of on every access. It was allocating two escaped identifier strings plus an
  interpolation each time, and is read at least once per batch and again per nested
  batch. `TypeMap` is immutable and cached, so the value cannot go stale.
- Nested-table mapping and duplicate-column detection no longer allocate an
  anonymous-type instance per candidate property, nor a full grouping structure to
  answer a yes/no question. Behaviour, resulting maps and error messages are unchanged.

### Documentation

- Replaced unfilled template scaffolding on the published documentation site — the
  docs landing page was rendering the literal text `{{PROJECT_NAME}}`, and the Getting
  Started and Introduction pages contained placeholder comments instead of content.
- Corrected the README and CONTRIBUTING analyzer count (7 → 8; the list omitted
  `Microsoft.CodeAnalysis.PublicApiAnalyzers`), the build prerequisite (.NET 8.0 SDK →
  .NET 10.0 SDK, with an explanation of why the SDK must be newer than the oldest
  supported target), two broken documentation links, and the `format.ps1` path.
- Corrected the progress-reporting description: `SqlBulkCopyReport` exposes rows
  written, rows skipped and batch count. It does **not** currently populate elapsed
  time, despite previously being documented as doing so.
- Documented the data-validation feature (`EnableDataValidation`,
  `ValidationFailureBehavior`, `OnValidationFailed`, `OnNestedValidationFailed`),
  which shipped without a README entry.
- Corrected the release-setup guide, which still described configuring a
  `NUGET_API_KEY` secret that the pipeline no longer uses (publishing is via NuGet
  Trusted Publishing / OIDC), the reproducible-build guide, and the ADR index.
- Corrected several XML doc comments whose stated behaviour had diverged from the
  code, including exception contracts on `GeneratedTypeDescriptor`,
  `TypeMapReader.GetOrdinal` and the loader's internal constructor.

### Internal

- Mutation-testing score raised to **87.22 %** with the regression gate enabled
  (`break` threshold 80); accept-category mutants are excluded via configuration and
  documented in `docs/mutation-testing-triage.md`.
- Added snapshot tests (Verify) covering generated SQL identifier quoting — including
  the `]]`-escaping of a bracket inside an identifier — and exception-message
  formatting under a non-invariant culture.
- Integration tests now run on the Linux CI stage against a real SQL Server container
  and fail loudly when Docker is unavailable there, instead of being scheduled onto
  runners that cannot host Linux containers.
- The release pipeline now runs the same secret and static-security scans as pull
  requests before publishing, and the per-PR benchmark workflow reports real results
  (its filter previously matched nothing).


## [0.5.0] - 2026-08-09

### Added

- **Native AOT support via source-generated accessors.** Mark a record
  `[BulkCopyable]` to have the bundled source generator emit its property getters
  and enum→underlying converters at compile time; the loader prefers them over
  the runtime `Expression.Compile` getter, keeping the marked type's hot path
  free of runtime IL emission (`net5.0`+). When the type and its entire
  nested-table graph are `[BulkCopyable]` and eligible, the generator
  additionally emits a full compile-time type descriptor — including any nested
  tables — so the runtime builds the entire column map with **no** reflection
  over the type; the reflection path remains the fallback and produces
  the identical map (guarded by a conformance test). Opt-in and additive —
  unmarked types are unchanged, and the generator ships inside the existing
  package (no second NuGet). New public API: `BulkCopyableAttribute`,
  `GeneratedAccessorRegistry`, and the `GeneratedColumnDescriptor` /
  `GeneratedNestedTableDescriptor` / `GeneratedTypeDescriptor` /
  `GeneratedTypeMapRegistry` descriptor infrastructure the generated code
  registers. See
  [ADR 0006](docs/adr/0006-source-generated-property-accessors.md).
- **Dry-run support (`ISupportDryRun`).** Set `IsDryRun = true` on
  `SqlBulkCopyLoader<TRecord>` to validate a pipeline against real data without
  writing to SQL Server: the loader still enumerates the source, applies
  `SkipItemCount` / `MaximumItemCount`, runs validation, increments progress, and
  logs — but performs no SQL side effects (the pre-/post-action and the bulk
  insert are all skipped). New public API:
  `SqlBulkCopyLoader<TRecord>.IsDryRun`. ([#121](https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/issues/121))

### Changed

### Deprecated

### Removed

### Fixed

### Security

- **SLSA build-provenance attestation on release.** Every published `.nupkg` /
  `.snupkg` now carries a Sigstore keyless build-provenance attestation bound to
  this repo's GitHub OIDC identity, verifiable with `gh attestation verify` —
  proving the package was built by the release workflow at a specific commit and
  not altered afterward. No code-signing certificate required. See `SECURITY.md`.

## [0.4.0] - 2026-07-16

CI/CD hardening, test-quality tooling, and docs. **No change to the shipped
library API or behavior** — the compiled assembly and its dependencies are
identical to 0.3.0. Everything here is repo infrastructure and internal test
strengthening.

### Added

- Benchmark-result charts published to the docs site: the `benchmarks.yaml`
  workflow runs the BenchmarkDotNet project on each push to `main` and renders
  an interactive trend at `/dev/bench/` on gh-pages.
- `REPRODUCIBLE-BUILD.md` — a consumer guide to the deterministic-build
  guarantee and how to independently verify a released assembly.
- OpenSSF Scorecard badge in the README plus a documented supply-chain score
  floor in `SECURITY.md`.

### Changed

- `workflow-security.yaml` now **gates** PRs on new high-severity zizmor
  findings (previously advisory-only), with a non-empty-SARIF guard against
  false greens; accepted findings are suppressed in `.zizmor.yml`.
- Test-suite strengthening: additional boundary/guard tests raised the Stryker
  mutation score (67.96% → 70.63%), and one dead test helper was removed.

## [0.3.0] - 2026-07-15

Test-quality and performance-engineering tooling. **No change to the shipped
library API or behavior** — the compiled assembly is functionally identical to
0.2.0 (aside from an added `InternalsVisibleTo` for the benchmark project). This
release adds the internal benchmark/sample harnesses and mutation-testing
infrastructure that guard the library's performance and test rigor.

### Added

- `benchmarks/Wolfgang.Etl.SqlBulkCopy.Benchmarks` — a BenchmarkDotNet baseline
  project covering the compiled property getters, the end-to-end load path (via
  an in-process no-op wrapper), and the `SliceList` batching fast-path.
- `samples/Wolfgang.Etl.SqlBulkCopy.ShadowWorkloads` — realistic shadow
  workloads (bulk load, validation, and `PreAction.TruncateTable`) that exercise
  the loader against a real SQL Server via Testcontainers; doubles as usage
  documentation.
- Stryker.NET mutation-testing configuration (`stryker-config.json`), enabling
  the canonical weekly mutation run; baseline mutation score recorded and
  surviving mutants triaged.
- Nightly shadow-workload perf-regression workflow (`shadow.yaml`) that replays
  the sample workloads, publishes a trend to gh-pages under `dev/shadow/`, and
  fails + files an issue on a latency regression beyond threshold.
- Code-coverage trend graph published to the docs site under `/coverage/`.

### Changed

- Dropped the redundant `_Async` suffix from test method names across the unit
  and integration suites for naming consistency (test-only; no shipped change).

## [0.2.0] - 2026-07-13

Documentation, examples, and supply-chain tooling. **No change to the shipped
library API or behavior** — the compiled assembly is functionally identical to
0.1.0; this release adds consumer-facing docs/samples and hardens the build's
security posture.

### Added

- `examples/BulkLoadQuickstart` — a runnable end-to-end consumer sample:
  attribute-driven column mapping, `BatchSize` / `BulkCopyTimeout`,
  `PreAction.TruncateTable`, and `LoadAsync(source, progress)` with live
  per-batch progress reporting.
- Architecture Decision Records under `docs/adr/` capturing the non-obvious
  design decisions (SDK-wrapper isolation, async-only enforcement, compiled
  property getters + the Native-AOT limitation, AssemblyVersion policy).
- Migration-guide convention + template under `docs/migrations/`, established
  proactively so the first breaking release ships a guide written during its
  own PR.

### Security

- Supply-chain and build-integrity CI: OSSF Scorecard analysis, transitive
  dependency license audit, GitHub Actions workflow linting (actionlint +
  zizmor), and cross-runner build-reproducibility verification. The release
  pipeline already emits a CycloneDX SBOM per package.

## [0.1.0] - 2026-07-09

Initial release.

### Added

- `SqlBulkCopyLoader<T>` — a `LoaderBase<T, SqlBulkCopyReport>` implementation that streams `IAsyncEnumerable<T>` into SQL Server via `Microsoft.Data.SqlClient.SqlBulkCopy`. Multi-targeted for `net462`, `net481`, `netstandard2.0`, `net8.0`, and `net10.0`.
- Type-driven column mapping via `TypeMap` / `ColumnMap` — reads `[Table]` / `[Column]` / `[NotMapped]` attributes; ignores properties without a getter.
- Nested-table support via `NestedTableMap` — recursively writes child collections to their own tables inside the same bulk-copy session.
- Pre/post-load actions:
  - `PreAction` — `None`, `DeleteAllRecords`, `TruncateTable`, `CustomAction`.
  - `PostAction` — `None`, `CustomAction`.
  - `PreLoadActionParameters` / `PostLoadActionParameters` hand the custom-action delegate the connection, transaction, schema/table names, timeout, column list, and logger.
- Progress reporting via `SqlBulkCopyReport` (extends the Abstractions `ProgressReport`) — batch count, rows written, elapsed time.
- Constructors:
  - `SqlBulkCopyLoader(SqlConnection)`
  - `SqlBulkCopyLoader(SqlConnection, ILogger<SqlBulkCopyLoader<T>>)`
  - `SqlBulkCopyLoader(SqlConnection, SqlBulkCopyOptions, SqlTransaction?, ILogger<SqlBulkCopyLoader<T>>? = null)`
- `SqlBulkCopyValidationException` for column-map / type-map validation failures.
- Async-only I/O — banned-symbol analyzer enforces `WriteToServerAsync` / `ExecuteNonQueryAsync`; no sync fallbacks.

[Unreleased]: https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/releases/tag/v0.1.0
