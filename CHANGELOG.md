# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

## [0.1.0] - 2026-07-09

Initial release.

### Added

- `SqlBulkCopyLoader<T>` — a `LoaderBase<T, SqlBulkCopyReport>` implementation that streams `IAsyncEnumerable<T>` into SQL Server via `Microsoft.Data.SqlClient.SqlBulkCopy`. Multi-targeted for `net462`, `net481`, `netstandard2.0`, `net8.0`, and `net10.0`.
- Type-driven column mapping via `TypeMap` / `ColumnMap` — reads `[Table]` / `[Column]` / `[NotMapped]` attributes; ignores properties without a public getter.
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

[Unreleased]: https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/releases/tag/v0.1.0
