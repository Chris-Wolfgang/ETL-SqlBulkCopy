# Introduction

Welcome to Wolfgang.Etl.SqlBulkCopy!

## Overview

A loader that uses SqlBulkCopy for fast inserts into a Microsoft SQL database.

It consumes an `IAsyncEnumerable<TRecord>` and streams it to SQL Server through
`SqlBulkCopy`, deriving the schema, table and column mapping from the record type
itself — so there is no manual `ColumnMappings` wiring to keep in sync. It is
built on [Wolfgang.Etl.Abstractions](https://github.com/Chris-Wolfgang/ETL-Abstractions),
so it composes with the rest of the ETL family.

## Key Features

- **Streaming bulk load** — consumes `IAsyncEnumerable<TRecord>` and writes in
  configurable batches, so memory stays flat regardless of source size.
- **Type-driven mapping** — `[Table]`, `[Column]` and `[NotMapped]` attributes
  drive the schema, table and column names.
- **Nested tables** — child collections are recursively written to their own
  tables inside the same bulk-copy session.
- **Pre/post actions** — built-in `TruncateTable` / `DeleteAllRecords`, plus
  custom-action delegates for schema-aware work.
- **Data validation** — opt in with `EnableDataValidation`; DataAnnotations
  failures either throw or skip per `ValidationFailureBehavior`, with
  `OnValidationFailed` / `OnNestedValidationFailed` callbacks.
- **Transactions** — an optional `SqlTransaction` participates in both the bulk
  load and the pre/post commands.
- **Dry run** — set `IsDryRun` to enumerate, map, validate and report with no
  SQL side effects.
- **Progress reporting** — `IProgress<SqlBulkCopyReport>` reports rows written
  (`CurrentItemCount`), rows skipped and batch count.
- **Native AOT ready** — mark a record `[BulkCopyable]` to get compile-time
  source-generated accessors instead of runtime IL emission.
- **Async-only** — a banned-symbol analyzer keeps the library free of
  synchronous fallbacks.

## Getting Help

If you need help with Wolfgang.Etl.SqlBulkCopy, please:

- Check the [Getting Started](getting-started.md) guide
- Review the [API Reference](../api/index.md)
- Visit the [GitHub repository](https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy)
- Open an issue on [GitHub Issues](https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/issues)
