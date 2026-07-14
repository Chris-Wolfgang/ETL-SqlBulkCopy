# 0002. Isolate the SqlBulkCopy SDK behind wrapper interfaces

- **Status:** Accepted
- **Date:** 2026-07-10

## Context

The loader's real work is done by two `Microsoft.Data.SqlClient` types:
`SqlBulkCopy` (the batched row writer) and `SqlCommand` (the pre/post-action
SQL executor, e.g. `TRUNCATE TABLE`). Both are sealed, both require a live
`SqlConnection`, and neither can be constructed or exercised in a unit test
without a real SQL Server. Left un-abstracted, every test of the loader's
orchestration — batch sizing, pre/post-action selection, progress reporting,
error paths — would need Testcontainers and a running database, pushing that
logic entirely into the (slow, Docker-dependent) integration suite.

`Microsoft.Data.SqlClient` also does not initialize on every runner: its
`SqlPerformanceCounters` static constructor depends on Windows-only performance
counters, so merely constructing a `SqlConnection` throws on Linux CI and on
locked-down Windows agents.

## Decision

We define narrow internal interfaces —
`ISqlBulkCopyWrapper` / `ISqlBulkCopyWrapperFactory` for the row writer and
`ISqlCommandExecutor` for the SQL command path — and depend on those from
`SqlBulkCopyLoader<T>`. The production implementations
(`SqlBulkCopyWrapper`, `SqlConnectionCommandExecutor`) are thin one-line
pass-throughs to the SDK types; the unit tests supply fakes
(`FakeSqlBulkCopyWrapperFactory`, `FakeSqlCommandExecutor`).

## Consequences

- The loader's orchestration is unit-testable without a database: pre/post
  action dispatch, batch chunking, and progress reporting are all covered by
  fast in-memory tests.
- The only code that must run against real SQL Server is the thin wrapper
  layer, which is marked `[ExcludeFromCodeCoverage]` (a unit test of a
  one-line SDK pass-through would prove the test double, not the code) and is
  exercised end-to-end by the integration suite instead.
- One extra layer of indirection exists that would be unnecessary if the SDK
  types were mockable. This is a deliberate trade of a little indirection for a
  large, fast test surface — see also the constructor-probe skip pattern the
  tests use to tolerate `SqlConnection` failing to initialize on some runners.
