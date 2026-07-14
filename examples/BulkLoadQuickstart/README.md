# BulkLoadQuickstart

A minimal, runnable consumer of `SqlBulkCopyLoader<T>`: it streams a
variable-size workload into SQL Server with batching, a truncate-before-load
pre-action, and live per-batch progress reporting.

It exercises the loader's core public surface — attribute-driven column
mapping (`[Table]` / `[Column]` / `[NotMapped]`), `BatchSize`,
`BulkCopyTimeout`, `PreAction`, and `LoadAsync(source, progress)` — in one
realistic flow, and doubles as the workload for the shadow-test harness tracked
in [#82](https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/issues/82).

## Prerequisites

- .NET 10 SDK
- A reachable SQL Server (LocalDB, a container, or a real instance) containing:

  ```sql
  CREATE TABLE dbo.Customers
  (
      Id       INT            NOT NULL,
      FullName NVARCHAR(200)  NOT NULL,
      Balance  DECIMAL(18, 2) NOT NULL
  );
  ```

## Run

```bash
# The connection string is read from the environment — no secrets in source.
export SQLBULKCOPY_SAMPLE_CONNECTION="Server=.;Database=Sandbox;Integrated Security=True;Encrypt=True;TrustServerCertificate=True;"

dotnet run -c Release --project examples/BulkLoadQuickstart
```

Expected output:

```
Loading customers...
  batch 1: 10000 rows written
  batch 2: 20000 rows written
  ...
Done.
```

If `SQLBULKCOPY_SAMPLE_CONNECTION` is unset the sample prints setup instructions
and exits without touching a database.
