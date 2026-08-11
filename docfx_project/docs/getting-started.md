# Getting Started

This guide will help you quickly get up and running with Wolfgang.Etl.SqlBulkCopy.

## Prerequisites

- **.NET** — the package targets `net462`, `net481`, `netstandard2.0`, `net8.0`
  and `net10.0`, so any consumer on .NET Framework 4.6.2+ or .NET 8+ is supported.
- **A reachable Microsoft SQL Server** and a destination table whose columns match
  the record type you intend to load.

## Installation

### Via NuGet Package Manager

```bash
dotnet add package Wolfgang.Etl.SqlBulkCopy
```

### Via Package Manager Console

```powershell
Install-Package Wolfgang.Etl.SqlBulkCopy
```

## Quick Start

Describe the destination with attributes, then hand the loader an
`IAsyncEnumerable<TRecord>`:

```csharp
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading;
using Microsoft.Data.SqlClient;
using Wolfgang.Etl.SqlBulkCopy;

[Table("Customers", Schema = "dbo")]
public sealed record Customer
{
    public int Id { get; init; }

    public string Name { get; init; } = string.Empty;

    public decimal Balance { get; init; }
}

async IAsyncEnumerable<Customer> ReadSourceAsync()
{
    // your source: file, API, another database, etc.
    yield return new Customer { Id = 1, Name = "Acme", Balance = 100m };
    yield return new Customer { Id = 2, Name = "Contoso", Balance = 250m };
}

using var connection = new SqlConnection("Server=.;Database=Sandbox;Integrated Security=True;Encrypt=True;");
await connection.OpenAsync();

var loader = new SqlBulkCopyLoader<Customer>(connection)
{
    BatchSize = 10_000,
    PreAction = PreAction.TruncateTable
};

await loader.LoadAsync(ReadSourceAsync(), CancellationToken.None);
```

## Common Tasks

### Report progress

```csharp
var progress = new Progress<SqlBulkCopyReport>(r =>
    Console.WriteLine($"{r.CurrentItemCount} rows in {r.BatchCount} batch(es), {r.CurrentSkippedItemCount} skipped"));

await loader.LoadAsync(ReadSourceAsync(), progress, CancellationToken.None);
```

### Validate rows before loading

```csharp
var loader = new SqlBulkCopyLoader<Customer>(connection)
{
    EnableDataValidation = true,
    ValidationFailureBehavior = ValidationFailureBehavior.Skip,
    OnValidationFailed = (item, errors) => Console.WriteLine($"skipped: {errors.Count} error(s)")
};
```

### Rehearse without writing

```csharp
var loader = new SqlBulkCopyLoader<Customer>(connection)
{
    IsDryRun = true
};

// Enumerates, maps, validates, counts and logs — but issues no SQL.
await loader.LoadAsync(ReadSourceAsync(), CancellationToken.None);
```

## Next Steps

- Explore the [API Reference](../api/index.md) for detailed documentation
- Read the [Introduction](introduction.md) to learn more about Wolfgang.Etl.SqlBulkCopy
- Check out example projects in the [GitHub repository](https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy)

## Common Issues

- **`InvalidOperationException` about a missing connection** — `PreAction` /
  `PostAction` values that issue SQL need a loader constructed with a
  `SqlConnection`.
- **Column not loaded** — check the property has a getter and is not marked
  `[NotMapped]`; the column name comes from `[Column]` when present, otherwise
  the property name.
- **Native AOT falls back to reflection** — mark the record `[BulkCopyable]` so
  the bundled source generator emits accessors at compile time.

## Additional Resources

- [GitHub Repository](https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy)
- [Contributing Guidelines](https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/blob/main/CONTRIBUTING.md)
- [Report an Issue](https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/issues)
