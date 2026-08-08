# Wolfgang.Etl.SqlBulkCopy

A loader that uses SqlBulkCopy for fast inserts into a Microsoft SQL database

[![NuGet](https://img.shields.io/nuget/v/Wolfgang.Etl.SqlBulkCopy.svg?logo=nuget&label=NuGet)](https://www.nuget.org/packages/Wolfgang.Etl.SqlBulkCopy/)
[![Downloads](https://img.shields.io/nuget/dt/Wolfgang.Etl.SqlBulkCopy.svg?logo=nuget&label=downloads)](https://www.nuget.org/packages/Wolfgang.Etl.SqlBulkCopy/)
[![PR build](https://img.shields.io/github/actions/workflow/status/Chris-Wolfgang/ETL-SqlBulkCopy/pr.yaml?event=pull_request_target&label=PR%20build&logo=github)](https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/actions/workflows/pr.yaml)
[![release](https://img.shields.io/github/actions/workflow/status/Chris-Wolfgang/ETL-SqlBulkCopy/release.yaml?event=release&label=release&logo=github)](https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/actions/workflows/release.yaml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-Multi--Targeted-purple.svg)](https://dotnet.microsoft.com/)
[![GitHub](https://img.shields.io/badge/GitHub-Repository-181717?logo=github)](https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/Chris-Wolfgang/ETL-SqlBulkCopy/badge)](https://securityscorecards.dev/viewer/?uri=github.com/Chris-Wolfgang/ETL-SqlBulkCopy)

---

## 📦 Installation

```bash
dotnet add package Wolfgang.Etl.SqlBulkCopy
```

**NuGet Package:** [Wolfgang.Etl.SqlBulkCopy](https://www.nuget.org/packages/Wolfgang.Etl.SqlBulkCopy/)

---

## 📄 License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.

---

## 📚 Documentation

- **GitHub Repository:** [https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy](https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy)
- **API Documentation:** https://Chris-Wolfgang.github.io/ETL-SqlBulkCopy/
- **Formatting Guide:** [README-FORMATTING.md](README-FORMATTING.md)
- **Contributing Guide:** [CONTRIBUTING.md](CONTRIBUTING.md)
- **Architecture Decisions:** [docs/adr/index.md](docs/adr/index.md)
- **Migration Guides:** [docs/migrations/](docs/migrations/)

---

## 🚀 Quick Start

```csharp
using Microsoft.Data.SqlClient;
using Wolfgang.Etl.SqlBulkCopy;

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
    PreAction = PreAction.TruncateTable,
};

await loader.LoadAsync(ReadSourceAsync(), CancellationToken.None);
```

---

## ✨ Features

| Feature | Description |
|---------|-------------|
| **Streaming bulk load** | Consumes `IAsyncEnumerable<T>` and writes to SQL Server via `SqlBulkCopy` |
| **Type-driven mapping** | `[Table]` / `[Column]` / `[NotMapped]` attributes drive schema/table/column names — no manual `ColumnMappings` |
| **Nested tables** | Recursively writes child collections to their own tables inside the same bulk-copy session |
| **Pre/post actions** | Built-in `TruncateTable` / `DeleteAllRecords`; custom-action delegates for schema-aware work |
| **Progress reporting** | `IProgress<SqlBulkCopyReport>` — batch count, rows written, elapsed time |
| **Transactions** | Optional `SqlTransaction` participates in the bulk load and pre/post commands |
| **Async-only** | Banned-symbol analyzer enforces `WriteToServerAsync` / `ExecuteNonQueryAsync` — no sync fallbacks |
| **Native AOT ready** | Opt a record into compile-time source-generated accessors with `[BulkCopyable]` — no runtime IL emission on the hot path (net5.0+) |
| **Multi-targeted** | `net462`, `net481`, `netstandard2.0`, `net8.0`, `net10.0` |

**Examples:**
- **Truncate before load:** set `PreAction = PreAction.TruncateTable` (shown above).
- **Custom pre-action:** set `PreAction = PreAction.CustomAction` and `PreLoadCustomAction = async p => { /* p.Connection, p.Transaction, p.Columns, p.CancellationToken */ };`
- **Nested table:** decorate a `[NotMapped]`-free `IEnumerable<TChild>` property; the child rows write to the child's `[Table]` in the same session.

See the [API documentation](https://Chris-Wolfgang.github.io/ETL-SqlBulkCopy/) for the full surface.

---

## 🎯 Supported Frameworks

This library targets:

- **.NET Framework:** 4.6.2, 4.8.1
- **.NET Standard:** 2.0
- **.NET:** 8.0, 10.0

See the [NuGet package page](https://www.nuget.org/packages/Wolfgang.Etl.SqlBulkCopy/) for the authoritative per-TFM compatibility matrix.

## ⚡ Native AOT & trimming

By default the per-row hot path compiles a property getter at runtime with
`System.Linq.Expressions` — fast under the JIT, but it emits IL at run time, so
under Native AOT it falls back to the slower expression *interpreter*.

Opt a record into **compile-time source-generated accessors** by marking it
`[BulkCopyable]`:

```csharp
using System.ComponentModel.DataAnnotations.Schema;
using Wolfgang.Etl.SqlBulkCopy;

[BulkCopyable]
[Table("People")]
public sealed class Person
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public Status Status { get; set; }   // enum columns are handled too
}
```

The bundled source generator emits strongly-typed getters (and enum→underlying
converters) at compile time, and the loader uses them automatically — no runtime
`Expression.Compile`, so the marked type's hot path is AOT-clean **and** keeps
the compiled-getter throughput.

| | |
|---|---|
| **Opt-in & additive** | Unmarked types keep working exactly as before via the runtime-compiled getter. Marking a type never changes its mapping — only *how* the getters are produced. |
| **One package** | The generator ships inside `Wolfgang.Etl.SqlBulkCopy`; there is nothing extra to install or reference. |
| **`net5.0`+** | Generated registration uses module initializers. On older targets the attribute is a no-op and the type uses the runtime getter — correct on those JIT-only frameworks. |

> **Note:** a full Native-AOT publish of an app that *opens a SQL connection*
> also depends on `Microsoft.Data.SqlClient`'s own AOT support, which is outside
> this library's control. `[BulkCopyable]` makes **this library's** mapping path
> AOT-clean. See
> [ADR 0006](https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/blob/main/docs/adr/0006-source-generated-property-accessors.md)
> for the full rationale.

---

## 🔍 Code Quality & Static Analysis

This project enforces **strict code quality standards** through **7 specialized analyzers** and custom async-first rules:

### Analyzers in Use

1. **Microsoft.CodeAnalysis.NetAnalyzers** - Built-in .NET analyzers for correctness and performance
2. **Roslynator.Analyzers** - Advanced refactoring and code quality rules
3. **AsyncFixer** - Async/await best practices and anti-pattern detection
4. **Microsoft.VisualStudio.Threading.Analyzers** - Thread safety and async patterns
5. **Microsoft.CodeAnalysis.BannedApiAnalyzers** - Prevents usage of banned synchronous APIs
6. **Meziantou.Analyzer** - Comprehensive code quality rules
7. **SonarAnalyzer.CSharp** - Industry-standard code analysis

### Async-First Enforcement

This library uses **`BannedSymbols.txt`** to prohibit synchronous APIs and enforce async-first patterns:

**Blocked APIs Include:**
- ❌ `Task.Wait()`, `Task.Result` - Use `await` instead
- ❌ `Thread.Sleep()` - Use `await Task.Delay()` instead
- ❌ Synchronous file I/O (`File.ReadAllText`) - Use async versions
- ❌ Synchronous stream operations - Use `ReadAsync()`, `WriteAsync()`
- ❌ `Parallel.For/ForEach` - Use `Task.WhenAll()` or `Parallel.ForEachAsync()`
- ❌ Obsolete APIs (`WebClient`, `BinaryFormatter`)

**Why?** To ensure all code is **truly async** and **non-blocking** for optimal performance in async contexts.

---

## 🛠️ Building from Source

### Prerequisites
- [.NET 8.0 SDK](https://dotnet.microsoft.com/download) or later
- Optional: [PowerShell Core](https://github.com/PowerShell/PowerShell) for formatting scripts

### Build Steps

```bash
# Clone the repository
git clone https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy.git
cd ETL-SqlBulkCopy

# Restore dependencies
dotnet restore

# Build the solution
dotnet build --configuration Release

# Run tests
dotnet test --configuration Release

# Run code formatting (PowerShell Core)
pwsh ./format.ps1
```

### Code Formatting

This project uses `.editorconfig` and `dotnet format`:

```bash
# Format code
dotnet format

# Verify formatting (as CI does)
dotnet format --verify-no-changes
```

See [README-FORMATTING.md](README-FORMATTING.md) for detailed formatting guidelines.

### Building Documentation

This project uses [DocFX](https://dotnet.github.io/docfx/) to generate API documentation:

```bash
# Install DocFX (one-time setup)
dotnet tool install -g docfx

# Generate API metadata and build documentation
cd docfx_project
docfx metadata  # Extract API metadata from source code
docfx build     # Build HTML documentation

# Documentation is generated in the docs/ folder at the repository root
```

The documentation is automatically built and deployed to GitHub Pages when changes are pushed to the `main` branch.

**Local Preview:**
```bash
# Serve documentation locally (with live reload)
cd docfx_project
docfx build --serve

# Open http://localhost:8080 in your browser
```

**Documentation Structure:**
- `docfx_project/` - DocFX configuration and source files
- `docs/` - Generated HTML documentation (published to GitHub Pages)
- `docfx_project/index.md` - Main landing page content
- `docfx_project/docs/` - Additional documentation articles
- `docfx_project/api/` - Auto-generated API reference YAML files

---

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for:
- Code quality standards
- Build and test instructions
- Pull request guidelines
- Analyzer configuration details

---


## 🙏 Acknowledgments

Built on top of [Wolfgang.Etl.Abstractions](https://github.com/Chris-Wolfgang/ETL-Abstractions) and [Microsoft.Data.SqlClient](https://github.com/dotnet/SqlClient).

