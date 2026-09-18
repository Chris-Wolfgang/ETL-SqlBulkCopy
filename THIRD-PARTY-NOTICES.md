# Third-Party Notices

`Wolfgang.Etl.SqlBulkCopy` ships the runtime dependencies listed below.
`license-audit.yaml` audits the shipped package's transitive dependency
licences on every PR that touches a `.csproj`, plus weekly. Regenerate this
file's table by hand (from `dotnet-project-licenses`'s console output — see the
command below) and commit it whenever the dependency graph changes.

## Wolfgang.Etl.SqlBulkCopy

| Package | Version | License |
|---------|---------|---------|
| [Microsoft.Bcl.AsyncInterfaces](https://www.nuget.org/packages/Microsoft.Bcl.AsyncInterfaces/) | 10.0.11 | [MIT](https://licenses.nuget.org/MIT) |
| [Microsoft.Data.SqlClient](https://www.nuget.org/packages/Microsoft.Data.SqlClient/) | 7.0.2 | [MIT](https://licenses.nuget.org/MIT) |
| [Microsoft.Extensions.Logging.Abstractions](https://www.nuget.org/packages/Microsoft.Extensions.Logging.Abstractions/) | 10.0.11 | [MIT](https://licenses.nuget.org/MIT) |
| [System.ComponentModel.Annotations](https://www.nuget.org/packages/System.ComponentModel.Annotations/) | 5.0.0 | [MIT](https://licenses.nuget.org/MIT) |

> Microsoft.Bcl.AsyncInterfaces supplies `IAsyncEnumerable<T>` /
> `IAsyncDisposable` on the down-level targets; on net8.0+ those types are part
> of the framework. Microsoft.Data.SqlClient is the SQL Server driver behind
> `SqlBulkCopy`; System.ComponentModel.Annotations provides the
> `[Column]` / `[NotMapped]` attributes the mapper honours on targets where
> they are not in-box.

## First-party dependencies

`Wolfgang.Etl.Abstractions` (MIT) is also a shipped runtime dependency, but it
is authored and published by this project's owner rather than a third party, so
it is recorded here for completeness rather than listed in the table above.

`Wolfgang.Etl.SqlBulkCopy.SourceGenerator` (MIT) ships inside this package as an
analyzer asset; it is part of this repository, not a third-party dependency.

## Copyright

- Microsoft.Bcl.AsyncInterfaces, Microsoft.Data.SqlClient,
  Microsoft.Extensions.Logging.Abstractions, System.ComponentModel.Annotations —
  © Microsoft Corporation. All rights reserved.

## Baseline scan

Generated from:

```
dotnet-project-licenses --input src/Wolfgang.Etl.SqlBulkCopy/Wolfgang.Etl.SqlBulkCopy.csproj
```

against the src project's shipped (non-analyzer, non-test) dependency graph.
Analyzer packages are `PrivateAssets=all` build-time-only and are never
distributed in the NuGet package, so they are deliberately out of scope.
