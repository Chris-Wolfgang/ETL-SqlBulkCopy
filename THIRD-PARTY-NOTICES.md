# Third-Party Notices

`Wolfgang.Etl.SqlBulkCopy` ships the runtime dependencies listed below.
`license-audit.yaml` audits the shipped package's transitive dependency
licences on every PR that touches a `.csproj`, plus weekly.

> **This file is an informational summary of the direct dependencies, not the
> shipped notice.** The notice inside the NuGet package is generated per release
> by `scripts/third-party-notices.ps1` (`nuget-license`, over the full transitive
> closure) into `src/Wolfgang.Etl.SqlBulkCopy/obj/THIRD-PARTY-NOTICES.md`, which
> `Directory.Build.props` packs into the package root. Keep the versions below in
> step with `src/Wolfgang.Etl.SqlBulkCopy/Wolfgang.Etl.SqlBulkCopy.csproj`.

## Wolfgang.Etl.SqlBulkCopy

| Package | Version | License |
|---------|---------|---------|
| [Microsoft.Bcl.AsyncInterfaces](https://www.nuget.org/packages/Microsoft.Bcl.AsyncInterfaces/) | 10.0.13 | [MIT](https://licenses.nuget.org/MIT) |
| [Microsoft.Data.SqlClient](https://www.nuget.org/packages/Microsoft.Data.SqlClient/) | 7.1.1 | [MIT](https://licenses.nuget.org/MIT) |
| [Microsoft.Extensions.Logging.Abstractions](https://www.nuget.org/packages/Microsoft.Extensions.Logging.Abstractions/) | 10.0.13 | [MIT](https://licenses.nuget.org/MIT) |
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

## Regenerating the shipped notice

```
pwsh ./scripts/third-party-notices.ps1
```

renders `obj/THIRD-PARTY-NOTICES.md` for every `src/` project from its restored
dependency closure (`release.yaml` runs it between restore and pack).
Analyzer packages are `PrivateAssets=all` build-time-only and are never
distributed in the NuGet package, so they are deliberately out of scope.
