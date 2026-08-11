# Reproducible Builds

`Wolfgang.Etl.SqlBulkCopy` is built **deterministically**: rebuilding the same
source commit in the same environment (OS + .NET SDK band) produces a
byte-for-byte identical managed assembly. This lets you independently verify
that the package published to NuGet was built from the tagged source in this
repository — nothing was injected between source and artifact.

## What is guaranteed

The build relies on the standard .NET determinism knobs:

- **`Deterministic`** — no timestamps or machine-specific paths in the IL. This is
  the .NET SDK **default** (`true`); the repo does not override it, so there is no
  such property in `Directory.Build.props`.
- `<ContinuousIntegrationBuild>true</ContinuousIntegrationBuild>` — normalizes source
  paths. Set in `Directory.Build.props`, conditional on `CI == 'true'`, so it applies
  to CI/release builds; a plain local build does **not** set it, which is itself a
  source of divergence when comparing a local build against a released artifact.
- **SourceLink** + `<EmbedUntrackedSources>true</EmbedUntrackedSources>` — source
  references resolve to this repo at the exact commit. Both in `Directory.Build.props`.

The guarantee covers the **managed assembly** (`Wolfgang.Etl.SqlBulkCopy.dll`)
rebuilt on the **same OS and SDK band** the release was built with.

### Cross-OS reproducibility (advisory)

The CI workflow [`reproducible-build.yaml`](.github/workflows/reproducible-build.yaml)
additionally builds the same commit on `ubuntu-latest` and `windows-latest` and
compares the SHA-256 of the produced assembly *across* runners. Full byte-identity
**across operating systems** is a stronger property and is currently tracked as
**advisory** — the workflow reports a cross-OS hash divergence as a warning while
that path is investigated (embedded path / SourceLink normalization differences).
For now, verify against a build on the **same OS** as the release runner. The jobs
that build and pack the released artifact (`validate-release`, `pack-and-validate`,
`publish-nuget`) all run on **`windows-latest`** — so reproduce on Windows. (Only
the ancillary `aot-consumer` and `attest-build-provenance` jobs run on Ubuntu.)

## How to verify a released version

For a published version `vX.Y.Z`:

```bash
# 1. Get the exact source the release was built from.
git clone https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy.git
cd ETL-SqlBulkCopy
git checkout vX.Y.Z

# 2. Rebuild the managed assembly with the same deterministic flag CI uses.
dotnet build src/Wolfgang.Etl.SqlBulkCopy/Wolfgang.Etl.SqlBulkCopy.csproj \
  -c Release -f net8.0 -p:ContinuousIntegrationBuild=true

# 3. Hash your build.
sha256sum src/Wolfgang.Etl.SqlBulkCopy/bin/Release/net8.0/Wolfgang.Etl.SqlBulkCopy.dll
```

Then compare that hash against:

- the assembly extracted from the published `.nupkg`
  (`nuget install Wolfgang.Etl.SqlBulkCopy -Version X.Y.Z`, unzip, hash
  `lib/net8.0/Wolfgang.Etl.SqlBulkCopy.dll`), and/or
- the hash printed in that release's `reproducible-build.yaml` run logs.

A match confirms the published managed assembly is exactly what this source
commit produces.

## Caveats

- Only the **managed IL** is guaranteed byte-identical. Native assets, embedded
  PDBs, and the `.nupkg` container itself can carry environment-specific bytes
  (compression metadata, file ordering) and are not part of the cross-runner
  managed-assembly guarantee.
- You must build with the **same target framework and configuration** shown
  above (`-c Release -f net8.0 -p:ContinuousIntegrationBuild=true`). A plain
  `dotnet build` without `ContinuousIntegrationBuild=true` will embed local
  paths and will not match.
- Use the same major .NET SDK band the release was built with; large SDK version
  gaps can change emitted IL.
