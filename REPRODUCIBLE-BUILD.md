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

### Cross-OS reproducibility

The CI workflow [`reproducible-build.yaml`](.github/workflows/reproducible-build.yaml)
additionally builds the same commit on `ubuntu-latest` and `windows-latest` and
compares the SHA-256 of the produced assembly *across* runners. A difference
**fails** the workflow, and its log shows which outputs differ. The cross-OS
divergence tracked in #176 came from MSBuild writing two generated source files
with the host's line endings; `Directory.Build.targets` now normalizes them to LF
before compilation. Until a scheduled run of that workflow has passed on the
release commit, prefer reproducing on the **same OS** as the release runner. The
jobs that build and pack the released artifact (`validate-release`,
`pack-and-validate`, `publish-nuget`) run on **`windows-latest`**.

## Tooling

- **.NET SDK:** use the exact SDK version in the `"sdk"` field of the release's
  `reproducible-build-manifest.json` (for example `"sdk": "10.0.400"`). The
  release jobs install the latest 10.0.x SDK and there is no `global.json`, so
  each release records the SDK that built its assemblies. Pin it for your
  rebuild with `dotnet new globaljson --sdk-version <sdk>` in the checkout (or
  install it with the `dotnet-install` script: `-Version <sdk>` in PowerShell,
  `--version <sdk>` in bash). For releases published before the manifest
  carried this field, the `release.yaml` run log prints the SDK in the
  *Pack & Validate NuGet* job's *Setup .NET* step.
- **Build machine:** the manifest's `"buildImage"` field names the GitHub-hosted
  runner image the release built on (for example `win25 20261020.1`). Your OS
  does not have to match for the managed assembly, but quote it when you report
  a discrepancy.
- **Build flags:** `-c Release -p:ContinuousIntegrationBuild=true`, one target
  framework at a time (`-f <tfm>`).
- **Hashing:** any SHA-256 tool (`sha256sum`, `Get-FileHash`, `shasum -a 256`).

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

- the release's **`reproducible-build-manifest.json`** asset (on the GitHub
  release page). It lists the SHA-256 of every shipped assembly, keyed by its
  path inside the package (for example `lib/net8.0/Wolfgang.Etl.SqlBulkCopy.dll`),
  plus the exact SDK (`"sdk"`) and runner image (`"buildImage"`) that built them;
- the assembly extracted from the published `.nupkg`
  (`nuget install Wolfgang.Etl.SqlBulkCopy -Version X.Y.Z`, unzip, hash
  `lib/net8.0/Wolfgang.Etl.SqlBulkCopy.dll`); and/or
- the hash printed in that commit's `reproducible-build.yaml` run logs.

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
- Use the exact SDK version from the manifest's `"sdk"` field (see
  [Tooling](#tooling)); a different SDK, even within the same band, can change
  emitted IL.

## Reporting a discrepancy

If your rebuild does not match the manifest, open an issue with the
[bug report form](https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/issues/new/choose)
and include: the release tag, the target framework, your OS and `dotnet --version`
output, the exact build command, your hash, and the hash you compared it with
(manifest or `.nupkg`). A mismatch is treated as a potential supply-chain
problem until it is explained.

## Publishing a third-party verification

Independent verifiers are welcome to publish their results. The suggested
procedure, following the [Reproducible Builds](https://reproducible-builds.org/)
conventions:

1. Rebuild the tagged commit as above, on infrastructure you control.
2. Record the tag, commit SHA, OS, SDK version, target framework and the
   SHA-256 of each assembly you rebuilt.
3. Publish that record where it can be checked independently (a signed commit or
   a GitHub release in your own repository, or a signed attestation such as an
   in-toto/SLSA statement). Link it from an issue in this repository so it can
   be referenced from the release notes.

Releases also carry a GitHub build-provenance attestation (`attest-build-provenance`
in `release.yaml`), which you can check with
`gh attestation verify <file> --repo Chris-Wolfgang/ETL-SqlBulkCopy`. That proves
*where* the package was built. A rebuild like the one above proves *what* was built.
