# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability, please follow these steps:

1. **Do not** create a public issue on this repository.
2. In the top navigation of this repository, click the **Security** tab.
3. In the top right, click the **Report a vulnerability** button.
4. Fill out the provided form with:
   - A description of the vulnerability
   - Steps to reproduce the issue
   - Potential impact
   - Suggested fix (if you have one)

## Response Timeline

We will acknowledge your report within 48 hours and provide an estimated timeline for a fix.

## Supply-Chain Security Posture

This project's supply-chain posture is measured continuously by the
[OpenSSF Scorecard](https://securityscorecards.dev/viewer/?uri=github.com/Chris-Wolfgang/ETL-SqlBulkCopy)
(the `scorecard.yaml` workflow runs weekly and on pushes to `main`, publishing
results to the public registry — see the badge in the README).

**Score floor: 5.0.** The aggregate Scorecard score should not drop below this
floor, and no individual check that is currently passing should regress to a
failing state. If a change would push the score below the floor — or trip a
previously-clean check (e.g. by adding an unpinned action, a token with broad
permissions, or a workflow without least-privilege) — that is expected to draw
reviewer attention and be justified or remediated before merge. The floor is a
minimum, not a target; the aim is to raise the score over time (pinned actions,
branch protection, signed releases, etc.).

## Release path & compromise scope

Facts a maintainer would need at 2am if the release identity is compromised. Generic incident-response steps (rotating credentials, revoking OAuth apps, publishing advisories, unlisting NuGet packages) are not duplicated here — GitHub's and NuGet's own docs update faster than a checked-in runbook.

- **Release path**: OIDC / NuGet Trusted Publishing via `NuGet/login@v1` in `.github/workflows/release.yaml`. The workflow mints an ephemeral push token per run via OIDC — the release path does not depend on a long-lived API key stored in GitHub secrets or on the NuGet account. During an incident, check the NuGet account for any long-lived API keys anyway (they can be created outside of CI) and delete anything you don't recognize.
- **Fallback**: none. If Trusted Publishing is compromised, the incident is at the GitHub-account level (the OIDC identity is `Chris-Wolfgang/ETL-SqlBulkCopy`).
- **Owner**: @Chris-Wolfgang.
- **Downstream consumers**: no known `Wolfgang.*` fleet dependents (ETL-SqlBulkCopy is a leaf library — it consumes `Wolfgang.Etl.Abstractions`, but nothing in the fleet depends on it); unknown external consumers may exist on nuget.org.
- **Package coordinates for unlisting**: `Wolfgang.Etl.SqlBulkCopy` on nuget.org — <https://www.nuget.org/packages/Wolfgang.Etl.SqlBulkCopy/>.

## Supply-chain verification (consumer-side)

Every published `Wolfgang.Etl.SqlBulkCopy` NuGet has:

1. **A CycloneDX SBOM** (`Wolfgang.Etl.SqlBulkCopy.bom.json`) attached to the GitHub Release, listing every direct + transitive dependency at release time.
2. **A SLSA build-provenance attestation** signed by Sigstore's keyless CA using GitHub's OIDC identity. The attestation proves the `.nupkg` + `.snupkg` were built by `.github/workflows/release.yaml` at a specific commit SHA — no local build was substituted, no bit was flipped between build and publish.

> **On NuGet author-signing:** the packages are **not** author-signed with a code-signing certificate — that is intentionally out of scope (it requires a purchased/managed cert). The SLSA provenance attestation above provides the equivalent build-integrity guarantee (who built it, from which commit, unaltered) without one, and is verified with `gh attestation verify` rather than `nuget verify`.

To verify a package you downloaded from nuget.org actually came from this repo's release pipeline:

```bash
# 1. Download the package from nuget.org (or your local NuGet feed).
curl -sSL -o Wolfgang.Etl.SqlBulkCopy.<version>.nupkg \
  "https://api.nuget.org/v3-flatcontainer/wolfgang.etl.sqlbulkcopy/<version>/wolfgang.etl.sqlbulkcopy.<version>.nupkg"

# 2. Verify the SLSA attestation.
gh attestation verify Wolfgang.Etl.SqlBulkCopy.<version>.nupkg \
  --owner Chris-Wolfgang \
  --repo ETL-SqlBulkCopy
```

The `gh attestation verify` command fetches the attestation from Sigstore's public transparency log, confirms it was signed by `Chris-Wolfgang/ETL-SqlBulkCopy`'s (GitHub-verified, unforgeable) OIDC identity, that the workflow was `.github/workflows/release.yaml`, and that the artifact's SHA-256 matches the one recorded at build time. Any mismatch = the file didn't come from a legitimate release, or was tampered with in transit / on your local cache.

For the SBOM, download the `Wolfgang.Etl.SqlBulkCopy.bom.json` asset from the GitHub Release and validate with any CycloneDX-aware tooling (`cyclonedx-cli`, Grype, Trivy, GitHub's Dependency Graph, etc.).

Refs [#90](https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/issues/90).

## Thank You

Your help is greatly appreciated!
Responsible disclosure of security vulnerabilities helps protect our entire community.
