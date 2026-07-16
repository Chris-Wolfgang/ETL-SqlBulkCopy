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

## Thank You

Your help is greatly appreciated!
Responsible disclosure of security vulnerabilities helps protect our entire community.
