window.BENCHMARK_DATA = {
  "lastUpdate": 1791699991191,
  "repoUrl": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy",
  "entries": {
    "Mutation score": [
      {
        "commit": {
          "author": {
            "name": "Chris Wolfgang",
            "username": "Chris-Wolfgang",
            "email": "210299580+Chris-Wolfgang@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "9e116de310f4c527f3662ff0f11b45c274eee0d3",
          "message": "fix(tests): clear the six InspectCode alerts in SourceLinkPdbTests (#411)\n\nAll six are in the new SourceLink test file and all are genuine, so each is fixed\nrather than suppressed.\n\nShortLivedHttpClient + UsingStatementResourceInitialization (both at the same\nline) share one fix: the per-call `using var http = new HttpClient { Timeout = ... }`\nbecomes a static readonly field. HttpClient is thread-safe, a long-lived instance\nis the documented way to avoid socket exhaustion, and moving it out of the method\nremoves the `using` statement whose object initializer was the disposal hazard -\nif a property setter threw, the client would not have been disposed.\n\nRedundantSuppressNullableWarningExpression and S8969 are the same defect reported\nby two tools: the `uri!` null-forgiving operator after `Uri.TryCreate`. Both said\nthe compiler already knows the expression is non-null, and a Release build with\nTreatWarningsAsErrors confirms it - no CS8602 after removing the `!`.\n\nRedundantUsingDirective on `using System.Net.Http;` is a true positive here, not\nthe usual TFM-blind false positive: this project is single-TFM net10.0 with\nImplicitUsings enabled, and System.Net.Http is in the SDK's implicit set. Verified\nby building after removing it.\n\nS125 (\"remove this commented out code\") pointed at explanatory prose, not code.\nThe trigger was a line ending in a semicolon mid-sentence, so the fix is to end\nthe sentence with a full stop and capitalise the next word. Rewording beats a\nsuppression: the comment reads better and the signal that misled the analyser is\ngone.\n\nVerified: Release build 0 warnings / 0 errors; 3/3 tests pass.\n\nCo-authored-by: Claude Opus 5 <noreply@anthropic.com>",
          "timestamp": "2026-09-25T00:35:52Z",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/9e116de310f4c527f3662ff0f11b45c274eee0d3"
        },
        "date": 1790490529257,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Mutation score",
            "value": 83.85,
            "unit": "%"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Chris Wolfgang",
            "username": "Chris-Wolfgang",
            "email": "210299580+Chris-Wolfgang@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "9e116de310f4c527f3662ff0f11b45c274eee0d3",
          "message": "fix(tests): clear the six InspectCode alerts in SourceLinkPdbTests (#411)\n\nAll six are in the new SourceLink test file and all are genuine, so each is fixed\nrather than suppressed.\n\nShortLivedHttpClient + UsingStatementResourceInitialization (both at the same\nline) share one fix: the per-call `using var http = new HttpClient { Timeout = ... }`\nbecomes a static readonly field. HttpClient is thread-safe, a long-lived instance\nis the documented way to avoid socket exhaustion, and moving it out of the method\nremoves the `using` statement whose object initializer was the disposal hazard -\nif a property setter threw, the client would not have been disposed.\n\nRedundantSuppressNullableWarningExpression and S8969 are the same defect reported\nby two tools: the `uri!` null-forgiving operator after `Uri.TryCreate`. Both said\nthe compiler already knows the expression is non-null, and a Release build with\nTreatWarningsAsErrors confirms it - no CS8602 after removing the `!`.\n\nRedundantUsingDirective on `using System.Net.Http;` is a true positive here, not\nthe usual TFM-blind false positive: this project is single-TFM net10.0 with\nImplicitUsings enabled, and System.Net.Http is in the SDK's implicit set. Verified\nby building after removing it.\n\nS125 (\"remove this commented out code\") pointed at explanatory prose, not code.\nThe trigger was a line ending in a semicolon mid-sentence, so the fix is to end\nthe sentence with a full stop and capitalise the next word. Rewording beats a\nsuppression: the comment reads better and the signal that misled the analyser is\ngone.\n\nVerified: Release build 0 warnings / 0 errors; 3/3 tests pass.\n\nCo-authored-by: Claude Opus 5 <noreply@anthropic.com>",
          "timestamp": "2026-09-25T00:35:52Z",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/9e116de310f4c527f3662ff0f11b45c274eee0d3"
        },
        "date": 1791101619630,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Mutation score",
            "value": 83.59,
            "unit": "%"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "b00d80b9b4faa57a57e58cf81ad4d950c9599f1f",
          "message": "build(deps): bump the github-actions group across 1 directory with 5 updates (#462)\n\nBumps the github-actions group with 5 updates in the / directory:\n\n| Package | From | To |\n| --- | --- | --- |\n| [github/codeql-action/upload-sarif](https://github.com/github/codeql-action) | `4.38.1` | `4.38.3` |\n| [github/codeql-action/init](https://github.com/github/codeql-action) | `4.38.1` | `4.38.3` |\n| [github/codeql-action/analyze](https://github.com/github/codeql-action) | `4.38.1` | `4.38.3` |\n| [actions/upload-artifact](https://github.com/actions/upload-artifact) | `7.0.1` | `7.0.2` |\n| [actions/download-artifact](https://github.com/actions/download-artifact) | `8.0.1` | `8.0.2` |\n\n\n\nUpdates `github/codeql-action/upload-sarif` from 4.38.1 to 4.38.3\n- [Release notes](https://github.com/github/codeql-action/releases)\n- [Changelog](https://github.com/github/codeql-action/blob/main/CHANGELOG.md)\n- [Commits](https://github.com/github/codeql-action/compare/1c5b675653bb5c22dbe9b12b556ec555138e09fd...24c54180a607b1449ed407dd24f251e4e9147c8d)\n\nUpdates `github/codeql-action/init` from 4.38.1 to 4.38.3\n- [Release notes](https://github.com/github/codeql-action/releases)\n- [Changelog](https://github.com/github/codeql-action/blob/main/CHANGELOG.md)\n- [Commits](https://github.com/github/codeql-action/compare/1c5b675653bb5c22dbe9b12b556ec555138e09fd...24c54180a607b1449ed407dd24f251e4e9147c8d)\n\nUpdates `github/codeql-action/analyze` from 4.38.1 to 4.38.3\n- [Release notes](https://github.com/github/codeql-action/releases)\n- [Changelog](https://github.com/github/codeql-action/blob/main/CHANGELOG.md)\n- [Commits](https://github.com/github/codeql-action/compare/1c5b675653bb5c22dbe9b12b556ec555138e09fd...24c54180a607b1449ed407dd24f251e4e9147c8d)\n\nUpdates `actions/upload-artifact` from 7.0.1 to 7.0.2\n- [Release notes](https://github.com/actions/upload-artifact/releases)\n- [Commits](https://github.com/actions/upload-artifact/compare/043fb46d1a93c77aae656e7c1c64a875d1fc6a0a...cf430e030ddbb5b0abf93d22962f4752f3646cd9)\n\nUpdates `actions/download-artifact` from 8.0.1 to 8.0.2\n- [Release notes](https://github.com/actions/download-artifact/releases)\n- [Commits](https://github.com/actions/download-artifact/compare/3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c...9000827ccba6bdab643e8b6fd33ac0654aef8333)\n\n---\nupdated-dependencies:\n- dependency-name: actions/download-artifact\n  dependency-version: 8.0.2\n  dependency-type: direct:production\n  update-type: version-update:semver-patch\n  dependency-group: github-actions\n- dependency-name: actions/upload-artifact\n  dependency-version: 7.0.2\n  dependency-type: direct:production\n  update-type: version-update:semver-patch\n  dependency-group: github-actions\n- dependency-name: github/codeql-action/analyze\n  dependency-version: 4.38.3\n  dependency-type: direct:production\n  update-type: version-update:semver-patch\n  dependency-group: github-actions\n- dependency-name: github/codeql-action/init\n  dependency-version: 4.38.3\n  dependency-type: direct:production\n  update-type: version-update:semver-patch\n  dependency-group: github-actions\n- dependency-name: github/codeql-action/upload-sarif\n  dependency-version: 4.38.3\n  dependency-type: direct:production\n  update-type: version-update:semver-patch\n  dependency-group: github-actions\n...\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2026-10-11T02:17:18Z",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/b00d80b9b4faa57a57e58cf81ad4d950c9599f1f"
        },
        "date": 1791699984557,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Mutation score",
            "value": 83.85,
            "unit": "%"
          }
        ]
      }
    ]
  }
}