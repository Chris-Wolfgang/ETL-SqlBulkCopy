window.BENCHMARK_DATA = {
  "lastUpdate": 1791101626406,
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
      }
    ]
  }
}