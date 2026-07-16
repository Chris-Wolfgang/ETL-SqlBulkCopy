window.BENCHMARK_DATA = {
  "lastUpdate": 1784178496609,
  "repoUrl": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy",
  "entries": {
    "SqlBulkCopy shadow workloads": [
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
          "id": "66aaa44195b705c0c998793af944c0f328a50573",
          "message": "Merge pull request #169 from Chris-Wolfgang/vNext\n\nrelease: v0.3.0",
          "timestamp": "2026-07-15T20:19:00Z",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/66aaa44195b705c0c998793af944c0f328a50573"
        },
        "date": 1784178495510,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.ShadowWorkloads.BulkLoadShadowWorkloads.LoadFlat(RecordCount: 1000)",
            "value": 11810334,
            "unit": "ns",
            "range": "± 454427.77695471037"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.ShadowWorkloads.BulkLoadShadowWorkloads.LoadWithValidation(RecordCount: 1000)",
            "value": 15524228.833333334,
            "unit": "ns",
            "range": "± 499059.5518195532"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.ShadowWorkloads.BulkLoadShadowWorkloads.LoadWithTruncatePreAction(RecordCount: 1000)",
            "value": 13860427.333333334,
            "unit": "ns",
            "range": "± 416579.1825023585"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.ShadowWorkloads.BulkLoadShadowWorkloads.LoadFlat(RecordCount: 100000)",
            "value": 439195557.3333333,
            "unit": "ns",
            "range": "± 14531317.247063197"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.ShadowWorkloads.BulkLoadShadowWorkloads.LoadWithValidation(RecordCount: 100000)",
            "value": 482475603,
            "unit": "ns",
            "range": "± 761883.7312129718"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.ShadowWorkloads.BulkLoadShadowWorkloads.LoadWithTruncatePreAction(RecordCount: 100000)",
            "value": 435568297.8333333,
            "unit": "ns",
            "range": "± 19226358.528757397"
          }
        ]
      }
    ]
  }
}