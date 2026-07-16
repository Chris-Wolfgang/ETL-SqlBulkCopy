window.BENCHMARK_DATA = {
  "lastUpdate": 1784231981404,
  "repoUrl": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy",
  "entries": {
    "BenchmarkDotNet": [
      {
        "commit": {
          "author": {
            "email": "210299580+Chris-Wolfgang@users.noreply.github.com",
            "name": "Chris Wolfgang",
            "username": "Chris-Wolfgang"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ce39349543acd91872e8cc2daabf0f45dae6e152",
          "message": "Merge pull request #182 from Chris-Wolfgang/protected/v0.4.0-workflows\n\nci: benchmarks + zizmor gate — protected-only PR ahead of v0.4.0",
          "timestamp": "2026-07-16T15:57:24-04:00",
          "tree_id": "223ecde65b156a75e0e893ddde8b33428774a249",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/ce39349543acd91872e8cc2daabf0f45dae6e152"
        },
        "date": 1784231980066,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 73232.94868977864,
            "unit": "ns",
            "range": "± 614.9470724827244"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7146633.700520833,
            "unit": "ns",
            "range": "± 63072.957358423584"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 8.380099818110466,
            "unit": "ns",
            "range": "± 0.24519962648021604"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.6644853924711546,
            "unit": "ns",
            "range": "± 0.0016150938478617022"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 12.781451483567556,
            "unit": "ns",
            "range": "± 0.0797644656824689"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 6.981499095757802,
            "unit": "ns",
            "range": "± 0.03656264470623601"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.745085450510184,
            "unit": "ns",
            "range": "± 0.031109082338352176"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 10895.859680175781,
            "unit": "ns",
            "range": "± 54.60787623748772"
          }
        ]
      }
    ]
  }
}