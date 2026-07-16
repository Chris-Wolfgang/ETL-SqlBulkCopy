window.BENCHMARK_DATA = {
  "lastUpdate": 1784233116324,
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
      },
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
          "id": "00f8c2b978c06d538a3ec44bc9d30f493ebfe5b4",
          "message": "Merge pull request #183 from Chris-Wolfgang/vNext\n\nrelease: v0.4.0",
          "timestamp": "2026-07-16T16:16:06-04:00",
          "tree_id": "6c92cf6c9fc490fe7efc8de62b0587c60064f94d",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/00f8c2b978c06d538a3ec44bc9d30f493ebfe5b4"
        },
        "date": 1784233114657,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 76474.72680664062,
            "unit": "ns",
            "range": "± 1050.0794861001805"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7417131.575520833,
            "unit": "ns",
            "range": "± 180547.3038422599"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 7.468239165842533,
            "unit": "ns",
            "range": "± 0.016600346061341847"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.8438654541969299,
            "unit": "ns",
            "range": "± 0.2041121233683688"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 13.537619481484095,
            "unit": "ns",
            "range": "± 0.05644194493125195"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 7.758208741744359,
            "unit": "ns",
            "range": "± 0.060679681350749484"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 1.702940861384074,
            "unit": "ns",
            "range": "± 0.0007944797233096277"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 12195.821111043295,
            "unit": "ns",
            "range": "± 317.1035723638065"
          }
        ]
      }
    ]
  }
}