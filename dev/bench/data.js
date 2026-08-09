window.BENCHMARK_DATA = {
  "lastUpdate": 1786305477200,
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
          "id": "4c6521949730da813e109cbdcc2ebf593b813400",
          "message": "Merge pull request #196 from Chris-Wolfgang/dependabot/github_actions/github-actions-693826a35f\n\nbuild(deps): bump the github-actions group with 6 updates",
          "timestamp": "2026-07-27T17:40:16-04:00",
          "tree_id": "4c9a51761701ec15e7a0fd60a679c1175deb2730",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/4c6521949730da813e109cbdcc2ebf593b813400"
        },
        "date": 1785188559303,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 73056.08768717448,
            "unit": "ns",
            "range": "± 555.3082985219985"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7122104.283854167,
            "unit": "ns",
            "range": "± 25827.33780178142"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 8.125954180955887,
            "unit": "ns",
            "range": "± 0.012746979895355262"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.6665406773487726,
            "unit": "ns",
            "range": "± 0.0029444877020593646"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 12.96039683620135,
            "unit": "ns",
            "range": "± 0.062308850805879314"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 12.7639608780543,
            "unit": "ns",
            "range": "± 1.301417147649083"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.7623995219667754,
            "unit": "ns",
            "range": "± 0.035831277188034964"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 10564.464579264322,
            "unit": "ns",
            "range": "± 12.535777378146674"
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
          "id": "0564627d0df0f9bd190bc8ce19621a513b7c2b47",
          "message": "Merge pull request #197 from Chris-Wolfgang/dependabot/nuget/dotnet-dependencies-fd4bd7f9c6\n\nBump the dotnet-dependencies group with 5 updates",
          "timestamp": "2026-07-27T17:58:50-04:00",
          "tree_id": "49c946243fea9b0f821df97405494b47846f58ce",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/0564627d0df0f9bd190bc8ce19621a513b7c2b47"
        },
        "date": 1785189684641,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 75875.04622395833,
            "unit": "ns",
            "range": "± 614.6157717466609"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7290926.3125,
            "unit": "ns",
            "range": "± 19429.75927046989"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 7.772583946585655,
            "unit": "ns",
            "range": "± 0.0066713320406912085"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.6195074071486791,
            "unit": "ns",
            "range": "± 0.18800062802807072"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 13.149497965971628,
            "unit": "ns",
            "range": "± 0.13528886846881094"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 7.770555292566617,
            "unit": "ns",
            "range": "± 0.1431534648046935"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 1.8137561877568562,
            "unit": "ns",
            "range": "± 0.045550195485140714"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 11744.738141377768,
            "unit": "ns",
            "range": "± 5.187821292328933"
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
          "id": "94ad09f8b64618bf301448e68a510f7f129d9e78",
          "message": "Merge pull request #221 from Chris-Wolfgang/vNext\n\nRelease v0.5.0 — source-generated AOT accessors + CI hardening",
          "timestamp": "2026-08-08T11:48:10-04:00",
          "tree_id": "08976c92085aa1ace3dfe66c9659593f46feac12",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/94ad09f8b64618bf301448e68a510f7f129d9e78"
        },
        "date": 1786204249324,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 73883.5908203125,
            "unit": "ns",
            "range": "± 672.9794464945958"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7239488.770833333,
            "unit": "ns",
            "range": "± 36528.06185353468"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 7.4829752792914705,
            "unit": "ns",
            "range": "± 0.008621707818276479"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.8518557374676069,
            "unit": "ns",
            "range": "± 0.19540684654884363"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 13.5563625395298,
            "unit": "ns",
            "range": "± 0.3613535922530209"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 7.772440083324909,
            "unit": "ns",
            "range": "± 0.11355288632114334"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 1.7859518267214298,
            "unit": "ns",
            "range": "± 0.004619791333407303"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 12025.568349202475,
            "unit": "ns",
            "range": "± 39.96280531711449"
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
          "id": "d2e70158026e5d7d616de967a0bed36b92b41e19",
          "message": "Merge pull request #222 from Chris-Wolfgang/fix/inherited-generated-getter\n\nfix: generated getter lookup for inherited [BulkCopyable] properties",
          "timestamp": "2026-08-08T12:31:08-04:00",
          "tree_id": "d43593ef397e273d507dfcac859f03c0fa2c47dc",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/d2e70158026e5d7d616de967a0bed36b92b41e19"
        },
        "date": 1786206820197,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 86750.35860188802,
            "unit": "ns",
            "range": "± 652.1834340507343"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 9049166.9375,
            "unit": "ns",
            "range": "± 50341.2223840062"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 7.0780878365039825,
            "unit": "ns",
            "range": "± 0.1116202142678759"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 1.2089689026276271,
            "unit": "ns",
            "range": "± 0.005697443306882114"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 13.063624312480291,
            "unit": "ns",
            "range": "± 0.25668514402564224"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 7.049000859260559,
            "unit": "ns",
            "range": "± 0.012421936871189171"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.2983278185129166,
            "unit": "ns",
            "range": "± 0.03207492963269152"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 9281.115458170572,
            "unit": "ns",
            "range": "± 162.88843290353194"
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
          "id": "dd8da13e0350193530293979dcb9efa8b3068f1d",
          "message": "Merge pull request #225 from Chris-Wolfgang/fix/generator-doc-summary\n\ndocs: correct generator descriptor summary (nested tables)",
          "timestamp": "2026-08-08T12:47:50-04:00",
          "tree_id": "75fb0e7d616cb5e0a2e44e16c2fcbc94b7b8a480",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/dd8da13e0350193530293979dcb9efa8b3068f1d"
        },
        "date": 1786207809939,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 75723.78470865886,
            "unit": "ns",
            "range": "± 1847.404521911052"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7160463.776041667,
            "unit": "ns",
            "range": "± 34150.00223410499"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 8.122320880492529,
            "unit": "ns",
            "range": "± 0.012531902883739711"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.6631886040170988,
            "unit": "ns",
            "range": "± 0.0018659383861393867"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 12.83258056640625,
            "unit": "ns",
            "range": "± 0.07315772731707622"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 14.907039513190588,
            "unit": "ns",
            "range": "± 0.8331568197663197"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.7464327613512673,
            "unit": "ns",
            "range": "± 0.03371135846674573"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 10535.44491068522,
            "unit": "ns",
            "range": "± 0.5672768964109762"
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
          "id": "eb83f0ab19d1d8ed747ffce924af363977d594c4",
          "message": "Merge pull request #227 from Chris-Wolfgang/feat/support-dry-run\n\nfeat: ISupportDryRun on SqlBulkCopyLoader (#121)",
          "timestamp": "2026-08-09T15:55:17-04:00",
          "tree_id": "2439745a05051bddedc21bea7f07b5a2024e7207",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/eb83f0ab19d1d8ed747ffce924af363977d594c4"
        },
        "date": 1786305475470,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 65300.5683186849,
            "unit": "ns",
            "range": "± 574.9954953857426"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 6192069.239583333,
            "unit": "ns",
            "range": "± 78733.93307604459"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 5.495927602052689,
            "unit": "ns",
            "range": "± 0.09643031467713861"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.656199686229229,
            "unit": "ns",
            "range": "± 0.12796281841257662"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 10.622960458199183,
            "unit": "ns",
            "range": "± 0.23346128630419166"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 5.684947421153386,
            "unit": "ns",
            "range": "± 0.12877908690308285"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 1.8294359544912975,
            "unit": "ns",
            "range": "± 0.021602790579879114"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 7748.9074783325195,
            "unit": "ns",
            "range": "± 126.93612006342293"
          }
        ]
      }
    ]
  }
}