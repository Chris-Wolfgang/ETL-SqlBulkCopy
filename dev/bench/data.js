window.BENCHMARK_DATA = {
  "lastUpdate": 1789783331587,
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
          "id": "1737cfcd67de8124e1b00204976677bcdc18d043",
          "message": "Merge pull request #228 from Chris-Wolfgang/docs/txn-examples-and-dryrun\n\ndocs: transaction-control examples (#217) + dry-run README note",
          "timestamp": "2026-08-09T17:01:59-04:00",
          "tree_id": "62381536c03e6928d019132b5702b3406caf94a8",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/1737cfcd67de8124e1b00204976677bcdc18d043"
        },
        "date": 1786309467472,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 76597.3848470052,
            "unit": "ns",
            "range": "± 327.1297486650393"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7389332.34375,
            "unit": "ns",
            "range": "± 34116.135892702274"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 8.391562094291052,
            "unit": "ns",
            "range": "± 0.015792393987906495"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.663954516251882,
            "unit": "ns",
            "range": "± 0.014365772553997078"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 13.567543377478918,
            "unit": "ns",
            "range": "± 0.1349960641836608"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 14.114603757858276,
            "unit": "ns",
            "range": "± 0.4285993421042096"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.7248854214946427,
            "unit": "ns",
            "range": "± 0.0018185291322231935"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 11588.040079752604,
            "unit": "ns",
            "range": "± 207.8229413152931"
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
          "id": "ada89d0b2165465cac15e9aace3f3c92ff003ce4",
          "message": "Merge pull request #230 from Chris-Wolfgang/chore/pkgvalidation-baseline-0.5.0\n\nchore: bump PackageValidation baseline to 0.5.0 (post-release)",
          "timestamp": "2026-08-09T21:18:27-04:00",
          "tree_id": "a337e4473b3901913fd8be43054e1d9b97e4a77f",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/ada89d0b2165465cac15e9aace3f3c92ff003ce4"
        },
        "date": 1786324845958,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 79784.57751464844,
            "unit": "ns",
            "range": "± 1694.36943004772"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7431469.40625,
            "unit": "ns",
            "range": "± 51328.46789505602"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 8.24125832815965,
            "unit": "ns",
            "range": "± 0.17280633369482035"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.6785468608140945,
            "unit": "ns",
            "range": "± 0.013013683888839977"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 14.67307049036026,
            "unit": "ns",
            "range": "± 0.4251198463275627"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 13.255679170290628,
            "unit": "ns",
            "range": "± 0.9036459892640082"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.720708886782328,
            "unit": "ns",
            "range": "± 0.006694394119194869"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 11461.53416188558,
            "unit": "ns",
            "range": "± 372.4382657090721"
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
          "id": "ad448df5849a02e583d76f0a6b7c915367182e07",
          "message": "Merge pull request #261 from Chris-Wolfgang/vNext\n\nRelease 0.6.0",
          "timestamp": "2026-08-12T20:31:31-04:00",
          "tree_id": "f175c2936220326e1a7c3f423e7aa74485ee9728",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/ad448df5849a02e583d76f0a6b7c915367182e07"
        },
        "date": 1786581234574,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 77210.79870605469,
            "unit": "ns",
            "range": "± 234.91786812868762"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7517283.778645833,
            "unit": "ns",
            "range": "± 83656.41026731675"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 8.111568426092466,
            "unit": "ns",
            "range": "± 0.008035762498124413"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.6612821569045385,
            "unit": "ns",
            "range": "± 0.0033387566678574282"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 12.957351018985113,
            "unit": "ns",
            "range": "± 0.05778194977683185"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 15.689265916744867,
            "unit": "ns",
            "range": "± 0.878368898750592"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.7251300886273384,
            "unit": "ns",
            "range": "± 0.008615474679693187"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 10622.496047973633,
            "unit": "ns",
            "range": "± 36.80364251784241"
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
          "id": "972793bf8c196d37890fe5d43aabb122c8dbef09",
          "message": "Merge pull request #265 from Chris-Wolfgang/chore/pkgvalidation-baseline-0.6.0-v2\n\nchore: bump PackageValidation baseline 0.5.0 -> 0.6.0 (post-release)",
          "timestamp": "2026-08-12T21:27:12-04:00",
          "tree_id": "9dd02789ae2df4a1253766ae7d3f1adbeaa8eb4c",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/972793bf8c196d37890fe5d43aabb122c8dbef09"
        },
        "date": 1786584584028,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 90666.53023274739,
            "unit": "ns",
            "range": "± 344.3756692546741"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 9423789.6875,
            "unit": "ns",
            "range": "± 57779.33344851883"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 6.951484476526578,
            "unit": "ns",
            "range": "± 0.05367471235297203"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 1.0819644778966904,
            "unit": "ns",
            "range": "± 0.0004898397279285951"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 12.046142041683197,
            "unit": "ns",
            "range": "± 0.08222470879915228"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 6.580554624398549,
            "unit": "ns",
            "range": "± 0.08364494568317128"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.3020185033480325,
            "unit": "ns",
            "range": "± 0.025336114053617898"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 9595.412170410156,
            "unit": "ns",
            "range": "± 98.23760308446307"
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
          "id": "cf0c589c35fe8380fdd0e42706ac441d5c72bae9",
          "message": "Merge pull request #271 from Chris-Wolfgang/vNext\n\nRelease 0.7.0",
          "timestamp": "2026-08-14T07:59:33-04:00",
          "tree_id": "97a4963fd78e2fd349a20fab00fdc8e9073e257f",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/cf0c589c35fe8380fdd0e42706ac441d5c72bae9"
        },
        "date": 1786708913234,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 79095.21610514323,
            "unit": "ns",
            "range": "± 586.692692460388"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7489375.729166667,
            "unit": "ns",
            "range": "± 28693.060320686403"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 8.159408787886301,
            "unit": "ns",
            "range": "± 0.027808912626776733"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.666482704381148,
            "unit": "ns",
            "range": "± 0.004837284424428658"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 12.969779272874197,
            "unit": "ns",
            "range": "± 0.10258438700277143"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 16.283306578795116,
            "unit": "ns",
            "range": "± 0.5751150938488822"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.725408859550953,
            "unit": "ns",
            "range": "± 0.0028264312551333636"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 10720.284968058268,
            "unit": "ns",
            "range": "± 65.20925267441238"
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
          "id": "e36628696b2eef4c7e19c8cc95abc625aa465958",
          "message": "Merge pull request #273 from Chris-Wolfgang/fix/assemblyversion-0.7.0\n\nfix: bump AssemblyVersion/FileVersion to 0.7.0",
          "timestamp": "2026-08-14T08:35:22-04:00",
          "tree_id": "6b2fb42850fd6ee27c1a070d4b6148580449bee0",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/e36628696b2eef4c7e19c8cc95abc625aa465958"
        },
        "date": 1786711075303,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 78846.43294270833,
            "unit": "ns",
            "range": "± 976.4132965007019"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7674410.729166667,
            "unit": "ns",
            "range": "± 10571.83940074791"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 7.456977461775144,
            "unit": "ns",
            "range": "± 0.003651491566663531"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.5229473536213239,
            "unit": "ns",
            "range": "± 0.22246973086442987"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 13.505004664262136,
            "unit": "ns",
            "range": "± 0.19649019795549214"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 7.503371179103851,
            "unit": "ns",
            "range": "± 0.3430037650847477"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.0491672133406005,
            "unit": "ns",
            "range": "± 0.003995715802739961"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 12044.395365397135,
            "unit": "ns",
            "range": "± 330.7215290149921"
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
          "id": "a20d5f4ea9d5e2cefce3fa4f4022c1bf6a8193b3",
          "message": "Merge pull request #274 from Chris-Wolfgang/chore/baseline-0.7.0\n\nchore: bump PackageValidationBaselineVersion to 0.7.0",
          "timestamp": "2026-08-14T15:21:16-04:00",
          "tree_id": "4c07d0f0f05da034956b16ae188af9cc7a4bf54f",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/a20d5f4ea9d5e2cefce3fa4f4022c1bf6a8193b3"
        },
        "date": 1786735422875,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 78155.19474283855,
            "unit": "ns",
            "range": "± 796.8160403679019"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7493048.46875,
            "unit": "ns",
            "range": "± 17211.18651992314"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 8.230550080537796,
            "unit": "ns",
            "range": "± 0.08557953439086226"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.7722278734048208,
            "unit": "ns",
            "range": "± 0.12454421152989877"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 12.944409598906836,
            "unit": "ns",
            "range": "± 0.05908269818416002"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 16.562500456968944,
            "unit": "ns",
            "range": "± 1.3367736100188572"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.7239862556258836,
            "unit": "ns",
            "range": "± 0.0011542682851596133"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 10877.528788248697,
            "unit": "ns",
            "range": "± 129.60778726538123"
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
          "id": "34947b877d127898440607dbebd584b039bb6df7",
          "message": "Merge pull request #278 from Chris-Wolfgang/dependabot/nuget/dotnet-dependencies-f0d6a19fe6\n\nBump the dotnet-dependencies group with 7 updates",
          "timestamp": "2026-08-19T12:11:19-04:00",
          "tree_id": "9c954be33ab8e3768891fc879e9802ea16c19a16",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/34947b877d127898440607dbebd584b039bb6df7"
        },
        "date": 1787156052482,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 78960.42655436198,
            "unit": "ns",
            "range": "± 532.7113829859911"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7621373.390625,
            "unit": "ns",
            "range": "± 8860.216706708914"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 7.649363279342651,
            "unit": "ns",
            "range": "± 0.012508748229687533"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0,
            "unit": "ns",
            "range": "± 0"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 13.538799405097961,
            "unit": "ns",
            "range": "± 0.09187541252792027"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 8.744152093927065,
            "unit": "ns",
            "range": "± 0.4701236440759325"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 1.7837495505809784,
            "unit": "ns",
            "range": "± 0.0006512616850529503"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 12065.699513753256,
            "unit": "ns",
            "range": "± 68.51316733399065"
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
          "id": "19fe599cccb46bd57ebe6e7bdd3c933dd514a5bf",
          "message": "Merge pull request #286 from Chris-Wolfgang/vNext\n\nRelease v0.7.1 — 999 → 0 code-scanning alerts (drop-in PATCH)",
          "timestamp": "2026-08-22T15:06:48-04:00",
          "tree_id": "5379121228371e1b24d3c52e785b1514cd6de04a",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/19fe599cccb46bd57ebe6e7bdd3c933dd514a5bf"
        },
        "date": 1787425762198,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 81734.16251627605,
            "unit": "ns",
            "range": "± 1155.8176339689685"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7623187.619791667,
            "unit": "ns",
            "range": "± 2333.8618691219576"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 7.801013479630153,
            "unit": "ns",
            "range": "± 0.009616223114552876"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.030512763808170956,
            "unit": "ns",
            "range": "± 0.0026430485004718916"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 13.895976603031158,
            "unit": "ns",
            "range": "± 0.1048856811973013"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 6.401508102814357,
            "unit": "ns",
            "range": "± 0.122760219639093"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.138751675685247,
            "unit": "ns",
            "range": "± 0.006608480913826788"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 12099.76166788737,
            "unit": "ns",
            "range": "± 92.58913017046682"
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
          "id": "b227285837f8e1570cc52a304ccb4fb3959b56fa",
          "message": "Merge pull request #291 from Chris-Wolfgang/vNext\n\nRelease v0.7.2 — post-v0.7.1 alert cleanup (30 → 0)",
          "timestamp": "2026-08-23T09:52:19-04:00",
          "tree_id": "eee9c7fae29b7078a2afeb8da7abb2a2c861734e",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/b227285837f8e1570cc52a304ccb4fb3959b56fa"
        },
        "date": 1787493279057,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 80614.86474609375,
            "unit": "ns",
            "range": "± 479.79937400930146"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7699112.46875,
            "unit": "ns",
            "range": "± 15254.263120929469"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 7.831474348902702,
            "unit": "ns",
            "range": "± 0.07338883708099854"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.8394185428818067,
            "unit": "ns",
            "range": "± 0.2035600542979072"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 14.219717035690943,
            "unit": "ns",
            "range": "± 0.13653184542211033"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 6.916635577877362,
            "unit": "ns",
            "range": "± 0.25602246125444955"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.0473960960904756,
            "unit": "ns",
            "range": "± 0.0037201609902237157"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 12123.727663675943,
            "unit": "ns",
            "range": "± 35.900620202738935"
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
          "id": "843209fb3d6fd41f1afebd78143b7c2c32bcb24f",
          "message": "Merge pull request #293 from Chris-Wolfgang/chore/baseline-0.7.2\n\nchore(pack): advance PackageValidation baseline to 0.7.2",
          "timestamp": "2026-08-23T15:41:05-04:00",
          "tree_id": "96d7eed5aa90b5f7ca89f0b1ac7036b1f5ac8ac7",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/843209fb3d6fd41f1afebd78143b7c2c32bcb24f"
        },
        "date": 1787514207429,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 80027.62670898438,
            "unit": "ns",
            "range": "± 297.4507350551344"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7650687.236979167,
            "unit": "ns",
            "range": "± 24840.48691697774"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 7.504176164666812,
            "unit": "ns",
            "range": "± 0.12001070485738"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.8895547340313593,
            "unit": "ns",
            "range": "± 0.2452553265291902"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 13.123201409975687,
            "unit": "ns",
            "range": "± 0.2889904867906921"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 7.53842677672704,
            "unit": "ns",
            "range": "± 0.6793452186603002"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.125576987862587,
            "unit": "ns",
            "range": "± 0.08947013387832853"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 11698.830098470053,
            "unit": "ns",
            "range": "± 74.6284399106023"
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
          "id": "7a36a80a0ce9b769b0e8ebabfa387f425f3efe1b",
          "message": "Release v0.8.0 — SqlBulkCopyLoaderOptions<T> record and the options constructor; 13 setters deprecated (#313)\n\n* chore(analyzers): resolve 4 post-v0.7.2 residuals — record PrintMembers + RecordCount disable-pair (#263)\n\nFresh scan on main after v0.7.2 tag surfaced 4 alerts I missed:\n\n3 x RS0016 — record-inherent PrintMembers() overrides. Records\nauto-generate BOTH ToString() AND PrintMembers() (the latter is a\nprotected virtual/override that ToString delegates to). v0.7.2 tracked\nToString but not PrintMembers. This is exactly the class of trap\nreference_record_publicapi_needs_tostring_too flags — \"The build is\nthe ground truth\"; I should have re-built after the ToString fold to\nenumerate remaining record members. Added to PublicAPI.Unshipped.txt:\n  - override Wolfgang.Etl.SqlBulkCopy.SqlBulkCopyReport.PrintMembers(...)\n  - virtual Wolfgang.Etl.SqlBulkCopy.PostLoadActionParameters.PrintMembers(...)\n  - virtual Wolfgang.Etl.SqlBulkCopy.PreLoadActionParameters.PrintMembers(...)\n\n1 x UnusedAutoPropertyAccessor.Global — ShadowWorkloads.RecordCount.set.\nThe inline `// ReSharper disable once UnusedAutoPropertyAccessor.Global`\ncomment I added in #289 sat two lines above the property; the\nintervening [Params(...)] attribute meant the `once` scope applied to\nthe attribute line, not the property. Replaced with an explicit\ndisable/restore pair around both the attribute and the property.\n\nZero behavior change; releases as PATCH in the next cycle.\n\nRefs #263.\n\nCo-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>\n\n* feat: make logger an optional trailing ctor parameter, defaulting to NullLogger\n\nAligns SqlBulkCopyLoader<T> with the fleet-wide constructor convention (logger\nalways last, always optional) already followed by Etl-DbClient.\n\n  (SqlConnection, ILogger<T> logger)\n    -> (SqlConnection, ILogger<T>? logger = null)\n\nnull (or omitted) now resolves to NullLogger.Instance instead of throwing\nArgumentNullException.\n\nNot a breaking change: the parameter list is unchanged, so the emitted\nsignature is identical. Release build with TreatWarningsAsErrors is clean and\nPackageValidation passes, so the PublicAPI.Shipped.txt entry was corrected in\nplace rather than recorded as an add/remove pair.\n\nThe 4-parameter (SqlConnection, SqlBulkCopyOptions, SqlTransaction?, ILogger<T>?)\nconstructor already conformed and is untouched, as is the 1-argument\n(SqlConnection) constructor - removing the latter would be a binary break, since\noptional-argument defaults are baked in at the caller's compile time.\n\nNo new ambiguity: (SqlConnection) still wins overload resolution for a\nsingle-argument call because all of its parameters have a corresponding\nargument, and SqlBulkCopyOptions is an enum so an ILogger argument is not\nconvertible to it.\n\nTest: the one test asserting a null logger throws now asserts the NullLogger\ncontract. All suites pass in Release with TreatWarningsAsErrors across every\ntarget framework (331 unit per TFM, plus fuzz / snapshots / concurrency /\nintegration / doc-examples).\n\nRefs Chris-Wolfgang/ETL-SqlBulkCopy#252\n\nCo-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>\n\n* chore: put the logger last on the internal test-injection ctor\n\nApplies Rule 6 of the fleet constructor standard: the logger is the final\nparameter on EVERY constructor, internal ones included.\n\n  internal SqlBulkCopyLoader(wrapperFactory, ILogger? logger,\n                             IProgressTimer? timer, ISqlCommandExecutor? executor = null)\n    -> internal SqlBulkCopyLoader(wrapperFactory, IProgressTimer? timer,\n                                  ISqlCommandExecutor? executor = null, ILogger? logger = null)\n\nThis constructor injects a second test dependency after the timer, so the\nlogger moves past both rather than merely swapping with the timer - the rule is\n\"logger last\", not \"logger second-to-last\".\n\nInternal-only: no public API change, no PublicAPI entry, no consumer impact and\nnothing to deprecate. Call sites across six test files updated, covering both\nthe 3-argument and 4-argument forms; the logger is now passed by name to keep\nthe intent obvious at each site.\n\nAll suites pass in Release with TreatWarningsAsErrors across every target\nframework - 331 unit tests per TFM, plus fuzz, snapshots and concurrency.\n\nCo-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>\n\n* feat(options): SqlBulkCopyLoaderOptions<T> : LoaderOptions and the options constructor (ADR-0009, part 1)\n\nA new options record for the loader, which had no options type at all:\nevery settable property has a { get; init; } member of the same name\nand default (docs lifted from the loader), BulkCopyOptions carries the\nSqlBulkCopyOptions flags, and the record derives from LoaderOptions so\nReportingInterval / SkipItemCount / MaximumItemCount / ErrorPolicy are\nconfigured there too.\n\nDestinationTableName / DestinationSchemaName sit on the record (nullable\noverrides with an attribute fallback, so configuration, not identity);\nthe SqlTransaction stays a constructor parameter, like DbClient's\nDbTransaction. That settles the two #303 design questions and #252.\n\nNew (connection, options = null, transaction = null, logger = null)\nconstructor chaining base(options) is the single initialization path;\nthe three public constructors and the internal test-injection one chain\ninto it. ApplyOptions copies members in declaration order so the\nsetters' own range checks fire at construction. Public signatures are\nunchanged.\n\nISupportDryRun dropped (removed in 0.24); CompatibilitySuppressions.xml\nnew here (CP0008 x5). Family 0.23.2 -> 0.24.0. PublicAPI: 38 entries;\n25 pre-existing unrecorded record members left out, tracked in #310.\nDry-run contract test on the non-generic TestKit base;\nSqlBulkCopyLoaderOptionsRecordTests adds 10 cases. Setters stay live;\npart 2 deprecates them.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>\n\n* feat(options): deprecate the 13 setters, move every caller onto the record, README + migration guide (ADR-0009, part 2) (#312)\n\nThe setter of each configurable SqlBulkCopyLoader<T> property is\n[Obsolete] on the accessor (reads stay clean), pointing at\nSqlBulkCopyLoaderOptions<T>; ApplyOptions writes them under one CS0618\npragma as the supported replacement.\n\nEvery internal caller moves onto the record: 45 object-initializer\nsites across the unit / integration / mutation / dry-run tests, the AOT\nquickstart and the shadow-workload benchmark, folded one site at a time\nwith a balanced-brace scanner and reviewed in the diff. The CreateSut\nhelpers take an optional record so the configure-then-run facts pass it\ninstead of assigning after construction; the seven facts that exercise\nthe setters themselves sit under a single pragma region and retire with\nthem.\n\nREADME \"Configuring the loader\" + quick start on the record; docfx\ngetting-started examples; class-level example. docs/migrations/\nv0.7-to-v0.8.md is the first guide here. CHANGELOG Deprecated. No\nPublicAPI text change.\n\nCo-authored-by: Claude Opus 5 <noreply@anthropic.com>\n\n* release: v0.8.0\n\nSqlBulkCopyLoaderOptions<T> record and the options constructor; 13 setters deprecated MINOR bump from v0.7.2: new public surface (options records inheriting the Abstractions 0.24.0 base records, new constructors) and new [Obsolete] markers; no removals.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>\n\n* fix(tools): GcProfileWorkload configures the loader through the record (CS0618 under TreatWarningsAsErrors)\n\ntools/GcProfileWorkload is not in the solution, so #312's fold missed it; Stage 1 on the release PR builds it and failed on the two deprecated setters.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>\n\n* fix: FileVersion 0.8.0; docs, README, migration wording; SqlClient guard on the record tests; indentation (review on #313)\n\n- FileVersion follows Version and AssemblyVersion on the 0.x line (ADR-0005).\n- Internal ctor <param> tags in declaration order; SqlBulkCopyLoaderOptions remark\n  no longer claims BulkCopyOptions mirrors a loader property.\n- Migration guide: \"no loader member is removed\" and names the ISupportDryRun\n  removal; README common-pattern bullets configure through the record.\n- SqlBulkCopyLoaderOptionsRecordTests constructs SqlConnection behind the same\n  constructibility guard as SqlBulkCopyLoaderTests ([SkippableFact]).\n- PostActionIntegrationTests options block re-indented (fold artifact).\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>\n\n---------\n\nCo-authored-by: Claude Opus 4.7 <noreply@anthropic.com>",
          "timestamp": "2026-09-16T21:32:58-04:00",
          "tree_id": "51e8a546b7a0f06dad61685ea134aefae3d5708d",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/7a36a80a0ce9b769b0e8ebabfa387f425f3efe1b"
        },
        "date": 1789608935349,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 81887.67659505208,
            "unit": "ns",
            "range": "± 661.7379930335466"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7607596.815104167,
            "unit": "ns",
            "range": "± 189101.080041947"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 8.208730970819792,
            "unit": "ns",
            "range": "± 0.10632295728589845"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.3492332473397255,
            "unit": "ns",
            "range": "± 0.00446170923769402"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 13.614786426226297,
            "unit": "ns",
            "range": "± 0.14845393841424567"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 16.418104102214176,
            "unit": "ns",
            "range": "± 2.271808937825086"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.7253767400979996,
            "unit": "ns",
            "range": "± 0.006298675521219419"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 11354.881871541342,
            "unit": "ns",
            "range": "± 149.752926631191"
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
          "id": "159a3c9fdda224f7e1ccec21f83e9f713c12f536",
          "message": "ci: pin every workflow action to a commit SHA with an exact # vX.Y.Z comment (#314)\n\nRan repo-template's scripts/pin-actions.ps1 -PinTags: tag references become\nSHA pins and major-only comments (# v7) become the exact tag on the pinned\ncommit (# v7.0.1), so zizmor's ref-version-mismatch stops firing when the\nmajor tag moves on. Only the ref/comment text changed. Dependabot keeps the\nprecision it finds, so this stays converted.\n\n51 already exact, 40 line(s) rewritten, 0 tag reference(s), 0 pinned SHA(s) with no tag\n\nRefs Chris-Wolfgang/repo-template#447\n\nCo-authored-by: Chris Wolfgang <cwolfgan@ptd.net>\nCo-authored-by: Claude Opus 5 <noreply@anthropic.com>",
          "timestamp": "2026-09-16T22:55:56-04:00",
          "tree_id": "f1fcd290fd13bf8709332b7386dc694e7bf3d150",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/159a3c9fdda224f7e1ccec21f83e9f713c12f536"
        },
        "date": 1789613898829,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 66976.99430338542,
            "unit": "ns",
            "range": "± 426.30246649158255"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 6046303.65625,
            "unit": "ns",
            "range": "± 91252.16825052598"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 6.0715134516358376,
            "unit": "ns",
            "range": "± 0.004394845775537243"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.3508137396226327,
            "unit": "ns",
            "range": "± 0.15685030274196426"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 10.31837291518847,
            "unit": "ns",
            "range": "± 0.1792463327504402"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 6.236482592920463,
            "unit": "ns",
            "range": "± 0.22986009092585818"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 1.5213800917069118,
            "unit": "ns",
            "range": "± 0.24073696260590677"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 9631.705683390299,
            "unit": "ns",
            "range": "± 160.47712665534786"
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
          "id": "531587646d79c0066bb854f749678346bd045a90",
          "message": "chore(pack): advance PackageValidation baseline to 0.8.0 (#315)\n\nv0.8.0 is published and indexed on nuget.org; CompatibilitySuppressions.xml regenerated against it (the ADR-0009 entries were one-release-lived and are now pruned, as PackageValidation requires).\n\nCo-authored-by: Claude Opus 5 <noreply@anthropic.com>",
          "timestamp": "2026-09-17T13:54:31-04:00",
          "tree_id": "69f52a286173d2db2e451b0424eea6448ae9b653",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/531587646d79c0066bb854f749678346bd045a90"
        },
        "date": 1789667834465,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 79868.10888671875,
            "unit": "ns",
            "range": "± 397.0870352935394"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7650435.302083333,
            "unit": "ns",
            "range": "± 16252.592135395731"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 8.182711581389109,
            "unit": "ns",
            "range": "± 0.09418575733111911"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.40708021198709804,
            "unit": "ns",
            "range": "± 0.0953064807652278"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 14.247888704140982,
            "unit": "ns",
            "range": "± 0.12263923330006711"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 6.657509182890256,
            "unit": "ns",
            "range": "± 0.07417066224889408"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.7355752189954123,
            "unit": "ns",
            "range": "± 0.01013806406028692"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 12129.222854614258,
            "unit": "ns",
            "range": "± 144.99740738366097"
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
          "id": "0c4f5c78de7d83f3149564e37159652af4a4a8b3",
          "message": "fix(tfm): ship net5.0/net6.0/net7.0 assemblies (inherited init-setter modreq hazard) (#319)\n\n* fix(test): restore test discovery on netcoreapp3.1 and net5.0\n\nSame defect as Chris-Wolfgang/Etl-Csv#254 and Chris-Wolfgang/ETL-FixedWidth#336: xunit.runner.visualstudio 2.8.2 ships build/lib assets for net462 and net6.0 only, so the netcoreapp3.1 and net5.0 slots loaded no test adapter and ran zero tests (\"No test is available\") while the suite still read as clean. Pin 2.4.5 on those two slots, 2.8.2 elsewhere, both capped below 3.0.0.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>\n\n* test: skip the SkippableFact-based tests on netcoreapp3.1 instead of failing them\n\nRestoring discovery on netcoreapp3.1 surfaced 14 failures in the two SkippableFact-based classes: Xunit.SkippableFact probes SupportedOSPlatformAttribute while building each test case, and that type is .NET 5+, so on 3.1 the case fails during initialisation before the test (or its own Skip.IfNot guard) runs. Bumping to 1.5.85 does not change it. On netcoreapp3.1 only, alias SkippableFact/SkippableTheory to Fact/Theory subclasses that carry a Skip reason; the tests still run on the other twelve target frameworks.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>\n\n* test: only the connection-dependent tests keep [SkippableFact]\n\nReview catch on #318: the netcoreapp3.1 alias skipped every SkippableFact test in the file, including two that never touch SqlConnection (SqlBulkCopyLoaderOptions_derives_from_LoaderOptions, Internal_constructor_applies_the_record_too). Those are plain [Fact] again and run on all thirteen target frameworks; 11 connection-dependent cases keep the alias.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>\n\n* fix(tfm): ship net5.0/net6.0/net7.0 assemblies — inherited init setters would fail on .NET 5-7\n\nWolfgang.Etl.Abstractions ships per-runtime assemblies, and an init-only setter's IsExternalInit modreq has a different identity in its netstandard2.0 build (internal polyfill) and its net5.0+ builds (System.Runtime). This package's netstandard2.0 assembly is compiled against the former but, on .NET 5/6/7, runs beside the latter, so any write to an inherited options-record property from this assembly throws MissingMethodException — the defect Etl-Csv 0.9.0 hit in its release gate (Chris-Wolfgang/Etl-Csv#287). Nothing in this repo writes one today; the extra targets make it impossible to reintroduce. Same remedy as Abstractions, Etl-DbClient and Etl-Csv.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>\n\n* docs: changelog fragment instead of a CHANGELOG.md edit; framework lists in README and the docfx guide\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>\n\n---------\n\nCo-authored-by: Claude Opus 5 <noreply@anthropic.com>",
          "timestamp": "2026-09-17T19:53:03-04:00",
          "tree_id": "af222bca5363a4810ab9c27e5dddc4d6c01915b2",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/0c4f5c78de7d83f3149564e37159652af4a4a8b3"
        },
        "date": 1789689332826,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 77662.33435058594,
            "unit": "ns",
            "range": "± 215.37962832133425"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7629202.015625,
            "unit": "ns",
            "range": "± 38727.1011948054"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 7.47278293967247,
            "unit": "ns",
            "range": "± 0.047336774296022"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.7256197606523832,
            "unit": "ns",
            "range": "± 0.0022822885798090054"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 13.347455044587454,
            "unit": "ns",
            "range": "± 0.11561531364945465"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 7.318750205139319,
            "unit": "ns",
            "range": "± 0.24948471113176932"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 1.7847177957495053,
            "unit": "ns",
            "range": "± 0.0040243687593609035"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 11686.231399536133,
            "unit": "ns",
            "range": "± 4.613461257839862"
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
          "id": "a4486ad59dfc32891ce6c449c3e55b3bc77e2204",
          "message": "chore(publicapi): record the synthesized record members; per-TFM split for the covariant <Clone>$ lines (#320)\n\n* fix(test): restore test discovery on netcoreapp3.1 and net5.0\n\nSame defect as Chris-Wolfgang/Etl-Csv#254 and Chris-Wolfgang/ETL-FixedWidth#336: xunit.runner.visualstudio 2.8.2 ships build/lib assets for net462 and net6.0 only, so the netcoreapp3.1 and net5.0 slots loaded no test adapter and ran zero tests (\"No test is available\") while the suite still read as clean. Pin 2.4.5 on those two slots, 2.8.2 elsewhere, both capped below 3.0.0.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>\n\n* test: skip the SkippableFact-based tests on netcoreapp3.1 instead of failing them\n\nRestoring discovery on netcoreapp3.1 surfaced 14 failures in the two SkippableFact-based classes: Xunit.SkippableFact probes SupportedOSPlatformAttribute while building each test case, and that type is .NET 5+, so on 3.1 the case fails during initialisation before the test (or its own Skip.IfNot guard) runs. Bumping to 1.5.85 does not change it. On netcoreapp3.1 only, alias SkippableFact/SkippableTheory to Fact/Theory subclasses that carry a Skip reason; the tests still run on the other twelve target frameworks.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>\n\n* test: only the connection-dependent tests keep [SkippableFact]\n\nReview catch on #318: the netcoreapp3.1 alias skipped every SkippableFact test in the file, including two that never touch SqlConnection (SqlBulkCopyLoaderOptions_derives_from_LoaderOptions, Internal_constructor_applies_the_record_too). Those are plain [Fact] again and run on all thirteen target frameworks; 11 connection-dependent cases keep the alias.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>\n\n* fix(tfm): ship net5.0/net6.0/net7.0 assemblies — inherited init setters would fail on .NET 5-7\n\nWolfgang.Etl.Abstractions ships per-runtime assemblies, and an init-only setter's IsExternalInit modreq has a different identity in its netstandard2.0 build (internal polyfill) and its net5.0+ builds (System.Runtime). This package's netstandard2.0 assembly is compiled against the former but, on .NET 5/6/7, runs beside the latter, so any write to an inherited options-record property from this assembly throws MissingMethodException — the defect Etl-Csv 0.9.0 hit in its release gate (Chris-Wolfgang/Etl-Csv#287). Nothing in this repo writes one today; the extra targets make it impossible to reintroduce. Same remedy as Abstractions, Etl-DbClient and Etl-Csv.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>\n\n* docs: changelog fragment instead of a CHANGELOG.md edit; framework lists in README and the docfx guide\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>\n\n* chore(publicapi): record the synthesized record members that shipped unrecorded; per-TFM split for the covariant <Clone>$ lines\n\nRS0016 is muzzled by the blanket analyzer severity, so the compiler-synthesized members of every shipped record (<Clone>$, copy ctor, Deconstruct, Equals, GetHashCode, ToString, PrintMembers, EqualityContract, ==/!=) have been shipping without an entry in PublicAPI.Shipped.txt. Harvested by raising RS0016 to warning on the project across every target framework and appended to Shipped — they are already public.\n\nThe <Clone>$ of every record that derives from an Abstractions record has a covariant return (the derived type) on net5.0+ but returns the base type on net462 / netstandard2.0, so those lines cannot live in the shared file. Adopts Try-Pattern's layout: PublicApi/modern and PublicApi/legacy PublicAPI.{Shipped,Unshipped}.txt, wired by IsTargetFrameworkCompatible(net5.0) in the csproj; the shared file keeps the TFM-invariant surface. On PublicApiAnalyzers 5.6.0 RS0017 accepts the <Clone>$ lines in the form RS0016 emits, so the blocker recorded in Chris-Wolfgang/Etl-Csv#263 no longer reproduces once the split is in place. Verified: RS0016 raised again reports nothing on any target; plain Release build has no RS0017.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>\n\n* chore: changelog fragment (internal) for the PublicAPI backfill\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>\n\n---------\n\nCo-authored-by: Claude Opus 5 <noreply@anthropic.com>",
          "timestamp": "2026-09-17T20:11:37-04:00",
          "tree_id": "66e3fe7fe2b36f638cb7465dabd165a40abea256",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/a4486ad59dfc32891ce6c449c3e55b3bc77e2204"
        },
        "date": 1789690436984,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 79586.22998046875,
            "unit": "ns",
            "range": "± 679.371589721209"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 8092249.807291667,
            "unit": "ns",
            "range": "± 61829.837101929246"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 8.131091634432474,
            "unit": "ns",
            "range": "± 0.034662270074132695"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.668556809425354,
            "unit": "ns",
            "range": "± 0.008290694383371128"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 12.99340828259786,
            "unit": "ns",
            "range": "± 0.07410587898731273"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 18.623199899991352,
            "unit": "ns",
            "range": "± 0.3320526866948959"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.725768248240153,
            "unit": "ns",
            "range": "± 0.0043487995740440905"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 11509.110951741537,
            "unit": "ns",
            "range": "± 147.53228316450065"
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
          "id": "fa0cc52e6fbee326080c68639ddf3510bdc64937",
          "message": "build: adopt Wolfgang.Etl.Abstractions / TestKit / TestKit.Xunit 0.25.0 (#347)\n\nThe base contract classes now take the base configuration through CreateSut(int itemCount, int maximumItemCount, int skipItemCount, int reportingInterval); every implementer forwards the three values into its options record. Tests that configured a stage through the now-deprecated base setters configure through the record instead; the vestigial CreateSutWithTimer overrides go (Chris-Wolfgang/ETL-Abstractions#372 removes the member next).\n\nVerified locally: Release build 0 errors; unit suites green on net462 / netcoreapp3.1 / net10.0; coverage gate reproduced with no class below 90 %.\n\nCo-authored-by: Claude Opus 5 <noreply@anthropic.com>",
          "timestamp": "2026-09-18T21:02:02-04:00",
          "tree_id": "299f8841b083a7c335518ee14d60f658444cd153",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/fa0cc52e6fbee326080c68639ddf3510bdc64937"
        },
        "date": 1789779865378,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 60887.68395996094,
            "unit": "ns",
            "range": "± 263.5803858277718"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 5959576.997395833,
            "unit": "ns",
            "range": "± 6503.3361709679775"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 5.80574141194423,
            "unit": "ns",
            "range": "± 0.01400781131482673"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.5326073952019215,
            "unit": "ns",
            "range": "± 0.003457700950619659"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 10.81278249869744,
            "unit": "ns",
            "range": "± 0.1380197678484626"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 5.344950492183368,
            "unit": "ns",
            "range": "± 0.1384369605793452"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 1.5921498959263165,
            "unit": "ns",
            "range": "± 0.002091329827498266"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 9315.456756591797,
            "unit": "ns",
            "range": "± 13.778243384356728"
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
          "id": "b430fa51456d35b8d015d478f67b64af06dd2813",
          "message": "refactor: options constructor assigns backing fields, not the deprecated setters (#345) (#349)\n\nEvery `{ get; [Obsolete] set; }` property now has an explicit backing field the constructor / ApplyOptions writes, so the constructor no longer calls the setters it deprecates and the CS0618 pragma blocks that wrapped those writes are dropped (observation-only reads keep theirs). The `BatchSize` (≥ 1) and `BulkCopyTimeout` (≥ 0) guards now also run on the record's init accessors; the two record tests that expected the loader constructor to throw now assert the throw at `new SqlBulkCopyLoaderOptions { … }` and no longer need a SqlConnection.\n\nVerified locally: Release build 0 errors; unit suites green on net462 / net10.0; coverage gate reproduced with no class below 90 %.\n\nCo-authored-by: Claude Opus 5 <noreply@anthropic.com>",
          "timestamp": "2026-09-18T21:38:00-04:00",
          "tree_id": "b43cb1ac5ed4b9e57821bbc4f640229d598ac696",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/b430fa51456d35b8d015d478f67b64af06dd2813"
        },
        "date": 1789782025799,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 76466.13732910156,
            "unit": "ns",
            "range": "± 406.2276887069413"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7447001.143229167,
            "unit": "ns",
            "range": "± 62380.38375740086"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 8.150975639621416,
            "unit": "ns",
            "range": "± 0.012730844508055051"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.6586986730496088,
            "unit": "ns",
            "range": "± 0.0028860176676147806"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 13.084822525580725,
            "unit": "ns",
            "range": "± 0.09012001213830266"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 16.318184693654377,
            "unit": "ns",
            "range": "± 0.07598657956329433"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.762296438217163,
            "unit": "ns",
            "range": "± 0.06733719266044522"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 10539.03618367513,
            "unit": "ns",
            "range": "± 45.62369714392045"
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
          "id": "f36071bf3eaa807882e736d1e0caf9e2e3985886",
          "message": "docs(pack): add THIRD-PARTY-NOTICES.md and ship it in the package (#300) (#351)\n\nAdds the hand-maintained licence notices for the shipped runtime dependencies (Microsoft.Bcl.AsyncInterfaces, Microsoft.Data.SqlClient, Microsoft.Extensions.Logging.Abstractions, System.ComponentModel.Annotations — all MIT; the first-party source generator noted separately) in the Etl-Csv house format, and packs it unconditionally so a missing file fails `dotnet pack`.\n\nCo-authored-by: Claude Opus 5 <noreply@anthropic.com>",
          "timestamp": "2026-09-18T21:59:46-04:00",
          "tree_id": "cb51bd0164f0fffb9a9a78347f026d4e9aee2d8a",
          "url": "https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/commit/f36071bf3eaa807882e736d1e0caf9e2e3985886"
        },
        "date": 1789783328015,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 1000)",
            "value": 80094.2714029948,
            "unit": "ns",
            "range": "± 232.09818826429935"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.LoaderBenchmarks.LoadAsync(RecordCount: 100000)",
            "value": 7606064.6484375,
            "unit": "ns",
            "range": "± 2412.104164657834"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Reference",
            "value": 8.135039508342743,
            "unit": "ns",
            "range": "± 0.02034683487698611"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Reference",
            "value": 0.6672258997956911,
            "unit": "ns",
            "range": "± 0.007632277625883673"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Reflection_Value_Boxed",
            "value": 13.430123627185822,
            "unit": "ns",
            "range": "± 0.044219828687331666"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.PropertyGetterBenchmarks.Compiled_Value_Boxed",
            "value": 12.609529594580332,
            "unit": "ns",
            "range": "± 0.831542650171994"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.FullSpan_FastPath(Size: 10000)",
            "value": 2.7282693212231,
            "unit": "ns",
            "range": "± 0.00402159166895972"
          },
          {
            "name": "Wolfgang.Etl.SqlBulkCopy.Benchmarks.SliceListBenchmarks.PartialSlice_Copy(Size: 10000)",
            "value": 10773.122105916342,
            "unit": "ns",
            "range": "± 39.0861254986209"
          }
        ]
      }
    ]
  }
}