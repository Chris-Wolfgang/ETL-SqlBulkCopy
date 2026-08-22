window.BENCHMARK_DATA = {
  "lastUpdate": 1787425764129,
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
      }
    ]
  }
}