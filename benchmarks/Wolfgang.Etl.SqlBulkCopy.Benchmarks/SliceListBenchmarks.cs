using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Attributes;

namespace Wolfgang.Etl.SqlBulkCopy.Benchmarks;

/// <summary>
/// The batching loop calls <c>SliceList</c> once per batch. The full-span case
/// (offset 0, whole list) returns the source reference with zero allocation and
/// is the common path; the partial case materializes a copy for the final short
/// batch. This benchmark contrasts the two.
/// </summary>
[MemoryDiagnoser]
public class SliceListBenchmarks
{
    private IReadOnlyList<object> _source = Array.Empty<object>();

    [Params(10_000)]
    public int Size { get; set; }

    [GlobalSetup]
    public void Setup() =>
        _source = Enumerable.Range(0, Size).Select(i => (object)i).ToArray();

    [Benchmark(Baseline = true)]
    public IReadOnlyList<object> FullSpan_FastPath() =>
        SqlBulkCopyLoader<LoaderBenchmarks.BenchRow>.SliceList(_source, 0, _source.Count);

    [Benchmark]
    public IReadOnlyList<object> PartialSlice_Copy() =>
        SqlBulkCopyLoader<LoaderBenchmarks.BenchRow>.SliceList(_source, 0, _source.Count / 2);
}
