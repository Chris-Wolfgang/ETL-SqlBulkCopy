using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Attributes;

namespace Wolfgang.Etl.SqlBulkCopy.Benchmarks;

/// <summary>
/// The batching loop calls <c>SliceList</c> once per batch. The full-span case
/// (offset 0, whole list) returns the source reference with zero allocation and
/// is the common path; the partial case materializes a copy for a final short
/// batch taken from a non-zero offset (the mid-stream/tail slice a real batch
/// loop produces). This benchmark contrasts the two.
/// </summary>
[MemoryDiagnoser]
public class SliceListBenchmarks
{
    private IReadOnlyList<object> _source = Array.Empty<object>();

    // ReSharper disable once UnusedAutoPropertyAccessor.Global -- BDN
    // populates [Params] properties via reflection, not from source.
    [Params(10_000)]
    public int Size { get; set; }

    [GlobalSetup]
    public void Setup() =>
        _source = Enumerable.Range(0, Size).Select(i => (object)i).ToArray();

    // SliceList is a static generic helper independent of the record type, so
    // this uses SqlBulkCopyLoader<object> rather than coupling to another
    // benchmark's model type.
    [Benchmark(Baseline = true)]
    public IReadOnlyList<object> FullSpan_FastPath() =>
        SqlBulkCopyLoader<object>.SliceList(_source, 0, _source.Count);

    [Benchmark]
    public IReadOnlyList<object> PartialSlice_Copy() =>
        SqlBulkCopyLoader<object>.SliceList(_source, _source.Count / 2, _source.Count / 2);
}
