using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Wolfgang.Etl.SqlBulkCopy.Benchmarks;

/// <summary>
/// End-to-end loader throughput: streams N records through
/// <see cref="SqlBulkCopyLoader{T}.LoadAsync(System.Collections.Generic.IAsyncEnumerable{T})"/>
/// into a <see cref="NoOpSqlBulkCopyWrapper"/>. This exercises the real primary
/// path — type-map build, batch slicing, per-row compiled-getter mapping,
/// <c>DbDataReader</c> projection — without a SQL Server, so the measurement is
/// the loader's own cost rather than network/database time.
/// </summary>
[MemoryDiagnoser]
public class LoaderBenchmarks
{
    [Table("BenchRows", Schema = "dbo")]
    public sealed record BenchRow
    {
        public int Id { get; init; }

        [Column("FullName")]
        public string Name { get; init; } = string.Empty;

        public decimal Amount { get; init; }

        public DateTime CreatedUtc { get; init; }
    }

    private BenchRow[] _rows = Array.Empty<BenchRow>();

    [Params(1_000, 100_000)]
    public int RecordCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _rows = new BenchRow[RecordCount];
        for (var i = 0; i < RecordCount; i++)
        {
            _rows[i] = new BenchRow
            {
                Id = i,
                Name = "Customer",
                Amount = i * 1.25m,
                CreatedUtc = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            };
        }
    }

    [Benchmark]
    public async Task LoadAsync()
    {
        var loader = new SqlBulkCopyLoader<BenchRow>(new NoOpSqlBulkCopyWrapperFactory(), logger: null, timer: null);
        await loader.LoadAsync(ToAsyncEnumerable(_rows));
    }

    // Synchronous source exposed as IAsyncEnumerable for LoadAsync. No await on
    // the enumerated path keeps the measured allocation/latency free of extra
    // await-state-machine work; CS1998 is expected and suppressed.
#pragma warning disable CS1998
    private static async IAsyncEnumerable<BenchRow> ToAsyncEnumerable(BenchRow[] rows)
    {
        foreach (var row in rows)
        {
            yield return row;
        }
    }
#pragma warning restore CS1998
}
