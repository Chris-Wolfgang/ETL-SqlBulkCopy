using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Wolfgang.Etl.TestKit.Xunit;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

public class SqlBulkCopyLoaderSupportsDryRunTests
    : SupportsDryRunContractTests<SqlBulkCopyLoader<TestRecord>>
{
    protected override SqlBulkCopyLoader<TestRecord> CreateSut()
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        return new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer: null);
    }



    protected override async Task<bool> RunAndReportSideEffectAsync(bool isDryRun)
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer: null)
        {
            IsDryRun = isDryRun
        };

        await sut
            .LoadAsync(ToAsyncEnumerableAsync(new[] { new TestRecord { Id = 1, Name = "A", Amount = 10m } }))
            .ConfigureAwait(false);

        // The external side effect is the bulk insert: a fake wrapper records a
        // batch only when WriteToServerAsync actually ran.
        return factory.CreatedWrappers.Any(w => w.BatchRowCounts.Count > 0);
    }



    [Fact]
    public async Task Dry_run_still_exercises_mapping_and_surfaces_getter_errors()
    {
        // A dry run must still pull every value (so mapping / value-extraction
        // errors surface); it only skips the write. A property whose getter
        // throws must therefore still fault the dry run.
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var sut = new SqlBulkCopyLoader<ThrowingGetterRecord>(factory, logger: null, timer: null)
        {
            IsDryRun = true
        };

        var ex = await Assert.ThrowsAnyAsync<Exception>
        (
            () => sut.LoadAsync(ToAsyncEnumerableAsync(new[] { new ThrowingGetterRecord { Id = 1 } }))
        );

        Assert.Contains("mapping boom", ex.ToString(), StringComparison.Ordinal);
    }



    private static async IAsyncEnumerable<T> ToAsyncEnumerableAsync<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }



    private sealed class ThrowingGetterRecord
    {
        public int Id { get; set; }

        public int Boom => throw new InvalidOperationException("mapping boom");
    }
}
