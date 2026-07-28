using System.Collections.Generic;
using System.Linq;
using Wolfgang.Etl.Abstractions;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Wolfgang.Etl.TestKit.Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

public class SqlBulkCopyLoaderContractTests
    : LoaderBaseContractTests<SqlBulkCopyLoader<TestRecord>, TestRecord, SqlBulkCopyReport>
{
    private static readonly IReadOnlyList<TestRecord> SourceItems = Enumerable
        .Range(1, 10)
        .Select
        (
            i => new TestRecord
            {
                Id = i,
                Name = $"Item{i}",
                Amount = i * 10m,
                Ignored = $"Ignored{i}"
            }
        )
        .ToList();



    protected override SqlBulkCopyLoader<TestRecord> CreateSut(int itemCount)
    {
        // No injected timer: the loader falls through to the base progress-timer
        // seam, which the contract's timer tests drive via ManualProgressTimerCore
        // + WithManualProgressTimer (TestKit 0.22 — CreateSutWithTimer is retired).
        var factory = new FakeSqlBulkCopyWrapperFactory();
        return new SqlBulkCopyLoader<TestRecord>(factory, logger: null, timer: null);
    }



    protected override IReadOnlyList<TestRecord> CreateSourceItems() => SourceItems;
}
