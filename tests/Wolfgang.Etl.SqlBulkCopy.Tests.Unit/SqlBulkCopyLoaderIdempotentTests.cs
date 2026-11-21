using System.Collections.Generic;
using System.Linq;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Wolfgang.Etl.TestKit.Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

public class SqlBulkCopyLoaderIdempotentTests
    : IdempotentLoaderContractTests<SqlBulkCopyLoader<TestRecord>, TestRecord, SqlBulkCopyReport>
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
        return new SqlBulkCopyLoader<TestRecord>
        (
            new FakeSqlBulkCopyWrapperFactory(),
            timer: null,
            logger: null
        );
    }



    protected override IReadOnlyList<TestRecord> CreateSourceItems() => SourceItems;



    // The loader writes to the database and keeps no collection of loaded items, so the
    // contract's collection check is skipped.
    protected override IReadOnlyList<TestRecord>? TryGetLoadedItems(SqlBulkCopyLoader<TestRecord> sut) => null;
}
