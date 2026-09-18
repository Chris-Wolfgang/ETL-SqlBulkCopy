using System.Collections.Generic;
using System.Linq;
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



    protected override SqlBulkCopyLoader<TestRecord> CreateSut(int itemCount, int maximumItemCount, int skipItemCount, int reportingInterval)
    {
        var factory = new FakeSqlBulkCopyWrapperFactory();
        var options = new SqlBulkCopyLoaderOptions<TestRecord>
        {
            MaximumItemCount = maximumItemCount,
            SkipItemCount = skipItemCount,
            ReportingInterval = reportingInterval,
        };
        return new SqlBulkCopyLoader<TestRecord>(factory, timer: null, logger: null, options: options);
    }



    protected override IReadOnlyList<TestRecord> CreateSourceItems() => SourceItems;
}
