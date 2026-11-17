using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Wolfgang.Etl.TestKit.Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

public class SqlBulkCopyLoaderDisposableTests
    : DisposableStageContractTests<SqlBulkCopyLoader<TestRecord>>
{
    protected override SqlBulkCopyLoader<TestRecord> CreateSut()
    {
        return new SqlBulkCopyLoader<TestRecord>
        (
            new FakeSqlBulkCopyWrapperFactory(),
            timer: null,
            logger: null,
            options: null
        );
    }



    protected override async Task<bool> InvokeReportsObjectDisposedAsync(bool disposeFirst, bool useAsyncDispose)
    {
        var sut = CreateSut();

        if (disposeFirst)
        {
            if (useAsyncDispose)
            {
                await sut.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                sut.Dispose();
            }
        }

        try
        {
            await sut.LoadAsync(EmptyAsync()).ConfigureAwait(false);
            return false;
        }
        catch (ObjectDisposedException)
        {
            return true;
        }
    }



    private static async IAsyncEnumerable<TestRecord> EmptyAsync()
    {
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }
}
