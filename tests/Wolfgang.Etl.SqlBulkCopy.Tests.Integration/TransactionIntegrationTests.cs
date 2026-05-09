using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Wolfgang.Etl.SqlBulkCopy.Tests.Integration.Fixtures;
using Wolfgang.Etl.SqlBulkCopy.Tests.Integration.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Integration;

[Collection("SqlServer")]
public class TransactionIntegrationTests
{
    private readonly SqlServerFixture _fixture;



    public TransactionIntegrationTests(SqlServerFixture fixture)
    {
        _fixture = fixture;
    }



    private static async IAsyncEnumerable<T> ToAsyncEnumerableAsync<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }



    [Fact]
    public async Task LoadAsync_with_external_transaction_rolled_back_writes_no_rows_Async()
    {
        await using var connection = await _fixture.OpenConnectionAsync().ConfigureAwait(false);
        await TestSchema.DropIfExistsAsync(connection, "[dbo].[Widgets]").ConfigureAwait(false);
        await TestSchema.ExecuteAsync
        (
            connection,
            @"CREATE TABLE [dbo].[Widgets] (
                Id INT NOT NULL,
                WidgetName NVARCHAR(100) NOT NULL,
                Price DECIMAL(18,2) NOT NULL
            )"
        ).ConfigureAwait(false);

        using (var transaction = (SqlTransaction)await connection.BeginTransactionAsync().ConfigureAwait(false))
        {
            var sut = new SqlBulkCopyLoader<WidgetRecord>
            (
                connection,
                SqlBulkCopyOptions.Default,
                transaction
            );

            var items = new[]
            {
                new WidgetRecord { Id = 1, Name = "rolled-back", Price = 1m }
            };

            await sut.LoadAsync(ToAsyncEnumerableAsync(items)).ConfigureAwait(false);

            await transaction.RollbackAsync().ConfigureAwait(false);
        }

        Assert.Equal(0, await TestSchema.CountRowsAsync(connection, "[dbo].[Widgets]").ConfigureAwait(false));
    }



    [Fact]
    public async Task LoadAsync_with_external_transaction_committed_writes_rows_Async()
    {
        await using var connection = await _fixture.OpenConnectionAsync().ConfigureAwait(false);
        await TestSchema.DropIfExistsAsync(connection, "[dbo].[Widgets]").ConfigureAwait(false);
        await TestSchema.ExecuteAsync
        (
            connection,
            @"CREATE TABLE [dbo].[Widgets] (
                Id INT NOT NULL,
                WidgetName NVARCHAR(100) NOT NULL,
                Price DECIMAL(18,2) NOT NULL
            )"
        ).ConfigureAwait(false);

        using (var transaction = (SqlTransaction)await connection.BeginTransactionAsync().ConfigureAwait(false))
        {
            var sut = new SqlBulkCopyLoader<WidgetRecord>
            (
                connection,
                SqlBulkCopyOptions.Default,
                transaction
            );

            var items = new[]
            {
                new WidgetRecord { Id = 1, Name = "committed", Price = 1m }
            };

            await sut.LoadAsync(ToAsyncEnumerableAsync(items)).ConfigureAwait(false);

            await transaction.CommitAsync().ConfigureAwait(false);
        }

        Assert.Equal(1, await TestSchema.CountRowsAsync(connection, "[dbo].[Widgets]").ConfigureAwait(false));
    }
}
