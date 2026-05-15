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



    [SkippableFact]
    public async Task LoadAsync_with_external_transaction_rolled_back_writes_no_rows_Async()
    {
        Skip.IfNot(_fixture.IsAvailable, _fixture.UnavailableReason ?? "SQL Server container unavailable.");

        await using var connection = await _fixture.OpenConnectionAsync();
        await TestSchema.DropIfExistsAsync(connection, "[dbo].[Widgets]");
        await TestSchema.ExecuteAsync
        (
            connection,
            @"CREATE TABLE [dbo].[Widgets] (
                Id INT NOT NULL,
                WidgetName NVARCHAR(100) NOT NULL,
                Price DECIMAL(18,2) NOT NULL
            )"
        );

        using (var transaction = (SqlTransaction)await connection.BeginTransactionAsync())
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

            await sut.LoadAsync(ToAsyncEnumerableAsync(items));

            await transaction.RollbackAsync();
        }

        Assert.Equal(0, await TestSchema.CountRowsAsync(connection, "[dbo].[Widgets]"));
    }



    [SkippableFact]
    public async Task LoadAsync_with_external_transaction_committed_writes_rows_Async()
    {
        Skip.IfNot(_fixture.IsAvailable, _fixture.UnavailableReason ?? "SQL Server container unavailable.");

        await using var connection = await _fixture.OpenConnectionAsync();
        await TestSchema.DropIfExistsAsync(connection, "[dbo].[Widgets]");
        await TestSchema.ExecuteAsync
        (
            connection,
            @"CREATE TABLE [dbo].[Widgets] (
                Id INT NOT NULL,
                WidgetName NVARCHAR(100) NOT NULL,
                Price DECIMAL(18,2) NOT NULL
            )"
        );

        using (var transaction = (SqlTransaction)await connection.BeginTransactionAsync())
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

            await sut.LoadAsync(ToAsyncEnumerableAsync(items));

            await transaction.CommitAsync();
        }

        Assert.Equal(1, await TestSchema.CountRowsAsync(connection, "[dbo].[Widgets]"));
    }
}
