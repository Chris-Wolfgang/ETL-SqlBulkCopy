using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Wolfgang.Etl.SqlBulkCopy.Tests.Integration.Fixtures;
using Wolfgang.Etl.SqlBulkCopy.Tests.Integration.TestModels;
using Xunit;
using static Wolfgang.Etl.SqlBulkCopy.Tests.Integration.Fixtures.AsyncEnumerableHelpers;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Integration;

[Collection("SqlServer")]
public class PreActionIntegrationTests
{
    private readonly SqlServerFixture _fixture;



    public PreActionIntegrationTests(SqlServerFixture fixture)
    {
        _fixture = fixture;
    }



    private static async Task CreatePopulatedWidgetsTableAsync(SqlConnection connection, int existingRowCount)
    {
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

        for (var i = 1; i <= existingRowCount; i++)
        {
            await TestSchema.ExecuteAsync
            (
                connection,
                $"INSERT INTO [dbo].[Widgets] (Id, WidgetName, Price) VALUES ({i}, N'existing-{i}', {i})"
            ).ConfigureAwait(false);
        }
    }



    [SkippableFact]
    public async Task PreAction_DeleteAllRecords_clears_existing_rows_before_load()
    {
        Skip.IfNot(_fixture.IsAvailable, _fixture.UnavailableReason ?? "SQL Server container unavailable.");

        await using var connection = await _fixture.OpenConnectionAsync();
        await CreatePopulatedWidgetsTableAsync(connection, existingRowCount: 5);

        var sut = new SqlBulkCopyLoader<WidgetRecord>(connection)
        {
            PreAction = PreAction.DeleteAllRecords
        };

        var newItems = new[]
        {
            new WidgetRecord { Id = 100, Name = "fresh", Price = 1m }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(newItems));

        Assert.Equal(1, await TestSchema.CountRowsAsync(connection, "[dbo].[Widgets]"));
    }



    [SkippableFact]
    public async Task PreAction_TruncateTable_clears_existing_rows_before_load()
    {
        Skip.IfNot(_fixture.IsAvailable, _fixture.UnavailableReason ?? "SQL Server container unavailable.");

        await using var connection = await _fixture.OpenConnectionAsync();
        await CreatePopulatedWidgetsTableAsync(connection, existingRowCount: 5);

        var sut = new SqlBulkCopyLoader<WidgetRecord>(connection)
        {
            PreAction = PreAction.TruncateTable
        };

        var newItems = new[]
        {
            new WidgetRecord { Id = 100, Name = "fresh", Price = 1m }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(newItems));

        Assert.Equal(1, await TestSchema.CountRowsAsync(connection, "[dbo].[Widgets]"));
    }



    [SkippableFact]
    public async Task PreAction_CustomAction_invokes_delegate_with_connection()
    {
        Skip.IfNot(_fixture.IsAvailable, _fixture.UnavailableReason ?? "SQL Server container unavailable.");

        await using var connection = await _fixture.OpenConnectionAsync();
        await CreatePopulatedWidgetsTableAsync(connection, existingRowCount: 0);

        SqlConnection? capturedConnection = null;
        string? capturedTableName = null;

        var sut = new SqlBulkCopyLoader<WidgetRecord>(connection)
        {
            PreAction = PreAction.CustomAction,
            PreLoadCustomAction = parameters =>
            {
                capturedConnection = parameters.Connection;
                capturedTableName = parameters.TableName;
                return Task.CompletedTask;
            }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(new[] { new WidgetRecord { Id = 1, Name = "x", Price = 1m } }));

        Assert.Same(connection, capturedConnection);
        Assert.Equal("Widgets", capturedTableName);
    }
}
