using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Wolfgang.Etl.SqlBulkCopy.Tests.Integration.Fixtures;
using Wolfgang.Etl.SqlBulkCopy.Tests.Integration.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Integration;

[Collection("SqlServer")]
public class PostActionIntegrationTests
{
    private readonly SqlServerFixture _fixture;



    public PostActionIntegrationTests(SqlServerFixture fixture)
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
    public async Task PostAction_CustomAction_invokes_delegate_after_load_Async()
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

        var rowCountAtPostAction = -1;

        var sut = new SqlBulkCopyLoader<WidgetRecord>(connection)
        {
            PostAction = PostAction.CustomAction,
            PostLoadCustomAction = async parameters =>
            {
                using var command = parameters.Connection.CreateCommand();
                command.CommandText = "SELECT COUNT(*) FROM [dbo].[Widgets]";
                rowCountAtPostAction = (int)(await command.ExecuteScalarAsync(parameters.CancellationToken).ConfigureAwait(false))!;
            }
        };

        var items = new[]
        {
            new WidgetRecord { Id = 1, Name = "a", Price = 1m },
            new WidgetRecord { Id = 2, Name = "b", Price = 2m },
            new WidgetRecord { Id = 3, Name = "c", Price = 3m }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items)).ConfigureAwait(false);

        // Post-action observed all 3 rows already loaded
        Assert.Equal(3, rowCountAtPostAction);
    }
}
