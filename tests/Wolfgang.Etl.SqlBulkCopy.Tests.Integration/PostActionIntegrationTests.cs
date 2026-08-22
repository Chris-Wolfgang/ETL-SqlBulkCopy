using System.Threading.Tasks;
using Wolfgang.Etl.SqlBulkCopy.Tests.Integration.Fixtures;
using Wolfgang.Etl.SqlBulkCopy.Tests.Integration.TestModels;
using Xunit;
using static Wolfgang.Etl.SqlBulkCopy.Tests.Integration.Fixtures.AsyncEnumerableHelpers;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Integration;

[Collection("SqlServer")]
public class PostActionIntegrationTests
{
    private readonly SqlServerFixture _fixture;



    public PostActionIntegrationTests(SqlServerFixture fixture)
    {
        _fixture = fixture;
    }



    [SkippableFact]
    public async Task PostAction_CustomAction_invokes_delegate_after_load()
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

        var rowCountAtPostAction = -1;

        var sut = new SqlBulkCopyLoader<WidgetRecord>(connection)
        {
            PostAction = PostAction.CustomAction,
            PostLoadCustomAction = async parameters =>
            {
                using var command = parameters.Connection.CreateCommand();
                command.CommandText = "SELECT COUNT(*) FROM [dbo].[Widgets]";
#pragma warning disable S8969 // ExecuteScalarAsync's [NotNullWhen] not honored across all TFMs; SELECT COUNT(*) is non-null by contract
                rowCountAtPostAction = (int)(await command.ExecuteScalarAsync(parameters.CancellationToken).ConfigureAwait(false))!;
#pragma warning restore S8969
            }
        };

        var items = new[]
        {
            new WidgetRecord { Id = 1, Name = "a", Price = 1m },
            new WidgetRecord { Id = 2, Name = "b", Price = 2m },
            new WidgetRecord { Id = 3, Name = "c", Price = 3m }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        // Post-action observed all 3 rows already loaded
        Assert.Equal(3, rowCountAtPostAction);
    }
}
