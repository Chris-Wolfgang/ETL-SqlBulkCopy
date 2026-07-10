using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Wolfgang.Etl.SqlBulkCopy.Tests.Integration.Fixtures;
using Wolfgang.Etl.SqlBulkCopy.Tests.Integration.TestModels;
using Xunit;
using static Wolfgang.Etl.SqlBulkCopy.Tests.Integration.Fixtures.AsyncEnumerableHelpers;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Integration;

[Collection("SqlServer")]
public class LoaderIntegrationTests
{
    private readonly SqlServerFixture _fixture;



    public LoaderIntegrationTests(SqlServerFixture fixture)
    {
        _fixture = fixture;
    }



    private static async Task CreateWidgetsTableAsync(SqlConnection connection)
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
    }



    [SkippableFact]
    public async Task Constructor_with_connection_only_loads_rows_Async()
    {
        Skip.IfNot(_fixture.IsAvailable, _fixture.UnavailableReason ?? "SQL Server container unavailable.");

        await using var connection = await _fixture.OpenConnectionAsync();
        await CreateWidgetsTableAsync(connection);

        var sut = new SqlBulkCopyLoader<WidgetRecord>(connection);
        var items = new[]
        {
            new WidgetRecord { Id = 1, Name = "Sprocket", Price = 9.99m },
            new WidgetRecord { Id = 2, Name = "Gadget", Price = 19.99m }
        };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        Assert.Equal(2, await TestSchema.CountRowsAsync(connection, "[dbo].[Widgets]"));
    }



    [SkippableFact]
    public async Task Constructor_with_logger_loads_rows_Async()
    {
        Skip.IfNot(_fixture.IsAvailable, _fixture.UnavailableReason ?? "SQL Server container unavailable.");

        await using var connection = await _fixture.OpenConnectionAsync();
        await CreateWidgetsTableAsync(connection);

        var sut = new SqlBulkCopyLoader<WidgetRecord>
        (
            connection,
            NullLogger<SqlBulkCopyLoader<WidgetRecord>>.Instance
        );

        await sut.LoadAsync(ToAsyncEnumerableAsync(new[] { new WidgetRecord { Id = 1, Name = "X", Price = 1m } }));

        Assert.Equal(1, await TestSchema.CountRowsAsync(connection, "[dbo].[Widgets]"));
    }



    [SkippableFact]
    public async Task Constructor_full_with_options_and_transaction_loads_rows_Async()
    {
        Skip.IfNot(_fixture.IsAvailable, _fixture.UnavailableReason ?? "SQL Server container unavailable.");

        await using var connection = await _fixture.OpenConnectionAsync();
        await CreateWidgetsTableAsync(connection);

        var sut = new SqlBulkCopyLoader<WidgetRecord>
        (
            connection,
            SqlBulkCopyOptions.Default,
            transaction: null
        );

        await sut.LoadAsync(ToAsyncEnumerableAsync(new[] { new WidgetRecord { Id = 1, Name = "X", Price = 1m } }));

        Assert.Equal(1, await TestSchema.CountRowsAsync(connection, "[dbo].[Widgets]"));
    }



    [SkippableFact]
    public async Task LoadAsync_maps_Column_attribute_correctly_Async()
    {
        Skip.IfNot(_fixture.IsAvailable, _fixture.UnavailableReason ?? "SQL Server container unavailable.");

        await using var connection = await _fixture.OpenConnectionAsync();
        await CreateWidgetsTableAsync(connection);

        var sut = new SqlBulkCopyLoader<WidgetRecord>(connection);
        var items = new[] { new WidgetRecord { Id = 1, Name = "Mapped", Price = 5m } };

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        // Read back: WidgetName column should contain "Mapped" (from Name property via [Column])
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT WidgetName FROM [dbo].[Widgets] WHERE Id = 1";
        var result = await command.ExecuteScalarAsync();

        Assert.Equal("Mapped", result);
    }



    // Note: the unit-test suite verifies that the loader chunks correctly
    // by observing the fake wrapper's per-call row counts. This integration
    // test only verifies that all rows arrive at the destination when
    // BatchSize is set smaller than the input — observing the actual batch
    // boundaries through real SqlBulkCopy isn't easy without hooking
    // SqlRowsCopied, which adds noise for limited extra coverage here.
    [SkippableFact]
    public async Task LoadAsync_with_BatchSize_smaller_than_source_writes_all_rows_Async()
    {
        Skip.IfNot(_fixture.IsAvailable, _fixture.UnavailableReason ?? "SQL Server container unavailable.");

        await using var connection = await _fixture.OpenConnectionAsync();
        await CreateWidgetsTableAsync(connection);

        var sut = new SqlBulkCopyLoader<WidgetRecord>(connection)
        {
            BatchSize = 3
        };

        var items = Enumerable.Range(1, 10)
            .Select(i => new WidgetRecord { Id = i, Name = $"W{i}", Price = i })
            .ToArray();

        await sut.LoadAsync(ToAsyncEnumerableAsync(items));

        Assert.Equal(10, await TestSchema.CountRowsAsync(connection, "[dbo].[Widgets]"));
    }
}
