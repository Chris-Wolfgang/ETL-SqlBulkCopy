using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Integration.Fixtures;

/// <summary>
/// xUnit collection fixture that spins up a single SQL Server container for the
/// lifetime of all integration tests in the <c>SqlServer</c> collection.
/// </summary>
public sealed class SqlServerFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _container = new MsSqlBuilder()
        .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
        .Build();



    /// <summary>
    /// Gets the connection string to the running SQL Server container.
    /// Only valid after <see cref="InitializeAsync"/> has completed.
    /// </summary>
    public string ConnectionString => _container.GetConnectionString();



    /// <summary>
    /// Opens a new <see cref="SqlConnection"/> against the test container.
    /// Caller is responsible for disposal.
    /// </summary>
    public async Task<SqlConnection> OpenConnectionAsync()
    {
        var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        return connection;
    }



    /// <inheritdoc />
    public Task InitializeAsync() => _container.StartAsync();



    /// <inheritdoc />
    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}
