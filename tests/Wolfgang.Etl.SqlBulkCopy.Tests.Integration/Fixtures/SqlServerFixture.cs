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
/// <remarks>
/// On runners that cannot pull the Linux SQL Server image (e.g. Windows GHA
/// runners in Windows-containers mode, macOS without Docker), the container
/// startup throws. The fixture catches that and exposes <see cref="IsAvailable"/>
/// as <c>false</c> so individual tests can skip themselves rather than
/// crashing the whole stage.
/// </remarks>
public sealed class SqlServerFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _container = new MsSqlBuilder()
        .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
        .Build();



    /// <summary>
    /// Gets a value indicating whether the SQL Server container started
    /// successfully and is available for tests to use.
    /// </summary>
    public bool IsAvailable { get; private set; }



    /// <summary>
    /// Gets the reason the container failed to start, if any. <c>null</c>
    /// when <see cref="IsAvailable"/> is <c>true</c>.
    /// </summary>
    public string? UnavailableReason { get; private set; }



    /// <summary>
    /// Gets the connection string to the running SQL Server container.
    /// Only valid after <see cref="InitializeAsync"/> has completed and
    /// <see cref="IsAvailable"/> is <c>true</c>.
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
    public async Task InitializeAsync()
    {
        try
        {
            await _container.StartAsync().ConfigureAwait(false);
            IsAvailable = true;
        }
        catch (Exception ex)
        {
            UnavailableReason = ex.Message;
            IsAvailable = false;
        }
    }



    /// <inheritdoc />
    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}
