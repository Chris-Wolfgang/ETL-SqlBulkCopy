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
    // Container is built and started in InitializeAsync so that ANY failure
    // (Docker daemon not running, image not reachable, port collision, etc.)
    // surfaces as IsAvailable=false rather than crashing the fixture's field
    // initializer — which would tear down every test in the collection
    // before they have a chance to call Skip.IfNot().
    private MsSqlContainer? _container;



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
    public string ConnectionString =>
        _container?.GetConnectionString()
        ?? throw new InvalidOperationException
        (
            "SQL Server container is not available. " +
            "Check IsAvailable before using ConnectionString."
        );



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
            _container = new MsSqlBuilder()
                .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
                .Build();
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
    public Task DisposeAsync() =>
        _container?.DisposeAsync().AsTask() ?? Task.CompletedTask;
}
