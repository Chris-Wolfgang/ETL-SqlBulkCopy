using System;
using System.IO;
using System.Net.Sockets;
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
    /// <exception cref="InvalidOperationException">
    /// Thrown when the SQL Server container is not available (check
    /// <see cref="IsAvailable"/> first).
    /// </exception>
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
            // Pin to a specific SQL Server 2022 CU tag for deterministic test
            // behaviour. Update this when consciously moving to a newer CU
            // (avoid floating ":2022-latest" so upstream image updates don't
            // surprise CI).
            _container = new MsSqlBuilder()
                .WithImage("mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04")
                .Build();
            await _container.StartAsync().ConfigureAwait(false);
            IsAvailable = true;
        }
        catch (Exception ex) when (IsDockerUnavailable(ex))
        {
            // Unwrap one level of AggregateException so the skip reason
            // surfaces the actionable inner message ("Docker is not running...")
            // instead of the generic "One or more errors occurred." wrapper.
            var reported = ex is AggregateException aggregate && aggregate.InnerException is not null
                ? aggregate.InnerException
                : ex;
            UnavailableReason = $"{reported.GetType().Name}: {reported.Message}";
            IsAvailable = false;
        }

        // Note: all other exceptions (e.g. bad image tag, invalid
        // Testcontainers configuration, NRE in fixture code) propagate so
        // CI fails loudly instead of silently skipping every integration
        // test in the collection.
    }



    /// <summary>
    /// Returns <c>true</c> when the exception indicates the Docker daemon is
    /// unreachable or the host can't run the requested container — the
    /// scenarios in which "skip the integration tests" is the correct
    /// outcome. Anything else (a typo in the image tag, a misconfigured
    /// MsSqlBuilder, etc.) is treated as a real CI failure.
    /// </summary>
    private static bool IsDockerUnavailable(Exception ex)
    {
        // Unwrap one level of aggregation — Testcontainers occasionally
        // wraps the underlying socket/IO failure in an AggregateException
        // depending on TFM.
        var inner = ex is AggregateException aggregate && aggregate.InnerException is not null
            ? aggregate.InnerException
            : ex;

        // Daemon down / no Docker socket / Windows-containers mode on a
        // Linux-image pull all surface as one of these.
        if (inner is IOException
            || inner is SocketException
            || inner is PlatformNotSupportedException
            || (inner.GetType().FullName ?? string.Empty).StartsWith("Docker.DotNet.", StringComparison.Ordinal))
        {
            return true;
        }

        // Testcontainers' MsSqlBuilder.Build() runs synchronous Validate()
        // before any await; when Docker isn't installed (e.g. macOS GHA
        // runners) Validate throws ArgumentException with
        // ParamName="DockerEndpointAuthConfig". Catch that specific case so
        // Stage 3 macOS skips cleanly instead of failing every integration
        // test in the collection. Any other ArgumentException — bad image
        // tag, misconfigured builder, etc. — still propagates as a real
        // failure because the ParamName differs.
        return inner is ArgumentException argumentException
            && string.Equals(argumentException.ParamName, "DockerEndpointAuthConfig", StringComparison.Ordinal);
    }



    /// <inheritdoc />
    public Task DisposeAsync() =>
        _container?.DisposeAsync().AsTask() ?? Task.CompletedTask;
}
