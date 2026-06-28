using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Production implementation of <see cref="ISqlCommandExecutor"/> that
/// builds and runs a <see cref="SqlCommand"/> against a real
/// <see cref="SqlConnection"/>.
/// </summary>
/// <remarks>
/// Excluded from code coverage for the same reason as <see cref="SqlBulkCopyWrapper"/>:
/// every line is a one-line pass-through to the
/// <see cref="Microsoft.Data.SqlClient"/> SDK. A unit test of these would
/// either mock <see cref="SqlCommand"/> (proving the test setup, not our
/// code) or require a live SQL Server — the integration suite in
/// <c>Wolfgang.Etl.SqlBulkCopy.Tests.Integration</c> already covers it
/// against a Testcontainers-hosted instance.
/// </remarks>
[ExcludeFromCodeCoverage]
internal sealed class SqlConnectionCommandExecutor : ISqlCommandExecutor
{
    private readonly SqlConnection _connection;
    private readonly SqlTransaction? _transaction;



    /// <summary>
    /// Initializes a new instance of the <see cref="SqlConnectionCommandExecutor"/> class.
    /// </summary>
    /// <param name="connection">The SQL Server connection to issue commands on.</param>
    /// <param name="transaction">Optional ambient transaction.</param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="connection"/> is <c>null</c>.
    /// </exception>
    internal SqlConnectionCommandExecutor(SqlConnection connection, SqlTransaction? transaction)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _transaction = transaction;
    }



    /// <inheritdoc />
    public async Task ExecuteNonQueryAsync(string commandText, int commandTimeout, CancellationToken cancellationToken)
    {
        using var command = _connection.CreateCommand();
        command.CommandText = commandText;
        command.CommandTimeout = commandTimeout;

        if (_transaction is not null)
        {
            command.Transaction = _transaction;
        }

        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
