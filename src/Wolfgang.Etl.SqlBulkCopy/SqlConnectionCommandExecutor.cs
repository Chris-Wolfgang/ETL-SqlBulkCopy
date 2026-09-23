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
        // The command text is the caller's, by design: this type is the thin
        // ISqlCommandExecutor behind SqlBulkCopyLoader's pre/post-load custom
        // actions, the same contract as Dapper's Execute or EF's ExecuteSqlRaw.
        // There is nothing here to parameterise - the SQL never mixes library
        // data with the caller's string - and the public API documents that the
        // caller owns the statement.
        // Two things the marker below depends on, both verified against semgrep
        // 1.170.0 with `--config p/csharp`: the rule id must be the full
        // `...csharp-sqli.csharp-sqli` (a `nosemgrep:` naming an id only
        // suppresses an exact match), and it must sit on the line directly above
        // the finding. Getting either wrong silently does nothing.
        // nosemgrep: csharp.lang.security.sqli.csharp-sqli.csharp-sqli
        command.CommandText = commandText;
        command.CommandTimeout = commandTimeout;

        if (_transaction is not null)
        {
            command.Transaction = _transaction;
        }

        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
