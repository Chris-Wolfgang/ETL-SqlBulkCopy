using System;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Production implementation of <see cref="ISqlBulkCopyWrapper"/> that delegates
/// to <see cref="Microsoft.Data.SqlClient.SqlBulkCopy"/>.
/// </summary>
/// <remarks>
/// Excluded from code coverage because every member is a one-line pass-through
/// to <see cref="Microsoft.Data.SqlClient.SqlBulkCopy"/>. Unit-testing these
/// would amount to either mocking <see cref="Microsoft.Data.SqlClient.SqlBulkCopy"/>
/// itself (proving the test setup, not our code) or requiring a live SQL Server
/// connection — which is what the integration tests in
/// <c>Wolfgang.Etl.SqlBulkCopy.Tests.Integration</c> already do against a
/// Testcontainers-hosted SQL Server. Behavior coverage lives there.
/// </remarks>
[ExcludeFromCodeCoverage]
internal sealed class SqlBulkCopyWrapper : ISqlBulkCopyWrapper
{
    private readonly Microsoft.Data.SqlClient.SqlBulkCopy _bulkCopy;



    /// <summary>
    /// Initializes a new instance of the <see cref="SqlBulkCopyWrapper"/> class.
    /// </summary>
    /// <param name="connection">The SQL Server connection.</param>
    /// <param name="options">The bulk copy options.</param>
    /// <param name="transaction">An optional external transaction.</param>
    internal SqlBulkCopyWrapper
    (
        SqlConnection connection,
        SqlBulkCopyOptions options,
        SqlTransaction? transaction
    )
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        _bulkCopy = new Microsoft.Data.SqlClient.SqlBulkCopy
        (
            connection,
            options,
            transaction
        );
    }



    /// <inheritdoc />
    public string DestinationTableName
    {
        get => _bulkCopy.DestinationTableName;
        set => _bulkCopy.DestinationTableName = value;
    }



    /// <inheritdoc />
    public int BatchSize
    {
        get => _bulkCopy.BatchSize;
        set => _bulkCopy.BatchSize = value;
    }



    /// <inheritdoc />
    public int BulkCopyTimeout
    {
        get => _bulkCopy.BulkCopyTimeout;
        set => _bulkCopy.BulkCopyTimeout = value;
    }



    /// <inheritdoc />
    public void AddColumnMapping(string sourceColumn, string destinationColumn)
    {
        _bulkCopy.ColumnMappings.Add(sourceColumn, destinationColumn);
    }



    /// <inheritdoc />
    public Task WriteToServerAsync(DbDataReader reader, CancellationToken cancellationToken)
    {
        return _bulkCopy.WriteToServerAsync(reader, cancellationToken);
    }



    /// <inheritdoc />
    public void Dispose()
    {
        ((IDisposable)_bulkCopy).Dispose();
    }
}
