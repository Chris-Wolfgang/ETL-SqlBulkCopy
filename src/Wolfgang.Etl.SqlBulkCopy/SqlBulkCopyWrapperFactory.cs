using System;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Production implementation of <see cref="ISqlBulkCopyWrapperFactory"/> that creates
/// <see cref="SqlBulkCopyWrapper"/> instances using a real <see cref="SqlConnection"/>.
/// </summary>
[ExcludeFromCodeCoverage]
internal sealed class SqlBulkCopyWrapperFactory : ISqlBulkCopyWrapperFactory
{
    private readonly SqlConnection _connection;
    private readonly SqlBulkCopyOptions _options;
    private readonly SqlTransaction? _transaction;



    /// <summary>
    /// Initializes a new instance of the <see cref="SqlBulkCopyWrapperFactory"/> class.
    /// </summary>
    /// <param name="connection">The SQL Server connection.</param>
    /// <param name="options">The bulk copy options.</param>
    /// <param name="transaction">An optional external transaction.</param>
    internal SqlBulkCopyWrapperFactory
    (
        SqlConnection connection,
        SqlBulkCopyOptions options,
        SqlTransaction? transaction
    )
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _options = options;
        _transaction = transaction;
    }



    /// <inheritdoc />
    public ISqlBulkCopyWrapper Create()
    {
        return new SqlBulkCopyWrapper(_connection, _options, _transaction);
    }
}
