using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Parameters passed to the <see cref="SqlBulkCopyLoader{TRecord}.PostLoadCustomAction"/> delegate.
/// </summary>
/// <param name="Connection">The active SQL Server connection.</param>
/// <param name="Transaction">The active transaction, if any.</param>
/// <param name="SchemaName">The destination schema name, if any.</param>
/// <param name="TableName">The destination table name.</param>
/// <param name="CommandTimeout">The command timeout in seconds.</param>
/// <param name="ColumnMappings">The column mappings used for bulk copy.</param>
/// <param name="Logger">The logger instance.</param>
/// <param name="CancellationToken">A token to cancel the operation.</param>
[ExcludeFromCodeCoverage]
public record PostLoadActionParameters
(
    SqlConnection Connection,
    SqlTransaction? Transaction,
    string? SchemaName,
    string TableName,
    int CommandTimeout,
    IReadOnlyList<ColumnMap> ColumnMappings,
    ILogger Logger,
    CancellationToken CancellationToken
);
