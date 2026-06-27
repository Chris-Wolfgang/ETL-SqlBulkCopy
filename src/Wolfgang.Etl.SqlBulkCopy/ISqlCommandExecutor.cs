using System.Threading;
using System.Threading.Tasks;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Abstraction over the SQL execution path used by the loader for pre/post
/// actions (<c>DELETE FROM</c>, <c>TRUNCATE TABLE</c>). Mirrors the role
/// <see cref="ISqlBulkCopyWrapper"/> plays for the bulk-copy data path: in
/// production it wraps a real <see cref="Microsoft.Data.SqlClient.SqlCommand"/>;
/// in unit tests a fake records the issued command text so the orchestration
/// logic in <see cref="SqlBulkCopyLoader{TRecord}.ExecutePreActionAsync"/> /
/// <see cref="SqlBulkCopyLoader{TRecord}.ExecutePostActionAsync"/> can be
/// verified without a SQL Server.
/// </summary>
internal interface ISqlCommandExecutor
{
    /// <summary>
    /// Executes a non-query SQL command (e.g. <c>DELETE</c>, <c>TRUNCATE</c>).
    /// </summary>
    /// <param name="commandText">The SQL command text to execute.</param>
    /// <param name="commandTimeout">
    /// Per-command timeout in seconds (0 = no timeout). Mirrors the
    /// <see cref="System.Data.IDbCommand.CommandTimeout"/> contract.
    /// </param>
    /// <param name="cancellationToken">Cooperative cancellation token.</param>
    Task ExecuteNonQueryAsync(string commandText, int commandTimeout, CancellationToken cancellationToken);
}
