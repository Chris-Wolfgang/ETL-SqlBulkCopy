using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Abstraction over <c>Microsoft.Data.SqlClient.SqlBulkCopy</c> to enable
/// unit testing without a real SQL Server connection.
/// </summary>
internal interface ISqlBulkCopyWrapper : IDisposable
{
    /// <summary>
    /// Gets or sets the name of the destination table on the server.
    /// </summary>
    string DestinationTableName { get; set; }



    /// <summary>
    /// Gets or sets the number of rows in each batch.
    /// </summary>
    int BatchSize { get; set; }



    /// <summary>
    /// Gets or sets the timeout in seconds for the bulk copy operation.
    /// </summary>
    int BulkCopyTimeout { get; set; }



    /// <summary>
    /// Adds a column mapping from source to destination.
    /// </summary>
    /// <param name="sourceColumn">The name of the source column.</param>
    /// <param name="destinationColumn">The name of the destination column.</param>
    void AddColumnMapping(string sourceColumn, string destinationColumn);



    /// <summary>
    /// Copies all rows from the supplied <see cref="DbDataReader"/> to the destination table.
    /// </summary>
    /// <param name="reader">The data reader providing the rows.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task WriteToServerAsync(DbDataReader reader, CancellationToken cancellationToken);
}
