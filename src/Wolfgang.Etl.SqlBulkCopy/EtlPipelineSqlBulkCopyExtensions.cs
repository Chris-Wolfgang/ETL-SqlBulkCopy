using System;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Wolfgang.Etl.Abstractions;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Class-named SQL Server sink terminator for the fluent <see cref="EtlPipeline"/> chain. The
/// terminator constructs a <see cref="SqlBulkCopyLoader{TRecord}"/> and terminates the pipeline,
/// so any source package can flow straight into a bulk insert.
/// </summary>
/// <remarks>
/// The caller owns <see cref="SqlConnection"/> and any <see cref="SqlTransaction"/>: the sink never
/// opens, closes or disposes them, exactly as when the loader is constructed directly.
/// </remarks>
/// <example>
/// <code>
/// await EtlPipeline
///     .Create()
///     .From(items)
///     .SqlBulkCopyLoader
///     (
///         connection,
///         new SqlBulkCopyLoaderOptions&lt;Person&gt; { DestinationTableName = "People", BatchSize = 5000 }
///     )
///     .RunAsync(progress: null, cancellationToken);
/// </code>
/// </example>
public static class EtlPipelineSqlBulkCopyExtensions
{
    /// <summary>
    /// Terminates the pipeline, bulk-copying every record into SQL Server through a
    /// <see cref="SqlBulkCopyLoader{TRecord}"/>.
    /// </summary>
    /// <typeparam name="TRecord">The record type to load.</typeparam>
    /// <param name="pipeline">The pipeline to terminate.</param>
    /// <param name="connection">The open <see cref="SqlConnection"/> to bulk-copy into. The caller owns its lifetime.</param>
    /// <param name="options">The loader's configuration. When <see langword="null"/>, every documented default applies.</param>
    /// <param name="transaction">An optional <see cref="SqlTransaction"/> the bulk copy and the pre/post actions run in.</param>
    /// <param name="logger">An optional logger for the loader.</param>
    /// <returns>A runnable <see cref="IEtlPipelineSink"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="pipeline"/> or <paramref name="connection"/> is <see langword="null"/>.</exception>
    public static IEtlPipelineSink SqlBulkCopyLoader<TRecord>
    (
        this IEtlPipeline<TRecord> pipeline,
        SqlConnection connection,
        SqlBulkCopyLoaderOptions<TRecord>? options = null,
        SqlTransaction? transaction = null,
        ILogger<SqlBulkCopyLoader<TRecord>>? logger = null
    )
        where TRecord : notnull
    {
        if (pipeline is null)
        {
            throw new ArgumentNullException(nameof(pipeline));
        }

        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        return pipeline.To(new SqlBulkCopyLoader<TRecord>(connection, options, transaction, logger));
    }
}
