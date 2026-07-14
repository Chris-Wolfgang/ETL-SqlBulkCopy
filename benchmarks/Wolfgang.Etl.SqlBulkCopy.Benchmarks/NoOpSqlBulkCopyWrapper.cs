using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Wolfgang.Etl.SqlBulkCopy.Benchmarks;

/// <summary>
/// A no-op <see cref="ISqlBulkCopyWrapper"/> for benchmarking the loader's
/// end-to-end path without a SQL Server. <see cref="WriteToServerAsync"/> drains
/// the <see cref="DbDataReader"/> — which drives the real per-row mapping
/// (TypeMapReader + the compiled getters) — but writes nowhere, isolating the
/// loader's mapping/enumeration cost from network + database time.
/// </summary>
internal sealed class NoOpSqlBulkCopyWrapper : ISqlBulkCopyWrapper
{
    public string DestinationTableName { get; set; } = string.Empty;

    public int BatchSize { get; set; }

    public int BulkCopyTimeout { get; set; }

    public void AddColumnMapping(string sourceColumn, string destinationColumn)
    {
        // No-op — mapping is recorded by the loader; nothing to persist here.
    }

    public async Task WriteToServerAsync(DbDataReader reader, CancellationToken cancellationToken)
    {
        // Drain the reader so the loader's per-row mapping actually executes.
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
        }
    }

    public void Dispose()
    {
        // Nothing to dispose.
    }
}

internal sealed class NoOpSqlBulkCopyWrapperFactory : ISqlBulkCopyWrapperFactory
{
    public ISqlBulkCopyWrapper Create() => new NoOpSqlBulkCopyWrapper();
}
