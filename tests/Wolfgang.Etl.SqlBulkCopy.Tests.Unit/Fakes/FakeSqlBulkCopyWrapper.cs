using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;

internal sealed class FakeSqlBulkCopyWrapper : ISqlBulkCopyWrapper
{
    private readonly List<(string Source, string Destination)> _columnMappings = new();
    private readonly List<int> _batchRowCounts = new();



    public string DestinationTableName { get; set; } = string.Empty;

    public int BatchSize { get; set; }

    public int BulkCopyTimeout { get; set; }



    public IReadOnlyList<(string Source, string Destination)> ColumnMappings => _columnMappings;

    public IReadOnlyList<int> BatchRowCounts => _batchRowCounts;



    public void AddColumnMapping(string sourceColumn, string destinationColumn)
    {
        _columnMappings.Add((sourceColumn, destinationColumn));
    }



    public async Task WriteToServerAsync(DbDataReader reader, CancellationToken cancellationToken)
    {
        var rowCount = 0;
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            rowCount++;
        }

        _batchRowCounts.Add(rowCount);
    }



    public void Dispose()
    {
        // No-op for fake
    }
}
