using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit.Fakes;

[ExcludeFromCodeCoverage]
internal sealed class FakeSqlBulkCopyWrapper : ISqlBulkCopyWrapper
{
    private readonly List<(string Source, string Destination)> _columnMappings = new();
    private readonly List<int> _batchRowCounts = new();
    private readonly Exception? _throwOnWrite;



    internal FakeSqlBulkCopyWrapper(Exception? throwOnWrite = null)
    {
        _throwOnWrite = throwOnWrite;
    }



    public string DestinationTableName { get; set; } = string.Empty;

    public int BatchSize { get; set; }

    public int BulkCopyTimeout { get; set; }



    public IReadOnlyList<(string Source, string Destination)> ColumnMappings => _columnMappings;

    public IReadOnlyList<int> BatchRowCounts => _batchRowCounts;

    public int TotalWriteCalls => _batchRowCounts.Count;



    public void AddColumnMapping(string sourceColumn, string destinationColumn)
    {
        _columnMappings.Add((sourceColumn, destinationColumn));
    }



    public Task WriteToServerAsync(DbDataReader reader, CancellationToken cancellationToken)
    {
        if (_throwOnWrite is not null)
        {
            throw _throwOnWrite;
        }

        var rowCount = 0;
        while (reader.Read())
        {
            rowCount++;
        }

        _batchRowCounts.Add(rowCount);

        return Task.CompletedTask;
    }



    public void Dispose()
    {
        // No-op for fake
    }
}
