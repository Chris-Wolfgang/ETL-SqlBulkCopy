// Sustained-load GC / allocation workload for gc-profile.yaml.
//
// Drives SqlBulkCopyLoader against a REAL SQL Server (Testcontainers, Docker)
// in a truncate-and-reload loop for GC_WORKLOAD_SECONDS seconds, so
// dotnet-counters / dotnet-trace can measure the loader's allocation and GC
// behaviour under a realistic ETL pattern (not the micro-scale BDN benchmarks).
// Prints a periodic GC snapshot to stdout for offline inspection. Refs #94.

using System.Diagnostics;
using GcProfileWorkload;
using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;
using Wolfgang.Etl.SqlBulkCopy;

var seconds = int.TryParse(Environment.GetEnvironmentVariable("GC_WORKLOAD_SECONDS"), out var parsed) && parsed > 0
    ? parsed
    : 600;
const int rowsPerIteration = 50_000;

Console.WriteLine($"Starting SQL Server container (workload target: {seconds}s, {rowsPerIteration} rows/iteration)...");

await using var container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04").Build();
await container.StartAsync();
var connectionString = container.GetConnectionString();

await using (var setup = new SqlConnection(connectionString))
{
    await setup.OpenAsync();
    await using var command = setup.CreateCommand();
    command.CommandText =
        "IF OBJECT_ID('dbo.Widgets') IS NULL " +
        "CREATE TABLE dbo.Widgets (Id INT, WidgetName NVARCHAR(200), Price DECIMAL(18,2));";
    await command.ExecuteNonQueryAsync();
}

var stopwatch = Stopwatch.StartNew();
long iterations = 0;
long totalRows = 0;

while (stopwatch.Elapsed.TotalSeconds < seconds)
{
    await using var connection = new SqlConnection(connectionString);
    await connection.OpenAsync();

    var loader = new SqlBulkCopyLoader<Widget>(connection)
    {
        BatchSize = 5_000,
        PreAction = PreAction.TruncateTable,
    };

    await loader.LoadAsync(GenerateAsync(rowsPerIteration));
    iterations++;
    totalRows += rowsPerIteration;

    if (iterations % 10 == 0)
    {
        Console.WriteLine(
            $"[{(int)stopwatch.Elapsed.TotalSeconds}s] iterations={iterations} rows={totalRows} " +
            $"gen0={GC.CollectionCount(0)} gen1={GC.CollectionCount(1)} gen2={GC.CollectionCount(2)} " +
            $"heapMB={GC.GetTotalMemory(forceFullCollection: false) / (1024 * 1024)}");
    }
}

Console.WriteLine(
    $"Done: iterations={iterations} rows={totalRows} elapsed={stopwatch.Elapsed} " +
    $"gen0={GC.CollectionCount(0)} gen1={GC.CollectionCount(1)} gen2={GC.CollectionCount(2)}");

static async IAsyncEnumerable<Widget> GenerateAsync(int count)
{
    for (var i = 0; i < count; i++)
    {
        yield return new Widget { Id = i, WidgetName = "Widget-" + i, Price = i * 1.5m };
    }

    await Task.CompletedTask;
}
