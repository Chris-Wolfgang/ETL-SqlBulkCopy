using System.Collections.Generic;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;

namespace Wolfgang.Etl.SqlBulkCopy.ShadowWorkloads;

/// <summary>
/// Realistic bulk-load shadow workloads for <see cref="SqlBulkCopyLoader{TRecord}"/>.
/// Spins up a real SQL Server container once, then measures latency and
/// allocations for the loader's public load paths at production-shaped record
/// counts (1k and 100k rows).
/// </summary>
/// <remarks>
/// Each workload exercises a distinct public configuration surface:
/// <list type="bullet">
///   <item>plain streaming load (the baseline),</item>
///   <item>load with <see cref="SqlBulkCopyLoader{TRecord}.EnableDataValidation"/>,</item>
///   <item>load with a <see cref="PreAction.TruncateTable"/> pre-action.</item>
/// </list>
/// </remarks>
[MemoryDiagnoser]
public class BulkLoadShadowWorkloads
{
    private MsSqlContainer _container = null!;
    private SqlConnection _connection = null!;
    private WidgetRecord[] _rows = null!;



    // BDN populates [Params] properties via reflection, not from source.
    // The disable/restore pair (not `disable once`) is needed because the
    // [Params] attribute sits between the comment and the property, so
    // `once` would apply to the attribute line, not the property.
    /// <summary>Gets or sets the number of rows loaded per invocation.</summary>
    // ReSharper disable UnusedAutoPropertyAccessor.Global
    [Params(1_000, 100_000)]
    public int RecordCount { get; set; }
    // ReSharper restore UnusedAutoPropertyAccessor.Global



    /// <summary>
    /// Starts the SQL Server container, creates the destination table, and
    /// materializes the source rows once for the whole run.
    /// </summary>
    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        // Pin the same image tag the integration-test fixture uses so shadow
        // measurements are comparable to the tested configuration.
        // Testcontainers 4.13+ takes the image via the constructor (the
        // parameterless MsSqlBuilder() + .WithImage() form is deprecated).
        _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04")
            .Build();
        await _container.StartAsync();

        _connection = new SqlConnection(_container.GetConnectionString());
        await _connection.OpenAsync();
        await ExecuteAsync
        (
            "IF OBJECT_ID('dbo.Widgets', 'U') IS NOT NULL DROP TABLE dbo.Widgets;" +
            "CREATE TABLE dbo.Widgets (" +
            "  Id INT NOT NULL," +
            "  WidgetName NVARCHAR(100) NOT NULL," +
            "  Price DECIMAL(18,2) NOT NULL);"
        );

        _rows = new WidgetRecord[RecordCount];
        for (var i = 0; i < RecordCount; i++)
        {
            _rows[i] = new WidgetRecord { Id = i, Name = "Widget" + i, Price = i * 1.5m };
        }
    }



    /// <summary>
    /// Before each measured iteration, reset to a known non-empty table: truncate,
    /// then seed a baseline of rows. Seeding (rather than starting empty) means
    /// <see cref="LoadWithTruncatePreAction"/>'s pre-action truncate clears real
    /// rows instead of running against an already-empty table, and the flat/
    /// validation loads run against a table that already holds data. Runs outside
    /// the measured window. Seed ids are negative so they never collide with the
    /// loaded rows (0..RecordCount-1).
    /// </summary>
    [IterationSetup]
    public void SeedBeforeIteration()
    {
        using var command = _connection.CreateCommand();
        command.CommandText =
            "TRUNCATE TABLE dbo.Widgets;" +
            "INSERT INTO dbo.Widgets (Id, WidgetName, Price) " +
            "SELECT TOP (1000) -ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 'seed', 0 " +
            "FROM sys.all_objects;";
        command.ExecuteNonQuery();
    }



    /// <summary>Disposes the connection and stops the SQL Server container.</summary>
    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        // The `= null!` on the field declarations promises non-null, so the
        // BDN [GlobalSetup] contract runs before this. If setup throws, BDN
        // skips the benchmark AND its cleanup, so we can't observe the
        // half-initialized case here.
        await _connection.DisposeAsync();
        await _container.DisposeAsync();
    }



    /// <summary>Streams <see cref="RecordCount"/> rows through a plain loader.</summary>
    [Benchmark(Baseline = true)]
    public async Task LoadFlat()
    {
        var loader = new SqlBulkCopyLoader<WidgetRecord>(_connection);
        await loader.LoadAsync(ToAsyncEnumerable(_rows));
    }



    /// <summary>Streams the rows with per-record data-annotation validation enabled.</summary>
    [Benchmark]
    public async Task LoadWithValidation()
    {
        var loader = new SqlBulkCopyLoader<WidgetRecord>(_connection)
        {
            EnableDataValidation = true
        };
        await loader.LoadAsync(ToAsyncEnumerable(_rows));
    }



    /// <summary>Truncates the table as a pre-action, then streams the rows.</summary>
    [Benchmark]
    public async Task LoadWithTruncatePreAction()
    {
        var loader = new SqlBulkCopyLoader<WidgetRecord>(_connection)
        {
            PreAction = PreAction.TruncateTable
        };
        await loader.LoadAsync(ToAsyncEnumerable(_rows));
    }



    private async Task ExecuteAsync(string sql)
    {
        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }



    // Synchronous source exposed as IAsyncEnumerable for LoadAsync. No await on
    // the enumerated path keeps the measured allocation/latency free of extra
    // await-state-machine work; CS1998 is expected and suppressed.
#pragma warning disable CS1998
    private static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }
    }
#pragma warning restore CS1998
}
