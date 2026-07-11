using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Wolfgang.Etl.SqlBulkCopy;

namespace Wolfgang.Etl.SqlBulkCopy.Examples.BulkLoadQuickstart;

/// <summary>
/// A domain record mapped to a SQL Server table via attributes. The loader
/// reads these attributes to build the column map — no manual configuration.
/// </summary>
[Table("Customers", Schema = "dbo")]
internal sealed record Customer
{
    public int Id { get; init; }

    // Property name differs from the column name; [Column] bridges them.
    [Column("FullName")]
    public string Name { get; init; } = string.Empty;

    public decimal Balance { get; init; }

    // Not a column — ignored by the loader.
    [NotMapped]
    public string InternalNote { get; init; } = string.Empty;
}

/// <summary>
/// Minimal end-to-end consumer of <see cref="SqlBulkCopyLoader{T}"/>: stream a
/// realistic, variable-size workload into SQL Server with batching, a
/// truncate-before-load pre-action, and live progress reporting.
///
/// This doubles as the workload for the (deferred) shadow-test harness in
/// issue #82 — replaying it against a candidate build and comparing latency /
/// allocations to a baseline release.
/// </summary>
internal static class Program
{
    private static async Task<int> Main()
    {
        // Connection string comes from the environment so the sample carries
        // no secrets and can point at any SQL Server (LocalDB, a container,
        // a real instance).
        var connectionString = Environment.GetEnvironmentVariable("SQLBULKCOPY_SAMPLE_CONNECTION");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            Console.WriteLine("Set SQLBULKCOPY_SAMPLE_CONNECTION to a SQL Server connection string to run this sample.");
            Console.WriteLine("The target must already contain a [dbo].[Customers] table with (Id int, FullName nvarchar, Balance decimal).");
            // Missing configuration is a no-op, not an error — exit cleanly so
            // running the sample without setup prints guidance and succeeds.
            return 0;
        }

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        // Progress is reported per batch as rows are written.
        var progress = new Progress<SqlBulkCopyReport>(report =>
            Console.WriteLine($"  batch {report.BatchCount}: {report.CurrentItemCount} rows written"));

        var loader = new SqlBulkCopyLoader<Customer>(connection)
        {
            BatchSize = 10_000,
            BulkCopyTimeout = 60,
            PreAction = PreAction.TruncateTable,
        };

        Console.WriteLine("Loading customers...");
        await loader.LoadAsync(ReadCustomersAsync(50_000), progress);
        Console.WriteLine("Done.");
        return 0;
    }

    /// <summary>
    /// A realistic streaming source. In a real consumer this reads from a file,
    /// an HTTP API, or another database; here it synthesizes rows on the fly and
    /// occasionally yields to the scheduler so the workload has the bursty,
    /// mixed-async shape production traffic tends to have.
    /// </summary>
    private static async IAsyncEnumerable<Customer> ReadCustomersAsync(int count)
    {
        for (var i = 1; i <= count; i++)
        {
            yield return new Customer
            {
                Id = i,
                Name = $"Customer {i}",
                Balance = i * 1.25m,
            };

            if (i % 10_000 == 0)
            {
                await Task.Yield();
            }
        }
    }
}
