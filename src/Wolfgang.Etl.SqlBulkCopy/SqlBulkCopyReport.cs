using Wolfgang.Etl.Abstractions;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Progress report for SQL Server bulk copy loading operations.
/// </summary>
/// <remarks>
/// Extends <see cref="Report"/> with bulk-copy-specific progress information,
/// including the count of skipped items and the number of batches written.
/// </remarks>
public record SqlBulkCopyReport : Report
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SqlBulkCopyReport"/> class.
    /// </summary>
    /// <param name="currentItemCount">The number of items loaded so far.</param>
    /// <param name="currentSkippedItemCount">The number of items skipped so far.</param>
    /// <param name="batchCount">The number of batches written so far.</param>
    public SqlBulkCopyReport
    (
        int currentItemCount,
        int currentSkippedItemCount,
        int batchCount
    )
        : base(currentItemCount)
    {
        CurrentSkippedItemCount = currentSkippedItemCount;
        BatchCount = batchCount;
    }



    /// <summary>
    /// Gets the number of items that have been skipped during processing.
    /// </summary>
    public int CurrentSkippedItemCount { get; }



    /// <summary>
    /// Gets the number of batches that have been written to the server.
    /// </summary>
    public int BatchCount { get; }
}
