using System;
using Microsoft.Extensions.Logging;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// High-performance cached log message delegates for <see cref="SqlBulkCopyLoader{TRecord}"/>.
/// </summary>
internal static class SqlBulkCopyLogMessages
{
    private static readonly Action<ILogger, string, Exception?> StartingOperationMessage =
        LoggerMessage.Define<string>
        (
            LogLevel.Information,
            new EventId(1, nameof(StartingOperation)),
            "Starting {OperationName}."
        );



    private static readonly Action<ILogger, int, int, Exception?> SkippedItemMessage =
        LoggerMessage.Define<int, int>
        (
            LogLevel.Debug,
            new EventId(2, nameof(SkippedItem)),
            "Skipped item {SkippedCount} of {SkipTarget}."
        );



    private static readonly Action<ILogger, int, Exception?> ReachedMaximumItemCountMessage =
        LoggerMessage.Define<int>
        (
            LogLevel.Information,
            new EventId(3, nameof(ReachedMaximumItemCount)),
            "Reached maximum item count of {MaximumItemCount}. Stopping."
        );



    private static readonly Action<ILogger, int, int, Exception?> BatchWrittenMessage =
        LoggerMessage.Define<int, int>
        (
            LogLevel.Debug,
            new EventId(100, nameof(BatchWritten)),
            "Batch {BatchNumber} written with {ItemCount} items."
        );



    private static readonly Action<ILogger, int, int, Exception?> BulkCopyCompletedMessage =
        LoggerMessage.Define<int, int>
        (
            LogLevel.Information,
            new EventId(101, nameof(BulkCopyCompleted)),
            "Bulk copy completed. {TotalItems} items loaded, {SkippedItems} items skipped."
        );



    private static readonly Action<ILogger, string, int, Exception?> ValidationFailedMessage =
        LoggerMessage.Define<string, int>
        (
            LogLevel.Warning,
            new EventId(102, nameof(ValidationFailed)),
            "Validation failed for item at position {Position} with {ErrorCount} errors."
        );



    private static readonly Action<ILogger, string, int, Exception?> NestedTableBatchWrittenMessage =
        LoggerMessage.Define<string, int>
        (
            LogLevel.Debug,
            new EventId(103, nameof(NestedTableBatchWritten)),
            "Nested table '{TableName}' batch written with {ItemCount} items."
        );



    private static readonly Action<ILogger, string, Exception?> ExecutingPreActionMessage =
        LoggerMessage.Define<string>
        (
            LogLevel.Information,
            new EventId(104, nameof(ExecutingPreAction)),
            "Executing pre-action: {PreAction}."
        );



    private static readonly Action<ILogger, string, Exception?> ExecutingPostActionMessage =
        LoggerMessage.Define<string>
        (
            LogLevel.Information,
            new EventId(105, nameof(ExecutingPostAction)),
            "Executing post-action: {PostAction}."
        );



    internal static void StartingOperation(ILogger logger, string operationName, Exception? exception) =>
        StartingOperationMessage(logger, operationName, exception);

    internal static void SkippedItem(ILogger logger, int skippedCount, int skipTarget, Exception? exception) =>
        SkippedItemMessage(logger, skippedCount, skipTarget, exception);

    internal static void ReachedMaximumItemCount(ILogger logger, int maximumItemCount, Exception? exception) =>
        ReachedMaximumItemCountMessage(logger, maximumItemCount, exception);

    internal static void BatchWritten(ILogger logger, int batchNumber, int itemCount, Exception? exception) =>
        BatchWrittenMessage(logger, batchNumber, itemCount, exception);

    internal static void BulkCopyCompleted(ILogger logger, int totalItems, int skippedItems, Exception? exception) =>
        BulkCopyCompletedMessage(logger, totalItems, skippedItems, exception);

    internal static void ValidationFailed(ILogger logger, string position, int errorCount, Exception? exception) =>
        ValidationFailedMessage(logger, position, errorCount, exception);

    internal static void NestedTableBatchWritten(ILogger logger, string tableName, int itemCount, Exception? exception) =>
        NestedTableBatchWrittenMessage(logger, tableName, itemCount, exception);

    internal static void ExecutingPreAction(ILogger logger, string preAction, Exception? exception) =>
        ExecutingPreActionMessage(logger, preAction, exception);

    internal static void ExecutingPostAction(ILogger logger, string postAction, Exception? exception) =>
        ExecutingPostActionMessage(logger, postAction, exception);
}
