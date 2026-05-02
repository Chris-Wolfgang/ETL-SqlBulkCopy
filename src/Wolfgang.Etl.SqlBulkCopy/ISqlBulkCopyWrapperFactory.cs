namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Factory for creating <see cref="ISqlBulkCopyWrapper"/> instances.
/// </summary>
/// <remarks>
/// A fresh wrapper is created per batch write to avoid reusing stale state
/// across multiple bulk copy operations.
/// </remarks>
internal interface ISqlBulkCopyWrapperFactory
{
    /// <summary>
    /// Creates a new <see cref="ISqlBulkCopyWrapper"/> instance.
    /// </summary>
    /// <returns>A new wrapper ready for configuration and use.</returns>
    ISqlBulkCopyWrapper Create();
}
