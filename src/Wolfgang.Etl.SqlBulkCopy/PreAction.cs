namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Specifies the action to execute before the bulk copy loading operation begins.
/// </summary>
public enum PreAction
{
    /// <summary>
    /// No pre-load action is executed.
    /// </summary>
    None = 0,

    /// <summary>
    /// Executes <c>DELETE FROM [Schema].[Table]</c> to remove all records
    /// from the destination table before loading.
    /// </summary>
    DeleteAllRecords = 1,

    /// <summary>
    /// Executes <c>TRUNCATE TABLE [Schema].[Table]</c> to remove all records
    /// and reset identity columns before loading.
    /// </summary>
    TruncateTable = 2,

    /// <summary>
    /// Invokes the user-supplied <see cref="SqlBulkCopyLoader{TRecord}.PreLoadCustomAction"/>
    /// delegate before loading.
    /// </summary>
    CustomAction = 3
}
