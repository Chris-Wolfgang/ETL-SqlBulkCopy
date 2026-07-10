namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Specifies the action to execute after the bulk copy loading operation completes.
/// </summary>
public enum PostAction
{
    /// <summary>
    /// No post-load action is executed.
    /// </summary>
    None = 0,

    /// <summary>
    /// Invokes the user-supplied <see cref="SqlBulkCopyLoader{TRecord}.PostLoadCustomAction"/>
    /// delegate after loading.
    /// </summary>
    CustomAction = 1
}
