namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Controls how <see cref="SqlBulkCopyLoader{TRecord}"/> reacts to an item
/// (root or nested) that fails DataAnnotation validation when
/// <see cref="SqlBulkCopyLoader{TRecord}.EnableDataValidation"/> is <c>true</c>.
/// </summary>
public enum ValidationFailureBehavior
{
    /// <summary>
    /// Throw a <see cref="SqlBulkCopyValidationException"/> carrying the
    /// failing item and the list of <see cref="System.ComponentModel.DataAnnotations.ValidationResult"/>s.
    /// The bulk-copy operation aborts and any partial work follows the
    /// configured transaction semantics (rolled back if an external
    /// transaction is supplied; otherwise the rows already sent to the
    /// server remain). This is the default — failing fast prevents silently
    /// shipping incomplete data sets.
    /// </summary>
    Throw = 0,

    /// <summary>
    /// Skip the failing item and continue. The configured
    /// <see cref="SqlBulkCopyLoader{TRecord}.OnValidationFailed"/> /
    /// <see cref="SqlBulkCopyLoader{TRecord}.OnNestedValidationFailed"/>
    /// callback (if set) is invoked, a warning is logged, and (for root
    /// items only) <see cref="Wolfgang.Etl.Abstractions.LoaderBase{TDestination, TProgress}.CurrentSkippedItemCount"/>
    /// is incremented. Use this when the caller wants to tolerate dirty
    /// data and prefers loading what is valid.
    /// </summary>
    Skip = 1,
}
