using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Thrown by <see cref="SqlBulkCopyLoader{TRecord}"/> when an item (root or
/// nested-collection child) fails DataAnnotation validation and the loader
/// is configured to throw via
/// <see cref="ValidationFailureBehavior.Throw"/> (the default).
/// </summary>
/// <remarks>
/// <para>
/// <see cref="Item"/> is the failing instance — typed as <see cref="object"/>
/// because the failure can come from either the root <c>TRecord</c> or a
/// nested-collection child whose type is only known at load time. Callers
/// can downcast when they know which side raised it.
/// </para>
/// <para>
/// <see cref="ValidationResults"/> holds the per-field failures from
/// <see cref="Validator.TryValidateObject(object, ValidationContext, ICollection{ValidationResult}, bool)"/>.
/// </para>
/// </remarks>
public sealed class SqlBulkCopyValidationException : Exception
{
    /// <summary>
    /// Initializes a new instance of the
    /// <see cref="SqlBulkCopyValidationException"/> class.
    /// </summary>
    /// <param name="item">The item that failed validation.</param>
    /// <param name="validationResults">The validation errors.</param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="item"/> or <paramref name="validationResults"/>
    /// is <c>null</c>.
    /// </exception>
    public SqlBulkCopyValidationException
    (
        object item,
        IReadOnlyList<ValidationResult> validationResults
    )
        : base(BuildMessage(item, validationResults))
    {
        Item = item ?? throw new ArgumentNullException(nameof(item));
        ValidationResults = validationResults ?? throw new ArgumentNullException(nameof(validationResults));
    }



    /// <summary>
    /// Gets the item that failed validation.
    /// </summary>
    public object Item { get; }



    /// <summary>
    /// Gets the validation errors produced by DataAnnotations.
    /// </summary>
    public IReadOnlyList<ValidationResult> ValidationResults { get; }



    private static string BuildMessage
    (
        object item,
        IReadOnlyList<ValidationResult>? validationResults
    )
    {
        if (item is null)
        {
            // Defer to the constructor's ArgumentNullException by returning
            // a placeholder — the constructor will throw before this string
            // is observable.
            return "Item is null.";
        }

        var count = validationResults?.Count ?? 0;
        return $"DataAnnotation validation failed for '{item.GetType().Name}' with {count} errors.";
    }
}
