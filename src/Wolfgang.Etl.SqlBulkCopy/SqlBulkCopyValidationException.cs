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
/// The loader always raises this exception through the
/// <see cref="SqlBulkCopyValidationException(object, IReadOnlyList{ValidationResult})"/>
/// constructor, so <see cref="Item"/> and <see cref="ValidationResults"/> are
/// populated for every instance the library itself throws. The conventional
/// <see cref="Exception"/> constructors are also provided so the type is a
/// well-behaved framework citizen (catch-rethrow with a custom message,
/// wrapping an inner exception); instances created that way have a
/// <see langword="null"/> <see cref="Item"/> and an empty
/// <see cref="ValidationResults"/>.
/// </para>
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
    public SqlBulkCopyValidationException()
    {
    }



    /// <summary>
    /// Initializes a new instance of the
    /// <see cref="SqlBulkCopyValidationException"/> class with a specified
    /// error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public SqlBulkCopyValidationException(string message)
        : base(message)
    {
    }



    /// <summary>
    /// Initializes a new instance of the
    /// <see cref="SqlBulkCopyValidationException"/> class with a specified
    /// error message and a reference to the inner exception that caused it.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of this exception.</param>
    public SqlBulkCopyValidationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }



    /// <summary>
    /// Initializes a new instance of the
    /// <see cref="SqlBulkCopyValidationException"/> class for a specific
    /// failing item and its validation errors. This is the constructor the
    /// loader uses.
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
    /// Gets the item that failed validation, or <c>null</c> when the
    /// exception was created through one of the conventional
    /// <see cref="Exception"/> constructors.
    /// </summary>
    public object? Item { get; }



    /// <summary>
    /// Gets the validation errors produced by DataAnnotations. Empty when the
    /// exception was created through one of the conventional
    /// <see cref="Exception"/> constructors — the property initializer below
    /// supplies the empty default, and only the rich constructor overrides it.
    /// </summary>
    public IReadOnlyList<ValidationResult> ValidationResults { get; } = Array.Empty<ValidationResult>();



    private static string BuildMessage
    (
        object? item,
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
        var errorWord = count == 1 ? "error" : "errors";
        return $"DataAnnotation validation failed for '{item.GetType().Name}' with {count} {errorWord}.";
    }
}
