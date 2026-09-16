using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Wolfgang.Etl.Abstractions;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Options for the <see cref="SqlBulkCopyLoader{TRecord}"/> constructor.
/// </summary>
/// <remarks>
/// Supplied as the second constructor parameter, ahead of the optional transaction and logger. When the whole
/// options object is <see langword="null"/>, or an individual property is left unset, the documented defaults
/// below apply — defaults live on the property initializers here rather than in constructor bodies, so no
/// constructor can accidentally diverge from them.
/// <para>
/// Derives from <see cref="LoaderOptions"/>, so the settings every loader shares —
/// <see cref="LoaderOptions.ReportingInterval"/>, <see cref="LoaderOptions.SkipItemCount"/>,
/// <see cref="LoaderOptions.MaximumItemCount"/> and <see cref="LoaderOptions.ErrorPolicy"/> — are configured
/// here as well (ADR-0009 in Wolfgang.Etl.Abstractions).
/// </para>
/// <para>
/// The record is generic because the validation callbacks are typed on the record being loaded. Every member
/// mirrors the property of the same name on the loader; the constructor applies them in declaration order, so
/// the loader's own range checks (for example <see cref="BatchSize"/> below <c>1</c>) fire at construction.
/// </para>
/// </remarks>
/// <typeparam name="TRecord">The record type the loader writes.</typeparam>
public sealed record SqlBulkCopyLoaderOptions<TRecord> : LoaderOptions
    where TRecord : notnull
{
    /// <summary>
    /// Gets the <see cref="SqlBulkCopyOptions"/> flags applied to every bulk copy the loader performs.
    /// Defaults to <see cref="SqlBulkCopyOptions.Default"/>.
    /// </summary>
    /// <remarks>
    /// Bulk-copy configuration such as <see cref="SqlBulkCopyOptions.TableLock"/> or
    /// <see cref="SqlBulkCopyOptions.KeepIdentity"/> belongs here; the transaction the copy runs in is not
    /// configuration and stays a constructor parameter.
    /// </remarks>
    public SqlBulkCopyOptions BulkCopyOptions { get; init; } = SqlBulkCopyOptions.Default;



    /// <summary>
    /// Gets the number of rows in each batch sent to the server.
    /// </summary>
    /// <value>The default is 10,000.</value>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown when the value is less than 1.
    /// </exception>
    public int BatchSize { get; init; } = 10_000;



    /// <summary>
    /// Gets the timeout in seconds for each bulk copy operation.
    /// A value of 0 means no timeout.
    /// </summary>
    /// <value>The default is 30 seconds.</value>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown when the value is negative.
    /// </exception>
    public int BulkCopyTimeout { get; init; } = 30;



    /// <summary>
    /// Gets an optional destination table name override.
    /// When <c>null</c>, the table name is derived from the <c>[Table]</c> attribute
    /// or the type name.
    /// </summary>
    public string? DestinationTableName { get; init; }



    /// <summary>
    /// Gets an optional destination schema name override.
    /// When <c>null</c>, the schema is derived from the <c>[Table]</c> attribute.
    /// </summary>
    public string? DestinationSchemaName { get; init; }



    /// <summary>
    /// Gets a value indicating whether to validate each item using
    /// <see cref="System.ComponentModel.DataAnnotations"/> attributes before loading.
    /// </summary>
    /// <value>The default is <c>false</c>.</value>
    /// <remarks>
    /// Enabling validation adds per-item overhead. Validation is applied recursively
    /// to root <typeparamref name="TRecord"/> instances and to every level of
    /// nested-collection children. How a failure is handled is controlled by
    /// <see cref="ValidationFailureBehavior"/> — the default is to throw a
    /// <see cref="SqlBulkCopyValidationException"/>, which fails loudly and
    /// stops the load. Set <see cref="ValidationFailureBehavior"/> to
    /// <see cref="Wolfgang.Etl.SqlBulkCopy.ValidationFailureBehavior.Skip"/>
    /// to tolerate dirty data and drop only the failing items.
    /// </remarks>
    public bool EnableDataValidation { get; init; }



    /// <summary>
    /// Gets a value indicating whether the load runs as a dry run —
    /// validating the pipeline against real data without writing to SQL Server.
    /// </summary>
    /// <value>The default is <c>false</c>.</value>
    /// <remarks>
    /// When <c>true</c>, the loader still enumerates the source, applies
    /// <c>SkipItemCount</c> / <c>MaximumItemCount</c>, runs data
    /// validation, increments the progress counters, and logs as usual — but
    /// performs <b>no</b> SQL side effects: the <see cref="PreAction"/> /
    /// <see cref="PostAction"/> (e.g. truncate / delete) and the bulk insert are
    /// all skipped. This lets a caller confirm a pipeline runs end-to-end and
    /// surfaces mapping / validation errors without touching the destination.
    /// </remarks>
    public bool IsDryRun { get; init; }



    /// <summary>
    /// Gets how the loader reacts to a validation failure when
    /// <see cref="EnableDataValidation"/> is <c>true</c>.
    /// </summary>
    /// <value>
    /// The default is
    /// <see cref="Wolfgang.Etl.SqlBulkCopy.ValidationFailureBehavior.Throw"/>.
    /// </value>
    /// <remarks>
    /// See <see cref="Wolfgang.Etl.SqlBulkCopy.ValidationFailureBehavior"/>
    /// for the semantics of each option. The same setting applies to both
    /// root <typeparamref name="TRecord"/> instances and nested-collection
    /// children.
    /// </remarks>
    public ValidationFailureBehavior ValidationFailureBehavior { get; init; } = ValidationFailureBehavior.Throw;



    /// <summary>
    /// Gets an optional callback invoked when a root
    /// <typeparamref name="TRecord"/> fails validation.
    /// </summary>
    /// <remarks>
    /// Only invoked when <see cref="EnableDataValidation"/> is <c>true</c>.
    /// The callback runs <em>before</em> the configured
    /// <see cref="ValidationFailureBehavior"/> takes effect, so consumers
    /// can log / inspect the failing item from a single hook regardless of
    /// whether the loader then throws or skips. The callback receives the
    /// failing root item and the collection of validation errors. For
    /// nested-collection children, see <see cref="OnNestedValidationFailed"/>.
    /// </remarks>
    public Action<TRecord, ICollection<ValidationResult>>? OnValidationFailed { get; init; }



    /// <summary>
    /// Gets an optional callback invoked when a nested-collection
    /// child instance fails validation.
    /// </summary>
    /// <remarks>
    /// Only invoked when <see cref="EnableDataValidation"/> is <c>true</c>.
    /// The callback runs <em>before</em> the configured
    /// <see cref="ValidationFailureBehavior"/> takes effect. The child is
    /// passed as <see cref="object"/> because the child type is resolved at
    /// load time, not at <typeparamref name="TRecord"/> definition. For
    /// root-item validation, see <see cref="OnValidationFailed"/>.
    /// </remarks>
    public Action<object, ICollection<ValidationResult>>? OnNestedValidationFailed { get; init; }



    /// <summary>
    /// Gets the action to execute before loading begins.
    /// </summary>
    /// <value>The default is <see cref="Wolfgang.Etl.SqlBulkCopy.PreAction.None"/>.</value>
    public PreAction PreAction { get; init; }



    /// <summary>
    /// Gets the custom delegate to invoke when
    /// <see cref="PreAction"/> is <see cref="Wolfgang.Etl.SqlBulkCopy.PreAction.CustomAction"/>.
    /// </summary>
    public Func<PreLoadActionParameters, Task>? PreLoadCustomAction { get; init; }



    /// <summary>
    /// Gets the action to execute after loading completes.
    /// </summary>
    /// <value>The default is <see cref="Wolfgang.Etl.SqlBulkCopy.PostAction.None"/>.</value>
    public PostAction PostAction { get; init; }



    /// <summary>
    /// Gets the custom delegate to invoke when
    /// <see cref="PostAction"/> is <see cref="Wolfgang.Etl.SqlBulkCopy.PostAction.CustomAction"/>.
    /// </summary>
    public Func<PostLoadActionParameters, Task>? PostLoadCustomAction { get; init; }
}
