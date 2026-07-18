using System;
using System.Collections.Generic;
using System.ComponentModel;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// The compile-time-generated description of how a <c>[BulkCopyable]</c> type
/// maps to a table — everything <c>TypeMap.Create</c> would otherwise derive by
/// reflecting over the type. Supplying this lets the runtime build the map
/// without reflection, which is what makes the mapping path Native-AOT clean.
/// See ADR 0006.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class GeneratedTypeDescriptor
{
    /// <summary>
    /// Initializes a new <see cref="GeneratedTypeDescriptor"/>. Intended to be
    /// called only by generated code.
    /// </summary>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="tableName"/> or <paramref name="columns"/> is
    /// <see langword="null"/>.
    /// </exception>
    public GeneratedTypeDescriptor
    (
        string? schemaName,
        string tableName,
        IReadOnlyList<GeneratedColumnDescriptor> columns
    )
    {
        SchemaName = schemaName;
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        Columns = columns ?? throw new ArgumentNullException(nameof(columns));
    }



    /// <summary>Gets the schema declared by <c>[Table(Schema=...)]</c>, or <c>null</c>.</summary>
    public string? SchemaName { get; }



    /// <summary>Gets the base table name (before any per-load override).</summary>
    public string TableName { get; }



    /// <summary>Gets the ordered mapped columns.</summary>
    public IReadOnlyList<GeneratedColumnDescriptor> Columns { get; }
}
