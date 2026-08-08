using System;
using System.ComponentModel;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Describes one mapped column of a <c>[BulkCopyable]</c> type as pure data
/// produced by the source generator at compile time — the same facts
/// <see cref="ColumnMap"/> would otherwise read via reflection. The getter and
/// enum converter delegates are not carried here; they are supplied separately
/// through <see cref="GeneratedAccessorRegistry"/>.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class GeneratedColumnDescriptor
{
    /// <summary>
    /// Initializes a new <see cref="GeneratedColumnDescriptor"/>. Intended to be
    /// called only by generated code.
    /// </summary>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="propertyName"/>, <paramref name="columnName"/>,
    /// or <paramref name="clrType"/> is <see langword="null"/>.
    /// </exception>
    public GeneratedColumnDescriptor
    (
        string propertyName,
        string columnName,
        Type clrType,
        bool isNullable,
        int ordinal
    )
    {
        PropertyName = propertyName ?? throw new ArgumentNullException(nameof(propertyName));
        ColumnName = columnName ?? throw new ArgumentNullException(nameof(columnName));
        ClrType = clrType ?? throw new ArgumentNullException(nameof(clrType));
        IsNullable = isNullable;
        Ordinal = ordinal;
    }



    /// <summary>Gets the name of the .NET property.</summary>
    public string PropertyName { get; }



    /// <summary>Gets the destination SQL column name.</summary>
    public string ColumnName { get; }



    /// <summary>Gets the CLR type, unwrapped from <see cref="Nullable{T}"/>.</summary>
    public Type ClrType { get; }



    /// <summary>Gets whether the property can hold <c>null</c>.</summary>
    public bool IsNullable { get; }



    /// <summary>Gets the zero-based ordinal position of this column.</summary>
    public int Ordinal { get; }
}
