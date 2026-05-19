using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Maps a single property on a .NET type to a column in a SQL Server table.
/// </summary>
/// <remarks>
/// The column name is determined by the <see cref="ColumnAttribute"/> if present,
/// otherwise the property name is used. Nullable types are unwrapped to their
/// underlying type for SQL mapping purposes.
/// </remarks>
public sealed class ColumnMap
{
    private readonly Func<object, object?> _getter;



    /// <summary>
    /// Initializes a new instance of the <see cref="ColumnMap"/> class
    /// from a <see cref="PropertyInfo"/>.
    /// </summary>
    /// <param name="propertyInfo">The property to map.</param>
    /// <param name="ordinal">The zero-based ordinal position of this column.</param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="propertyInfo"/> is <c>null</c>.
    /// </exception>
    internal ColumnMap
    (
        PropertyInfo propertyInfo,
        int ordinal
    )
    {
        if (propertyInfo is null)
        {
            throw new ArgumentNullException(nameof(propertyInfo));
        }

        PropertyName = propertyInfo.Name;

        var underlyingType = Nullable.GetUnderlyingType(propertyInfo.PropertyType);
        ClrType = underlyingType ?? propertyInfo.PropertyType;
        IsNullable = underlyingType is not null
                     || !propertyInfo.PropertyType.IsValueType;

        var columnAttribute = propertyInfo
            .GetCustomAttribute<ColumnAttribute>(inherit: false);

        ColumnName = columnAttribute?.Name ?? propertyInfo.Name;
        Ordinal = ordinal;

        _getter = CreateGetter(propertyInfo);
    }



    /// <summary>
    /// Gets the name of the .NET property.
    /// </summary>
    public string PropertyName { get; }



    /// <summary>
    /// Gets the name of the destination SQL column.
    /// </summary>
    /// <remarks>
    /// Defaults to <see cref="PropertyName"/> unless overridden
    /// by a <see cref="ColumnAttribute"/>.
    /// </remarks>
    public string ColumnName { get; }



    /// <summary>
    /// Gets the declared CLR type of the property, unwrapped from
    /// <see cref="Nullable{T}"/> if applicable.
    /// </summary>
    /// <remarks>
    /// For <see langword="enum"/> properties this returns the enum type itself
    /// — not its underlying integral type. At write time the loader converts
    /// enum values to their underlying integral representation for SQL Server
    /// (e.g., a <c>byte</c>-backed enum is sent as <c>tinyint</c>), so callers
    /// inspecting <see cref="ClrType"/> to reason about the on-wire SQL payload
    /// should call <see cref="Type.GetEnumUnderlyingType"/> on the result when
    /// <see cref="Type.IsEnum"/> is <see langword="true"/>.
    /// </remarks>
    public Type ClrType { get; }



    /// <summary>
    /// Gets a value indicating whether the property can hold <c>null</c>.
    /// </summary>
    public bool IsNullable { get; }



    /// <summary>
    /// Gets the zero-based ordinal position of this column in the type map.
    /// </summary>
    public int Ordinal { get; }



    /// <summary>
    /// Gets the value of the mapped property from the specified object.
    /// </summary>
    /// <param name="instance">The object to read the property value from.</param>
    /// <returns>The property value, or <c>null</c> if the property is null.</returns>
    internal object? GetValue(object instance) => _getter(instance);




    private static Func<object, object?> CreateGetter(PropertyInfo propertyInfo)
    {
        // Expression-tree compiled getter — emits direct IL that calls the
        // property's getter, avoiding the per-row PropertyInfo.GetValue
        // reflection dispatch on the bulk-copy hot path.
        return ReflectionHelpers.CompilePropertyGetter(propertyInfo);
    }
}
