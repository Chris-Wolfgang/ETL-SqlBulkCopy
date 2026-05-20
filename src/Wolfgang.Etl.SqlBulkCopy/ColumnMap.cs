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
    private readonly Func<object, object>? _enumConverter;



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

        // Precompute the enum→underlying-integral converter once per column.
        // ClrType is already nullable-unwrapped, so a nullable enum property
        // is still detected here (and the per-row null check happens before
        // the converter is invoked, so we never pass null to it).
        _enumConverter = ClrType.IsEnum
            ? ReflectionHelpers.CompileEnumToUnderlyingConverter(ClrType)
            : null;
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



    /// <summary>
    /// When the mapped property's <see cref="ClrType"/> is an enum, converts
    /// a non-null boxed value of that enum to its underlying integral type
    /// (also boxed). Returns <see langword="null"/> when this column is not
    /// an enum-typed column.
    /// </summary>
    /// <remarks>
    /// Used by <c>TypeMapReader.GetValue</c> to avoid per-row reflection
    /// (<see cref="object.GetType()"/> + <see cref="Enum.GetUnderlyingType(Type)"/>)
    /// on enum columns. The delegate itself is compiled once at type-map
    /// build time and emits a direct unbox-and-cast IL sequence.
    /// </remarks>
    internal Func<object, object>? EnumConverter => _enumConverter;




    private static Func<object, object?> CreateGetter(PropertyInfo propertyInfo)
    {
        // Expression-tree compiled getter — emits direct IL that calls the
        // property's getter, avoiding the per-row PropertyInfo.GetValue
        // reflection dispatch on the bulk-copy hot path.
        return ReflectionHelpers.CompilePropertyGetter(propertyInfo);
    }
}
