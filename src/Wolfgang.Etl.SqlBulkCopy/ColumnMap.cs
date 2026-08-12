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
    /// <param name="mappedType">
    /// The type being mapped (the type whose <see cref="TypeMap"/> is being built).
    /// Used to resolve the source-generated getter, which is registered under the
    /// mapped type — including inherited properties. When <c>null</c>, the getter
    /// lookup falls back to <paramref name="propertyInfo"/>'s declaring type.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="propertyInfo"/> is <c>null</c>.
    /// </exception>
    internal ColumnMap
    (
        PropertyInfo propertyInfo,
        int ordinal,
        Type? mappedType = null
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

        _getter = CreateGetter(propertyInfo, mappedType);

        // Precompute the enum→underlying-integral converter once per column.
        // ClrType is already nullable-unwrapped, so a nullable enum property
        // is still detected here (and the per-row null check happens before
        // the converter is invoked, so we never pass null to it).
        _enumConverter = ClrType.IsEnum
            ? CreateEnumConverter(ClrType)
            : null;
    }



    /// <summary>
    /// Initializes a <see cref="ColumnMap"/> from source-generated descriptor
    /// data — no reflection over the property. The getter and enum converter are
    /// taken from <see cref="GeneratedAccessorRegistry"/>, which the generator
    /// populates from the same module initializer. This is the Native-AOT-clean
    /// construction path used for <c>[BulkCopyable]</c> types. See ADR 0006.
    /// </summary>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="declaringType"/>, <paramref name="propertyName"/>,
    /// <paramref name="columnName"/> or <paramref name="clrType"/> is
    /// <see langword="null"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the required generated getter (or enum converter, for an enum
    /// column) has not been registered — which would indicate a source-generator
    /// defect.
    /// </exception>
    internal ColumnMap
    (
        Type declaringType,
        string propertyName,
        string columnName,
        Type clrType,
        bool isNullable,
        int ordinal
    )
    {
        // Validate before assigning: a null clrType previously slipped past the
        // field assignments and surfaced as a NullReferenceException from the
        // `clrType.IsEnum` check below, after the instance was already half-built.
        // Generated code always passes non-null values, so these guards only fire
        // on a generator defect — where a named parameter beats an NRE.
        if (declaringType is null)
        {
            throw new ArgumentNullException(nameof(declaringType));
        }

        if (propertyName is null)
        {
            throw new ArgumentNullException(nameof(propertyName));
        }

        if (columnName is null)
        {
            throw new ArgumentNullException(nameof(columnName));
        }

        if (clrType is null)
        {
            throw new ArgumentNullException(nameof(clrType));
        }

        PropertyName = propertyName;
        ColumnName = columnName;
        ClrType = clrType;
        IsNullable = isNullable;
        Ordinal = ordinal;

        if (!GeneratedAccessorRegistry.TryGetGetter(declaringType, propertyName, out var getter))
        {
            throw new InvalidOperationException
            (
                $"No source-generated getter is registered for '{declaringType}.{propertyName}'. " +
                "This indicates a source-generator defect."
            );
        }

        _getter = getter;

        if (clrType.IsEnum)
        {
            if (!GeneratedAccessorRegistry.TryGetEnumConverter(clrType, out var enumConverter))
            {
                throw new InvalidOperationException
                (
                    $"No source-generated enum converter is registered for '{clrType}'. " +
                    "This indicates a source-generator defect."
                );
            }

            _enumConverter = enumConverter;
        }
        else
        {
            _enumConverter = null;
        }
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




    private static Func<object, object?> CreateGetter(PropertyInfo propertyInfo, Type? mappedType)
    {
        // Prefer a source-generated accessor when one has been registered for
        // this property. Generated getters are ordinary C# emitted at compile
        // time, so they carry the same throughput as the runtime-compiled
        // getter below without emitting IL at runtime — which is what keeps the
        // per-row hot path Native-AOT clean. See ADR 0006.
        //
        // The generator registers getters under the *mapped* (marked) type —
        // including inherited properties — so look up by that type when the
        // caller knows it. When it doesn't, fall back to ReflectedType (the type
        // the PropertyInfo was obtained through — the derived/mapped type for an
        // inherited property) before DeclaringType (the base class), which would
        // miss the registration and silently fall back to the runtime-compiled
        // (non-AOT) getter.
        var lookupType = mappedType ?? propertyInfo.ReflectedType ?? propertyInfo.DeclaringType;
        if (lookupType is not null
            && GeneratedAccessorRegistry.TryGetGetter(lookupType, propertyInfo.Name, out var generated))
        {
            return generated;
        }

        // Fallback: an expression-tree compiled getter — emits direct IL that
        // calls the property's getter, avoiding the per-row PropertyInfo.GetValue
        // reflection dispatch on the bulk-copy hot path. Behaviourally identical
        // to a generated getter, but the runtime IL emission is not AOT-safe.
        return ReflectionHelpers.CompilePropertyGetter(propertyInfo);
    }



    private static Func<object, object> CreateEnumConverter(Type enumType)
    {
        // Prefer a source-generated enum→underlying converter when registered
        // (compile-time emitted, Native-AOT clean); otherwise fall back to the
        // runtime expression-compiled converter. See ADR 0006.
        if (GeneratedAccessorRegistry.TryGetEnumConverter(enumType, out var generated))
        {
            return generated;
        }

        return ReflectionHelpers.CompileEnumToUnderlyingConverter(enumType);
    }
}
