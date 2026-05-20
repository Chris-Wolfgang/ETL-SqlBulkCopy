using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Shared reflection-to-IL helpers. Compiling a property getter once and
/// caching the resulting delegate avoids <see cref="PropertyInfo.GetValue(object)"/>
/// on the per-row hot path during bulk copy writes — a 3-10x speedup
/// depending on property shape.
/// </summary>
internal static class ReflectionHelpers
{
    /// <summary>
    /// Compiles a delegate that reads <paramref name="propertyInfo"/> from an
    /// untyped <see cref="object"/> instance. The returned delegate boxes
    /// value-type results and returns <see langword="null"/> for reference-type
    /// properties whose runtime value is <see langword="null"/> — same shape
    /// as <see cref="PropertyInfo.GetValue(object)"/> but with the reflection
    /// dispatch cost paid once at compile time instead of every call.
    /// </summary>
    /// <param name="propertyInfo">The property to compile a getter for.</param>
    /// <returns>
    /// A delegate equivalent to <c>obj =&gt; (object?)((T)obj).PropertyName</c>
    /// where <c>T</c> is the property's declaring type.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="propertyInfo"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="propertyInfo"/> has no declaring type, no
    /// readable getter, or takes index parameters (indexer properties cannot
    /// be invoked without arguments and are not supported by the bulk-copy
    /// type-map filter — surfacing the failure here gives a clearer error
    /// than the <see cref="Expression.Property(Expression, PropertyInfo)"/>
    /// internals would).
    /// </exception>
    internal static Func<object, object?> CompilePropertyGetter(PropertyInfo propertyInfo)
    {
        if (propertyInfo is null)
        {
            throw new ArgumentNullException(nameof(propertyInfo));
        }

        if (propertyInfo.DeclaringType is null)
        {
            throw new ArgumentException
            (
                $"Property '{propertyInfo.Name}' has no declaring type.",
                nameof(propertyInfo)
            );
        }

        if (propertyInfo.GetMethod is null)
        {
            throw new ArgumentException
            (
                $"Property '{propertyInfo.Name}' has no readable getter.",
                nameof(propertyInfo)
            );
        }

        if (propertyInfo.GetIndexParameters().Length != 0)
        {
            throw new ArgumentException
            (
                $"Property '{propertyInfo.Name}' is an indexer and cannot be " +
                "compiled to a parameterless getter. Bulk-copy column / nested-table " +
                "maps already reject indexers via " +
                "TypeMap.IsReadableInstanceProperty — surfacing it here keeps the " +
                "contract consistent for any future direct caller.",
                nameof(propertyInfo)
            );
        }

        // (object instance) => (object?)((T)instance).PropertyName
        var instanceParameter = Expression.Parameter(typeof(object), "instance");
        var typedInstance = Expression.Convert(instanceParameter, propertyInfo.DeclaringType);
        var propertyAccess = Expression.Property(typedInstance, propertyInfo);
        var boxedResult = Expression.Convert(propertyAccess, typeof(object));

        return Expression
            .Lambda<Func<object, object?>>(boxedResult, instanceParameter)
            .Compile();
    }



    /// <summary>
    /// Compiles a delegate that converts a boxed enum value to its underlying
    /// integral type (also boxed). Equivalent semantics to
    /// <c>Convert.ChangeType(boxedEnum, Enum.GetUnderlyingType(enumType))</c>
    /// but with the type-discovery work paid once at compile time, eliminating
    /// the per-row <see cref="object.GetType()"/> and
    /// <see cref="Enum.GetUnderlyingType(Type)"/> reflection calls in
    /// <c>TypeMapReader.GetValue</c>.
    /// </summary>
    /// <param name="enumType">The enum type to compile a converter for.</param>
    /// <returns>
    /// A delegate equivalent to <c>(object boxed) =&gt; (object)(TUnderlying)(TEnum)boxed</c>.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="enumType"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="enumType"/> is not an enum type.
    /// </exception>
    internal static Func<object, object> CompileEnumToUnderlyingConverter(Type enumType)
    {
        if (enumType is null)
        {
            throw new ArgumentNullException(nameof(enumType));
        }

        if (!enumType.IsEnum)
        {
            throw new ArgumentException
            (
                $"Type '{enumType.Name}' is not an enum.",
                nameof(enumType)
            );
        }

        var underlyingType = Enum.GetUnderlyingType(enumType);

        // (object boxed) => (object)(TUnderlying)(TEnum)boxed
        var boxedParameter = Expression.Parameter(typeof(object), "boxed");
        var unboxedEnum = Expression.Convert(boxedParameter, enumType);
        var asUnderlying = Expression.Convert(unboxedEnum, underlyingType);
        var rebox = Expression.Convert(asUnderlying, typeof(object));

        return Expression
            .Lambda<Func<object, object>>(rebox, boxedParameter)
            .Compile();
    }
}
