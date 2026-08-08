using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Maps a collection property (IEnumerable or array) on a parent type
/// to a separate child table in SQL Server.
/// </summary>
/// <remarks>
/// When the parent type contains properties that are collections of complex types,
/// each collection is mapped to its own destination table. The child type is
/// recursively mapped using <see cref="TypeMap"/>.
/// </remarks>
internal sealed class NestedTableMap
{
    private readonly Func<object, IEnumerable<object>> _getValues;



    /// <summary>
    /// Initializes a new instance of the <see cref="NestedTableMap"/> class.
    /// </summary>
    /// <param name="propertyInfo">The collection property on the parent type.</param>
    /// <param name="childTypeMap">The type map for the element type of the collection.</param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="propertyInfo"/> or <paramref name="childTypeMap"/> is <c>null</c>.
    /// </exception>
    internal NestedTableMap
    (
        PropertyInfo propertyInfo,
        TypeMap childTypeMap
    )
    {
        if (propertyInfo is null)
        {
            throw new ArgumentNullException(nameof(propertyInfo));
        }

        ChildTypeMap = childTypeMap ?? throw new ArgumentNullException(nameof(childTypeMap));
        PropertyName = propertyInfo.Name;
        _getValues = CreateValuesGetter(propertyInfo);
    }



    /// <summary>
    /// Initializes a <see cref="NestedTableMap"/> from source-generated data — no
    /// reflection over the property. The collection getter is taken from
    /// <see cref="GeneratedAccessorRegistry"/>. This is the Native-AOT-clean
    /// construction path used for <c>[BulkCopyable]</c> types. See ADR 0006.
    /// </summary>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="propertyName"/> or <paramref name="childTypeMap"/>
    /// is <c>null</c>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the required generated getter has not been registered — which
    /// would indicate a source-generator defect.
    /// </exception>
    internal NestedTableMap
    (
        Type parentType,
        string propertyName,
        TypeMap childTypeMap
    )
    {
        ChildTypeMap = childTypeMap ?? throw new ArgumentNullException(nameof(childTypeMap));
        PropertyName = propertyName ?? throw new ArgumentNullException(nameof(propertyName));

        if (!GeneratedAccessorRegistry.TryGetGetter(parentType, propertyName, out var getter))
        {
            throw new InvalidOperationException
            (
                $"No source-generated getter is registered for '{parentType}.{propertyName}'. " +
                "This indicates a source-generator defect."
            );
        }

        _getValues = instance => Enumerate(getter(instance), propertyName);
    }



    /// <summary>
    /// Gets the name of the collection property on the parent type.
    /// </summary>
    public string PropertyName { get; }



    /// <summary>
    /// Gets the <see cref="TypeMap"/> describing how the child element type
    /// maps to a SQL Server table.
    /// </summary>
    public TypeMap ChildTypeMap { get; }



    /// <summary>
    /// Extracts the collection items from the specified parent object.
    /// </summary>
    /// <param name="parentInstance">The parent object to read the collection from.</param>
    /// <returns>The items in the collection.</returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="parentInstance"/> is <c>null</c>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the property value is <c>null</c> or cannot be enumerated.
    /// </exception>
    internal IEnumerable<object> GetValues(object parentInstance)
    {
        if (parentInstance is null)
        {
            throw new ArgumentNullException(nameof(parentInstance));
        }

        return _getValues(parentInstance);
    }




    private static Func<object, IEnumerable<object>> CreateValuesGetter(PropertyInfo propertyInfo)
    {
        // Compile the property accessor once via Expression.Lambda — same
        // shape as PropertyInfo.GetValue but emits direct IL, removing the
        // reflection dispatch from the per-parent-batch hot path during
        // nested-table flattening. The enumerable-vs-not branch logic stays
        // in user code because it depends on the runtime value, not on
        // the property's compile-time type.
        var compiledGetter = ReflectionHelpers.CompilePropertyGetter(propertyInfo);
        var propertyName = propertyInfo.Name;

        return obj => Enumerate(compiledGetter(obj), propertyName);
    }



    private static IEnumerable<object> Enumerate(object? value, string propertyName)
    {
        if (value is null)
        {
            throw new InvalidOperationException
            (
                $"Property '{propertyName}' is null. " +
                "Collection properties must not be null; use an empty collection instead."
            );
        }

        if (value is IEnumerable<object> typedEnumerable)
        {
            return typedEnumerable;
        }

        if (value is IEnumerable enumerable)
        {
            return enumerable.Cast<object>();
        }

        throw new InvalidOperationException
        (
            $"Property '{propertyName}' cannot be enumerated."
        );
    }
}
