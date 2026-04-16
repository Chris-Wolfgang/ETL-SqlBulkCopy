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
        return obj =>
        {
            var value = propertyInfo.GetValue(obj);
            if (value is null)
            {
                throw new InvalidOperationException
                (
                    $"Property '{propertyInfo.Name}' is null. " +
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
                $"Property '{propertyInfo.Name}' cannot be enumerated."
            );
        };
    }
}
