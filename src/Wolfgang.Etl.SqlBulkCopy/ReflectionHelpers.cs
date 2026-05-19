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
    /// Thrown when <paramref name="propertyInfo"/> has no declaring type or no
    /// readable getter.
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

        // (object instance) => (object?)((T)instance).PropertyName
        var instanceParameter = Expression.Parameter(typeof(object), "instance");
        var typedInstance = Expression.Convert(instanceParameter, propertyInfo.DeclaringType);
        var propertyAccess = Expression.Property(typedInstance, propertyInfo);
        var boxedResult = Expression.Convert(propertyAccess, typeof(object));

        return Expression
            .Lambda<Func<object, object?>>(boxedResult, instanceParameter)
            .Compile();
    }
}
